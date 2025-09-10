from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Optional, List

import ray
from datetime import datetime, timedelta
from common.event_bus import EventBus
from common.actors import RayHub
from common.logging_config import logger
from common.runtime import ServiceRuntime

from fog.coordinator_policy import CoordinatorPolicy
from fog.feature_agent import FeatureAgent
from fog.tuner_selector_agent import TunerSelectorAgent


# =========================
# Config
# =========================

def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except Exception:
        return int(default)

@dataclass(frozen=True)
class FogConfig:
    # Prefer fog-specific envs, fallback to generic
    mqtt_host: str = os.getenv("FOG_MQTT_HOST", os.getenv("MQTT_HOST", "mqtt"))
    mqtt_port: int = _int_env("FOG_MQTT_PORT", int(os.getenv("MQTT_PORT", "1883")))
    log_prefix: str = os.getenv("LOG_PREFIX", "[fog-agent]")

    # cloud broker - where the cloud agent publishes decisions
    cloud_mqtt_host: str = os.getenv("CLOUD_MQTT_HOST", os.getenv("FOG_MQTT_HOST", "mqtt"))
    cloud_mqtt_port: int = _int_env("CLOUD_MQTT_PORT", int(os.getenv("FOG_MQTT_PORT", "1883")))

    # identity - used to subscribe to the targeted topic
    fog_name: str = os.getenv("FOG_NAME", "")

    # Topics — make sure federated-grid/fog publishes these:
    topic_edge_model_received: str = os.getenv("TOPIC_EDGE_MODEL_RECEIVED", "fog/events/edge-model-received")
    topic_aggregation_complete: str = os.getenv("TOPIC_AGGREGATION_COMPLETE", "fog/events/aggregation-complete")
    topic_cloud_model_downlink: str = os.getenv("TOPIC_CLOUD_MODEL_DOWNLINK", "fog/events/cloud-model-downlink")

    # Commands back to fog node:
    topic_agent_commands: str = os.getenv("TOPIC_AGENT_COMMANDS", "fog/agent/commands")

    # Actor defaults
    feature_window: int = int(os.getenv("FEATURE_WINDOW", "12"))

    # cloud-agent, fog-agent topics
    # targeted and broadcast
    topic_cloud_to_fog_targeted: str = os.getenv("TOPIC_CLOUD_TO_FOG_TARGETED", f"cloud/agent/fog/{os.getenv('FOG_NAME', '')}/commands")
    topic_cloud_to_fog_broadcast: str = os.getenv("TOPIC_CLOUD_TO_FOG_BROADCAST", "cloud/fog/command")

# =========================
# Commands publisher
# =========================

class FogCommandPublisher:
    def __init__(self, cfg: FogConfig, bus: EventBus):
        self.cfg = cfg
        self.bus = bus

    def retrain_fog(self, round_id: Optional[int] = None, budget: str = "LOW", params: dict | None = None):
        self.bus.publish(
            self.cfg.topic_agent_commands,
            {
                "cmd": "RETRAIN_FOG",
                "budget": budget,
                "round_id": round_id,
                "params": dict(params or {}),
                "ts": int(time.time())},
            qos=1
        )

    def throttle(self, rate: float, reason: str = "backlog", round_id: Optional[int] = None):
        self.bus.publish(
            self.cfg.topic_agent_commands,
            {"cmd": "THROTTLE", "rate": float(rate), "reason": reason, "round_id": round_id, "ts": int(time.time())},
            qos=1
        )

    def suggest_model(self, model: str, reason: str = "bandit", round_id: Optional[int] = None):
        self.bus.publish(
            self.cfg.topic_agent_commands,
            {"cmd": "SUGGEST_MODEL", "model": str(model), "reason": reason, "round_id": round_id, "ts": int(time.time())},
            qos=1
        )


# =========================
# Handler
# =========================

class FogEventHandler:
    """
    Reacts to fog events from the node; uses Ray actors to decide actions.
    """
    def __init__(self, cfg: FogConfig, publisher: FogCommandPublisher):
        self.cfg = cfg
        self.pub = publisher

        # Ray actors
        self._ray = RayHub()
        self._coord = RayHub.get_or_create(CoordinatorPolicy, "fog_coordinator")
        self._features = RayHub.get_or_create(FeatureAgent, "fog_features", cfg.feature_window)
        self._tuner = RayHub.get_or_create(TunerSelectorAgent, "fog_tuner")

    def handle(self, topic: str, payload: dict):
        if topic.endswith("edge-model-received"):
            self._on_edge_model_received(payload)
        elif topic.endswith("aggregation-complete"):
            self._on_aggregation_complete(payload)
        elif topic.endswith("cloud-model-downlink"):
            self._on_cloud_model_downlink(payload)

    # --- events ---

    def _on_edge_model_received(self, payload: dict):
        """
        Payload suggestion:
        {"edge_name": "edge1_fog1",
         "metrics": {"mse": 0.12, "mae": 0.25, "drift": 1},
         "round_id": 123, "ts": 1712345}
        """
        metrics = payload.get("metrics") or {}
        drift = float(metrics.get("drift", 0.0))
        mae = float(metrics.get("mae", 0.0))
        mse = float(metrics.get("mse", 0.0))
        backlog = int(payload.get("backlog", 0))
        round_id = payload.get("round_id")
        edge_name = str(payload.get("edge_name", ""))

        # 1) Update bandit/tuner reward with NEGATIVE MSE (lower MSE = higher reward)
        try:
            _ = ray.get(self._tuner.reward.remote("lstm", -mse))  # arm label aligned with your model list
        except Exception:
            logger.exception("%s tuner reward failed (mse=%s)", self.cfg.log_prefix, mse)

        # 2) Optionally retrain edges if drift/mae threshold is breached; pass DATA-PLANE params.
        if drift or mae > 0.15:
            start = (datetime.utcnow() - timedelta(days=3)).strftime("%Y-%m-%d")
            retrain_params = {
                "date": start,  # edge will train [start .. start+2d], evaluate +2d..+4d
                "sequence_length": 144,  # flows into post_preprocessing_padding(...)
            }
            # fan-out RETRAIN_FOG to edges via fog node, preserving params
            self.pub.retrain_fog(round_id=round_id, budget="LOW", params=retrain_params)

        # Coordinator: decide retrain/throttle based on signals
        try:
            decision = ray.get(self._coord.decide.remote({"drift": drift, "mae": mae, "backlog": backlog}))
            action = (decision or {}).get("action", "NOOP")
            if action == "RETRAIN_FOG":
                self.pub.retrain_fog(round_id=round_id, budget=str(decision.get("budget", "LOW")))
            elif action == "THROTTLE":
                self.pub.throttle(rate=float(decision.get("rate", 0.5)), reason="coord", round_id=round_id)
        except Exception as e:
            print(f"{self.cfg.log_prefix} coordinator error: {e}")

        # Tuner: suggest model (context could include recent metrics)
        try:
            model = ray.get(self._tuner.suggest_model.remote({"mae": mae, "drift": drift}))
            if model:
                self.pub.suggest_model(model=str(model), reason="tuner", round_id=round_id)
        except Exception as e:
            print(f"{self.cfg.log_prefix} tuner suggest error: {e}")

    def _on_aggregation_complete(self, payload: dict):
        """
        Payload suggestion:
        {"round_id": 123, "fog_model_path": "/app/models/fog_model.keras",
         "eval": {"mse": 0.10}, "ts": 1712345}
        We can reward the tuner with NEGATIVE mse to favor smaller errors.
        """
        eval_metrics = payload.get("eval") or {}
        mse = eval_metrics.get("mse")
        if mse is not None:
            try:
                ray.get(self._tuner.reward.remote("lstm", -float(mse)))  # example: reward current arm
            except Exception as e:
                print(f"{self.cfg.log_prefix} tuner reward error: {e}")

    def _on_cloud_model_downlink(self, payload: dict):
        """
        Payload suggestion:
        {"round_id": 123, "ts": 1712345}
        Optional hook: could trigger edge refresh, etc.
        """
        # No-op for now; add orchestration here if needed.
        pass


# =========================
# Service
# =========================

class FogService:
    def __init__(self, cfg: FogConfig):
        self.cfg = cfg

        # local fog event bus
        self.bus = EventBus(cfg.mqtt_host, cfg.mqtt_port, cfg.log_prefix)

        # cloud_bus ( subscribe to cloud-agent -> fog-agent commands)
        self.cloud_bus = EventBus(cfg.cloud_mqtt_host, cfg.cloud_mqtt_port, f"{cfg.log_prefix}[cloud]")

        self.publisher = FogCommandPublisher(cfg, self.bus)
        self.handler = FogEventHandler(cfg, self.publisher)
        self.runtime = ServiceRuntime(log_prefix=cfg.log_prefix)

    def _on_message(self, topic: str, payload: dict):
        try:
            self.handler.handle(topic, payload)
        except Exception as e:
            print(f"{self.cfg.log_prefix} handler error on {topic}: {e}")

    def _on_cloud_message(self, topic: str, payload: dict):
        """
        Expected payloads from cloud agent, e.g.
          {"cmd":"GLOBAL_THROTTLE","rate":0.5,"round_id":1,"ts":...}
          {"cmd":"RETRAIN_FOG","budget":"LOW","round_id":1,"ts":...}
          {"cmd":"SUGGEST_MODEL","model":"lstm","round_id":1,"ts":...}
          {"cmd":"SELECT_FOG","target":"fog1","round_id":1,"ts":...}
        """
        try:
            cmd = str(payload.get("cmd", "")).upper()
            target = payload.get("target")
            round_id = payload.get("round_id")

            # If we are on the broadcast topic, honor 'target'
            if topic.endswith("/all/commands"):
                if target and self.cfg.fog_name and target != self.cfg.fog_name and target.lower() != "all":
                    logger.info("%s ignoring broadcast for target=%s (this=%s)",
                                self.cfg.log_prefix, target, self.cfg.fog_name)
                    return

            logger.info("%s cloud→fog-agent cmd=%s payload=%s", self.cfg.log_prefix, cmd, payload)

            if cmd == "GLOBAL_THROTTLE":
                rate = float(payload.get("rate", 0.5))
                self.publisher.throttle(rate=rate, reason="cloud", round_id=round_id)

            elif cmd == "RETRAIN_FOG":
                self.publisher.retrain_fog(round_id=round_id, budget=str(payload.get("budget", "LOW")))

            elif cmd == "SUGGEST_MODEL":
                model = payload.get("model")
                if model:
                    self.publisher.suggest_model(model=str(model), reason="cloud", round_id=round_id)

            elif cmd == "SELECT_FOG":
                # Optional mapping: when this fog is selected, nudge retraining.
                # (You can change this to another local command if you prefer.)
                tgt = target or self.cfg.fog_name
                if not self.cfg.fog_name or tgt == self.cfg.fog_name:
                    logger.info("%s this fog selected by cloud; triggering RETRAIN_FOG", self.cfg.log_prefix)
                    self.publisher.retrain_fog(
                        round_id = round_id,
                        budget = "LOW",
                        params = {"date": datetime.utcnow().strftime("%Y-%m-%d"),
                                 "sequence_length": 144},
                    )
                else:
                    logger.info("%s SELECT_FOG for %s; this fog is %s → ignore",
                                self.cfg.log_prefix, tgt, self.cfg.fog_name)

            else:
                logger.warning("%s unknown cloud cmd=%s payload=%s", self.cfg.log_prefix, cmd, payload)

        except Exception:
            logger.exception("%s cloud message handling failure on topic=%s", self.cfg.log_prefix, topic)

    def start(self):
        # Start local bus and subscribe to fog events
        self.bus.start(self._on_message)
        self.bus.subscribe_many([
            self.cfg.topic_edge_model_received,
            self.cfg.topic_aggregation_complete,
            self.cfg.topic_cloud_model_downlink,
        ], qos=1)
        logger.info("%s online (local). Subscribed to fog events.", self.cfg.log_prefix)

        # Start cloud bus and subscribe to cloud-agent commands
        self.cloud_bus.start(self._on_cloud_message)

        # Subscribe to both targeted and broadcast command topics
        subs = [self.cfg.topic_cloud_to_fog_targeted, self.cfg.topic_cloud_to_fog_broadcast]
        # filter out empty targeted topic if FOG_NAME wasn't set
        subs = [t for t in subs if t]
        self.cloud_bus.subscribe_many(subs, qos=1)
        logger.info("%s online (cloud). Subscribed to %s", self.cfg.log_prefix, subs)

        self.runtime.run_forever()
