# cloud_service.py
from __future__ import annotations
import time
from typing import Optional

import ray

from common.config import BaseConfig
from common.event_bus import EventBus
from common.actors import RayHub
from common.runtime import ServiceRuntime
from common.logging_config import logger  # make sure your logging setup initializes this

from cloud.orchestrator_policy import OrchestratorPolicy
from cloud.bandits import EpsilonGreedyBandit
from cloud.rules import Rule, RuleEngine


class CloudCommandPublisher:
    """Encapsulate commands the cloud agent emits to the cloud node."""
    def __init__(self, cfg: BaseConfig, bus: EventBus):
        self.cfg = cfg
        self.bus = bus

    def global_throttle(self, rate: float, round_id: Optional[int] = None):
        payload = {"cmd": "GLOBAL_THROTTLE", "rate": float(rate), "round_id": round_id, "ts": int(time.time())}
        self.bus.publish(self.cfg.topic_agent_commands, payload, qos=1)
        logger.info("[Cloud-Agent]: publish %s -> %s", self.cfg.topic_agent_commands, payload)

    def select_fog(self, target: str, round_id: Optional[int] = None):
        payload = {"cmd": "SELECT_FOG", "target": str(target), "round_id": round_id, "ts": int(time.time())}
        self.bus.publish(self.cfg.topic_agent_commands, payload, qos=1)
        logger.info("[Cloud-Agent]: publish %s -> %s", self.cfg.topic_agent_commands, payload)


def build_rules() -> RuleEngine:
    return RuleEngine([
        Rule("pressure_high", lambda ctx: float(ctx.get("pressure", 0.0)) > 0.7),
        Rule("quota_too_low", lambda ctx: float(ctx.get("quota", 0.0)) < 0.05),
    ])


class CloudEventHandler:
    """Business logic: react to events, use Ray actors, emit commands."""
    def __init__(self, cfg: BaseConfig, publisher: CloudCommandPublisher):
        self.cfg = cfg
        self.publisher = publisher
        self.rules = build_rules()

        # Ray actors
        try:
            self._ray = RayHub(namespace="cloud")
            self._orch = RayHub.get_or_create(OrchestratorPolicy, "orchestrator", namespace="cloud")
            self._bandit = RayHub.get_or_create(EpsilonGreedyBandit, "bandit",
                                                list(cfg.bandit_arms), cfg.bandit_epsilon,
                                                namespace="cloud")
            logger.info("[Cloud-Agent]: handler init ok; arms=%s epsilon=%s", cfg.bandit_arms, cfg.bandit_epsilon)
        except Exception:
            logger.exception("[Cloud-Agent]: handler init failed creating Ray actors")
            raise

    def handle(self, topic: str, payload: dict):
        logger.debug("[Cloud-Agent]: dispatch topic=%s payload=%s", topic, payload)
        if topic.endswith("round-started"):
            self._on_round_started(payload)
        elif topic.endswith("fog-model-received"):
            self._on_fog_model_received(payload)
        elif topic.endswith("cloud-model-broadcast"):
            self._on_cloud_model_broadcast(payload)

    # ---- handlers ----
    def _on_round_started(self, payload: dict):
        round_id = payload.get("round_id")
        data = payload.get("data", {}) or {}
        ctx = {
            "quota": data.get("quota", self.cfg.default_quota),
            "pressure": data.get("pressure", self.cfg.default_pressure),
        }
        logger.info("[Cloud-Agent]: round-started round_id=%s ctx=%s", round_id, ctx)

        # Rules → optional immediate throttle
        try:
            fired = self.rules.evaluate(ctx)
            logger.debug("[Cloud-Agent]: rules evaluated fired=%s", fired)
            if "pressure_high" in fired:
                logger.info("[Cloud-Agent]: rule pressure_high -> GLOBAL_THROTTLE 0.5")
                self.publisher.global_throttle(0.5, round_id)
        except Exception:
            logger.exception("[Cloud-Agent]: rules evaluation error")

        # Orchestrator → policy action
        try:
            logger.debug("[Cloud-Agent]: orchestrator.decide(ctx=%s)", ctx)
            decision = ray.get(self._orch.decide.remote(ctx))
            logger.info("[Cloud-Agent]: orchestrator decision=%s", decision)
            if (decision or {}).get("action") == "GLOBAL_THROTTLE":
                self.publisher.global_throttle(float(decision.get("rate", 0.5)), round_id)
        except Exception:
            logger.exception("[Cloud-Agent]: orchestrator error")

        # Bandit → pick fog to prioritize
        try:
            logger.debug("[Cloud-Agent]: bandit.select()")
            target = ray.get(self._bandit.select.remote())
            logger.info("[Cloud-Agent]: bandit selected target=%s", target)
            if target:
                self.publisher.select_fog(str(target), round_id)
        except Exception:
            logger.exception("[Cloud-Agent]: bandit select error")

    def _on_fog_model_received(self, payload: dict):
        fog = payload.get("fog_name")
        if not fog:
            logger.debug("[Cloud-Agent]: fog-model-received missing fog_name; payload=%s", payload)
            return
        try:
            logger.debug("[Cloud-Agent]: bandit.update(fog=%s, reward=0.0)", fog)
            ray.get(self._bandit.update.remote(fog, 0.0))  # placeholder reward
            logger.info("[Cloud-Agent]: bandit updated fog=%s reward=0.0", fog)
        except Exception:
            logger.exception("[Cloud-Agent]: bandit update error for fog=%s", fog)

    def _on_cloud_model_broadcast(self, payload: dict):
        logger.debug("[Cloud-Agent]: cloud-model-broadcast payload=%s", payload)
        # future hook: evaluation, registry updates, etc.
        pass


class CloudService:
    """Owns the MQTT bus + handler and runs a simple runtime loop."""
    def __init__(self, cfg: BaseConfig):
        self.cfg = cfg
        self.bus = EventBus(cfg.mqtt_host, cfg.mqtt_port, cfg.log_prefix)
        self.publisher = CloudCommandPublisher(cfg, self.bus)
        self.handler = CloudEventHandler(cfg, self.publisher)
        self.runtime = ServiceRuntime(log_prefix=cfg.log_prefix)

    def _on_message(self, topic: str, payload: dict):
        try:
            logger.debug("[Cloud-Agent]: on_message topic=%s payload=%s", topic, payload)
            self.handler.handle(topic, payload)
        except Exception:
            logger.exception("[Cloud-Agent]: handler error on topic=%s", topic)

    def start(self):
        logger.info("[Cloud-Agent]: starting; mqtt=%s:%s topics=[%s, %s, %s]",
                    self.cfg.mqtt_host, self.cfg.mqtt_port,
                    self.cfg.topic_round_started,
                    self.cfg.topic_fog_model_received,
                    self.cfg.topic_cloud_model_broadcast)
        # Subscribe to the cloud events bus
        self.bus.start(self._on_message)
        self.bus.subscribe_many([
            self.cfg.topic_round_started,
            self.cfg.topic_fog_model_received,
            self.cfg.topic_cloud_model_broadcast,
        ], qos=1)
        logger.info("[Cloud-Agent]: online. Subscribed to events.")
        self.runtime.run_forever()
