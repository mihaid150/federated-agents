from __future__ import annotations

import os
import time
import signal
import threading

import ray
from ray import serve

from edge.sensing_agent import SensingAgent
from edge.forecast_serve import ForecasterAgent
from common.adapters.mqtt_bridge import MQTTBridge


class Config:
    """Runtime configuration via environment variables."""
    def __init__(self) -> None:
        # Identity
        self.edge_name = os.getenv("EDGE_NAME", os.getenv("NODE_NAME", "edge1_fog1"))

        # MQTT (fog-local broker)
        self.fog_mqtt_host = os.getenv("FOG_MQTT_HOST", "mqtt-fog1")
        self.fog_mqtt_port = int(os.getenv("FOG_MQTT_PORT", "1883"))

        # Topics used by the generic MQTTBridge (you can override via env)
        # INPUT_TOPIC: node/{EDGE}/telemetry
        # OUTPUT_TOPIC: where we publish nudges; default below works with fog-node if you set it there too.
        self.mqtt_input_topic = os.getenv("MQTT_INPUT_TOPIC", f"node/{self.edge_name}/telemetry")
        self.mqtt_output_topic = os.getenv("MQTT_OUTPUT_TOPIC", "fog/agent/commands")  # unified with fog-node/agent

        # Policy thresholds
        self.policy_mae_threshold = float(os.getenv("POLICY_MAE_THRESHOLD", "0.12"))
        self.policy_requires_drift = os.getenv("POLICY_REQUIRES_DRIFT", "true").lower() == "true"

        # Serve
        self.serve_detached = True


class Actors:
    """Owns Ray initialization and core actors."""
    def __init__(self, cfg: Config):
        self.cfg = cfg
        # Start Ray (single-node) and Ray Serve
        ray.init(ignore_reinit_error=True, include_dashboard=False)

        if cfg.serve_detached:
            serve.start(detached=True)

        # Deploy the forecaster HTTP endpoint (Ray Serve)
        # POST /forecast   body: ForecastRequest -> ForecastResponse
        serve.run(ForecasterAgent.bind())

        # Sensing agent (detached so restarts won’t recreate it)
        self.sensing = self._get_or_create(SensingAgent, f"sensing_{cfg.edge_name}")

    @staticmethod
    def _get_or_create(cls, name: str, *args, **kwargs):
        try:
            return ray.get_actor(name)
        except Exception:
            return cls.options(name=name, lifetime="detached", namespace="edge").remote(*args, **kwargs)


class EdgeAgentService:
    """Boots the MQTT bridge and keeps the process alive."""
    def __init__(self, cfg: Config | None = None):
        self.cfg = cfg or Config()
        self.actors = Actors(self.cfg)
        self._stop = threading.Event()
        self._bridge = None

    def start(self):
        # Build and start the MQTT bridge (telemetry → sensing; nudges → output topic)
        # NOTE: The generic adapter reads topics from env:
        #  - FOG_MQTT_HOST / FOG_MQTT_PORT
        #  - MQTT_INPUT_TOPIC / MQTT_OUTPUT_TOPIC
        # Set them here to avoid surprises.
        os.environ["FOG_MQTT_HOST"] = self.cfg.fog_mqtt_host
        os.environ["FOG_MQTT_PORT"] = str(self.cfg.fog_mqtt_port)
        os.environ["MQTT_INPUT_TOPIC"] = self.cfg.mqtt_input_topic
        os.environ["MQTT_OUTPUT_TOPIC"] = self.cfg.mqtt_output_topic
        os.environ["EDGE_NAME"] = self.cfg.edge_name  # used by adapter for defaults

        self._bridge = MQTTBridge(self.actors.sensing, policy_threshold=self.cfg.policy_mae_threshold)
        self._bridge.start()

        print(f"[edge-agent] {self.cfg.edge_name} online. MQTT bridge + Ray Serve running.")
        signal.signal(signal.SIGTERM, self._graceful)
        signal.signal(signal.SIGINT, self._graceful)

        while not self._stop.is_set():
            time.sleep(0.5)

    def _graceful(self, *_):
        print("[edge-agent] shutdown requested.")
        self._stop.set()
