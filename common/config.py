from __future__ import annotations
import os
from dataclasses import dataclass
from typing import List, Tuple

@dataclass(frozen=True)
class BaseConfig:
    # MQTT
    mqtt_host: str = os.getenv("MQTT_HOST", os.getenv("MQTT_HOST", "mqtt"))
    mqtt_port: int = int(os.getenv("MQTT_PORT", os.getenv("MQTT_PORT", "1883")))
    log_prefix: str = os.getenv("LOG_PREFIX", "[agent]")

    # genetic topics to be reusable by each node
    topic_round_started: str = os.getenv("TOPIC_ROUND_STARTED", "[node]/events/round-started")
    topic_fog_model_received: str = os.getenv("TOPIC_FOG_MODEL_RECEIVED", "[node]/events/fog-model-received")
    topic_cloud_model_broadcast: str = os.getenv("TOPIC_CLOUD_MODEL_BROADCAST", "[node]/events/cloud-model-broadcast")
    topic_agent_commands: str = os.getenv("TOPIC_AGENT_COMMANDS", "[node]/agent/commands")

    # bandit defaults
    bandit_arms: Tuple[str, ...] = tuple(os.getenv("BANDIT_ARMS", "fog1,fog2").split(","))
    bandit_epsilon: float = float(os.getenv("BANDIT_EPSILON", "0.1"))

    # orchestrator defaults
    default_quota: float = float(os.getenv("ORCH_DEFAULT_QUOTA", "0.2"))
    default_pressure: float = float(os.getenv("ORCH_DEFAULT_PRESSURE", "0.3"))