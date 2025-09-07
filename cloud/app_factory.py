from __future__ import annotations
import os
from common.config import BaseConfig
from common.logging_config import logger
from cloud.cloud_service import CloudService


def build_cloud_service() -> CloudService:
    cfg = BaseConfig(
        mqtt_host=os.getenv("CLOUD_MQTT_HOST", "mqtt.cloud.svc.cluster.local"),
        mqtt_port=int(os.getenv("CLOUD_MQTT_PORT", 1883)),
        log_prefix=os.getenv("LOG_PREFIX", "[cloud-agent]"),
    )
    return CloudService(cfg)

if __name__ == "__main__":
    service = build_cloud_service()
    service.start()
    logger.info("[Cloud-Agent]: Cloud Agent service has started...")