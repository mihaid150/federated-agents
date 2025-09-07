import os
import platform
import ray
from common.logging_config import logger

@ray.remote
class OrchestratorPolicy:
    def __init__(self, log_prefix: str = "[orchestrator]"):
        self.log_prefix = log_prefix
        try:
            logger.info(f"[Cloud-Agent]:  {self.log_prefix} actor init (pid={os.getpid()}, node={platform.node()})")
        except Exception:
            logger.exception(f"[Cloud-Agent]: {self.log_prefix} failed during init logging")

    def decide(self, fleet_ctx: dict) -> dict:
        # Be defensive about types
        try:
            quota = float(fleet_ctx.get("quota", 0.2))
        except Exception:
            quota = 0.2
            logger.warning(f"[Cloud-Agent]: {self.log_prefix} quota not a number in ctx={fleet_ctx}; defaulting to {quota}")

        try:
            pressure = float(fleet_ctx.get("pressure", 0.0))
        except Exception:
            pressure = 0.0
            logger.warning(f"[Cloud-Agent]: {self.log_prefix} pressure not a number in ctx={fleet_ctx}; defaulting to {pressure}")

        logger.debug(f"[Cloud-Agent]: {self.log_prefix} decide(ctx={fleet_ctx}) -> parsed quota={quota} pressure={pressure}")

        try:
            if pressure > 0.7:
                decision = {"action": "GLOBAL_THROTTLE", "rate": 0.5}
            else:
                decision = {"action": "NOOP", "quota": quota}
            logger.info(f"[Cloud-Agent]: {self.log_prefix} decision={decision}")
            return decision
        except Exception:
            logger.exception(f"[Cloud-Agent]: {self.log_prefix} exception while deciding (ctx={fleet_ctx})")
            # Safe fallback so the caller isn't blocked
            return {"action": "NOOP", "quota": quota}
