from __future__ import annotations
from typing import Optional, Any

import ray
from common.logging_config import logger


class RayHub:
    """Reusable helper to init Ray and get-or-create named actors, with logging."""

    def __init__(
        self,
        ignore_reinit_error: bool = True,
        include_dashboard: bool = False,
        namespace: Optional[str] = "cloud",  # stable namespace for the cloud agent
    ):
        self.namespace = namespace
        try:
            if ray.is_initialized():
                # If already up, ensure namespace is what we expect (for clarity only).
                cur_ns = ray.get_runtime_context().namespace
                logger.debug(
                    "[RayHub] Ray already initialized (current_ns=%s, desired_ns=%s)",
                    cur_ns, namespace,
                )
            else:
                logger.info(
                    "[RayHub] Initializing Ray (namespace=%s, dashboard=%s, ignore_reinit=%s)",
                    namespace, include_dashboard, ignore_reinit_error,
                )
                ray.init(
                    ignore_reinit_error=ignore_reinit_error,
                    include_dashboard=include_dashboard,
                    namespace=namespace,
                )
                try:
                    resources = ray.cluster_resources()
                    logger.debug("[RayHub] Ray cluster resources: %s", resources)
                except Exception:
                    logger.debug("[RayHub] Ray cluster resources unavailable right after init")
        except Exception:
            logger.exception("[RayHub] Failed to initialize Ray (namespace=%s)", namespace)
            raise

    @staticmethod
    def get_or_create(
        actor_cls,
        name: str,
        *args: Any,
        namespace: Optional[str] = None,
        detached: bool = True,
        **kwargs: Any,
    ):
        """
        Look up a named actor; if missing, create it with a stable name.
        Uses the current Ray namespace unless one is provided.
        """
        try:
            ns = namespace or ray.get_runtime_context().namespace
        except Exception:
            # Fallback if runtime context is not available yet.
            ns = namespace
        try:
            handle = ray.get_actor(name, namespace=ns)
            logger.info("[RayHub] Found existing actor '%s' (namespace=%s)", name, ns)
            return handle
        except Exception:
            logger.info(
                "[RayHub] Creating actor '%s' (namespace=%s, detached=%s)",
                name, ns, detached,
            )
            opts = {"name": name}
            try:
                if detached:
                    # Ray >= 1.9 supports lifetime="detached"
                    actor = actor_cls.options(lifetime="detached", **opts).remote(*args, **kwargs)
                else:
                    actor = actor_cls.options(**opts).remote(*args, **kwargs)
            except TypeError:
                # Older Ray versions: no lifetime kw — still create a named actor
                actor = actor_cls.options(**opts).remote(*args, **kwargs)
            except Exception:
                logger.exception("[RayHub] Failed creating actor '%s' (namespace=%s)", name, ns)
                raise
            return actor
