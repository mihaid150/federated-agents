from __future__ import annotations

import os
from typing import Optional, Any

import ray
from common.logging_config import logger


def _cgroup_mem_limit_bytes() -> int | None:
    """
    Return the effective container memory limit in bytes, or None if unknown.
    Supports cgroup v2 (/sys/fs/cgroup/memory.max) and v1 (memory.limit_in_bytes).
    """
    # cgroup v2
    try:
        p = "/sys/fs/cgroup/memory.max"
        if os.path.exists(p):
            with open(p) as f:
                v = f.read().strip()
            if v and v != "max":
                return int(v)
    except Exception:
        pass
    # cgroup v1
    try:
        p = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
        if os.path.exists(p):
            with open(p) as f:
                return int(f.read().strip())
    except Exception:
        pass
    return None

def _plan_object_store_only() -> int:
    """
    Choose an object store size that leaves enough RAM for Redis/GCS + tasks.
    Heuristics for small pods (e.g., 512Mi):
      - Reserve overhead ~128–160Mi
      - Object store ~10–15% of pod limit, floor 80Mi, cap 256Mi
      - Ensure at least ~200Mi left for tasks/actors
    """
    limit = _cgroup_mem_limit_bytes()
    if limit is None:
        # Fallback to 1Gi probe if no cgroup (rare in k8s)
        limit = 1024 * 1024 * 1024

    # Reserve overhead for Ray core services
    overhead = int(min(160 * 1024 * 1024, limit * 0.25))

    # Initial guess for object store
    # ~10% of limit, floor 64Mi, cap 192Mi
    obj = int(min(192 * 1024 * 1024, max(64 * 1024 * 1024, limit * 0.10)))

    # Ensure remaining memory for tasks is healthy (~260Mi for Python + Serve)
    min_tasks = 260 * 1024 * 1024
    remaining = limit - overhead - obj
    if remaining < min_tasks:
        deficit = min_tasks - remaining
        obj = max(80 * 1024 * 1024, obj - deficit)

    # Final guard for Ray’s hard minimum (~75Mi)
    obj = max(obj, 64 * 1024 * 1024)
    return obj

def _plan_ray_memory() -> tuple[int, int]:
    """
    Return (memory_for_tasks_bytes, object_store_bytes) sized to the pod limit.
    Rules of thumb for small pods (e.g., 512 MiB):
      - leave headroom for Ray core (GCS/Redis/overhead) ~100–150 MiB
      - object store: max(96 MiB, 15% of limit, capped to 256 MiB)
      - tasks memory: whatever remains but at least 128 MiB
    """
    limit = _cgroup_mem_limit_bytes()
    # Fallback to system RAM if no cgroup limit is found (unlikely in k8s).
    if limit is None:
        limit = max(512 * 1024 * 1024, int(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")))
    # Reserve overhead (redis/gcs/others)
    overhead = int(min(160 * 1024 * 1024, limit * 0.25))
    # Object store sizing
    obj = int(min(256 * 1024 * 1024, max(96 * 1024 * 1024, limit * 0.15)))
    # Memory for tasks/actors
    mem = int(limit - overhead - obj)
    # Ensure minimums; if negative, shrink object store further.
    if mem < 128 * 1024 * 1024:
        deficit = (128 * 1024 * 1024) - mem
        obj = max(80 * 1024 * 1024, obj - deficit)
        mem = int(limit - overhead - obj)
    # Final guard: never below Ray hard mins
    obj = max(obj, 80 * 1024 * 1024)   # >= ~75 MiB
    mem = max(mem, 128 * 1024 * 1024)  # give tasks some room
    return mem, obj


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
                cur_ns = ray.get_runtime_context().namespace
                logger.debug("[RayHub] Ray already initialized (current_ns=%s, desired_ns=%s)", cur_ns, namespace)
            else:
                logger.info("[RayHub] Initializing Ray (namespace=%s, dashboard=%s, ignore_reinit=%s)",
                            namespace, include_dashboard, ignore_reinit_error)

                obj_store = _plan_object_store_only()

                ray.init(
                    ignore_reinit_error=True,
                    include_dashboard=False,
                    namespace=namespace,
                    # Key part:
                    object_store_memory=obj_store,
                    _system_config={
                        "automatic_object_spilling_enabled": True,
                        # For tiny pods, keep raylet lighter:
                        "min_spilling_size": 10 * 1024 * 1024,
                        # Let the node tolerate short spikes before killing workers
                        "memory_usage_threshold": 0.985,
                    },
                )
                try:
                    resources = ray.cluster_resources()
                    logger.debug("[RayHub] Ray cluster resources: %s (obj=%d)", resources,
                                 obj_store)
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
