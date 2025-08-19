"""Entry point for a simple fog agent runtime."""

from __future__ import annotations

import time

import ray

from agents.fog.coordinator_policy import CoordinatorPolicy
from agents.fog.feature_agent import FeatureAgent
from agents.fog.tuner_selector_agent import TunerSelectorAgent


if __name__ == "__main__":  # pragma: no cover - script usage
    ray.init(ignore_reinit_error=True)

    CoordinatorPolicy.options(name="coordinator", lifetime_scope="detached").remote()
    FeatureAgent.options(name="features", lifetime_scope="detached").remote()
    TunerSelectorAgent.options(name="tuner", lifetime_scope="detached").remote()

    print("[fog-agent] online. Actors registered with Ray.")
    while True:
        time.sleep(60)
