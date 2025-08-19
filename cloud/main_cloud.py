"""Minimal bootstrap for cloud-side agents."""

from __future__ import annotations

import time

import ray

from agents.cloud.bandits import EpsilonGreedyBandit
from agents.cloud.orchestrator_policy import OrchestratorPolicy


if __name__ == "__main__":  # pragma: no cover - script usage
    ray.init(ignore_reinit_error=True)

    OrchestratorPolicy.options(name="orchestrator", lifetime_scope="detached").remote()
    EpsilonGreedyBandit.options(name="bandit", lifetime_scope="detached").remote(["fog1", "fog2"])

    print("[cloud-agent] online. Actors registered with Ray.")
    while True:
        time.sleep(60)
