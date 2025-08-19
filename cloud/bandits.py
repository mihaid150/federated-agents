"""Bandit utilities for high-level model or policy selection."""

from __future__ import annotations

import random
from typing import Dict, List

import ray


@ray.remote
class EpsilonGreedyBandit:
    """Very small epsilon-greedy multi-armed bandit."""

    def __init__(self, arms: List[str], epsilon: float = 0.1):
        self.arms = arms
        self.epsilon = epsilon
        self.counts: Dict[str, int] = {a: 0 for a in arms}
        self.values: Dict[str, float] = {a: 0.0 for a in arms}

    def select(self) -> str:
        if not self.arms:
            raise ValueError("no arms configured")
        if random.random() < self.epsilon:
            return random.choice(self.arms)
        return max(self.values, key=self.values.get)

    def update(self, arm: str, reward: float) -> None:
        n = self.counts[arm] + 1
        value = self.values[arm]
        self.counts[arm] = n
        self.values[arm] = value + (reward - value) / n