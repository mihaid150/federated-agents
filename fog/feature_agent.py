"""Feature engineering utilities for the fog layer."""
from __future__ import annotations

from collections import deque
from typing import Deque, Dict

import ray
from common.contracts import Telemetry  # UPDATED import

@ray.remote
class FeatureAgent:
    """Maintains a sliding window of telemetry values and exposes summary features."""
    def __init__(self, window: int = 12):
        self.window = int(window)
        self.values: Deque[float] = deque(maxlen=self.window)

    def ingest(self, sample: Telemetry) -> Dict[str, float]:
        """Ingest a telemetry sample and return basic window statistics."""
        self.values.append(float(sample.value))
        mean = sum(self.values) / len(self.values)
        return {"mean": float(mean), "count": float(len(self.values))}

    def reset(self) -> None:
        self.values.clear()
