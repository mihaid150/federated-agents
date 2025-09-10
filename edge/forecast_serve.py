import ray
from ray import serve
from typing import Dict
from common.contracts import ForecastRequest, ForecastResponse

@serve.deployment(
    name="ForecasterAgent",
    num_replicas=1,
    max_ongoing_requests=2,  # <- replace deprecated max_concurrent_queries
    ray_actor_options={
        "num_cpus": 0.05,
        "memory": int(40 * 1024 * 1024),  # 40 MiB cap
        # NOTE: DO NOT pass max_restarts here; Serve forbids it.
    },
)
class ForecasterAgent:
    def __init__(self):
        self.history = []

    async def __call__(self, request):
        body = await request.json()
        req = ForecastRequest(**body)
        last = self.history[-1] if self.history else 0.0
        preds = [last for _ in range(req.horizon)]
        return ForecastResponse(preds=preds).model_dump()

    def ingest(self, value: float) -> None:
        self.history.append(value)
        if len(self.history) > 10_000:
            self.history = self.history[-10_000:]

if __name__ == "__main__":  # pragma: no cover
    ray.init(ignore_reinit_error=True)
    serve.run(ForecasterAgent.bind(), route_prefix="/forecast")
