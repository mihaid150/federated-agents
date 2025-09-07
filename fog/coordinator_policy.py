import ray

@ray.remote
class CoordinatorPolicy:
    def decide(self, signals: dict):
        """
        signals: {"node": str, "drift": float, "mae": float, "backlog": int}
        Return a simple coordination decision for the fog tier.
        """
        drift = float(signals.get("drift", 0.0))
        mae = float(signals.get("mae", 0.0))
        backlog = int(signals.get("backlog", 0))

        if drift or mae > 0.15:
            return {"action": "RETRAIN_FOG", "budget": "LOW"}
        if backlog > 10:
            return {"action": "THROTTLE", "rate": 0.5}
        return {"action": "NOOP"}
