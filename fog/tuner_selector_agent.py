import ray
from ray import tune
from mabwiser.mab import MAB, LearningPolicy

@ray.remote
class TunerSelectorAgent:
    def __init__(self):
        # Suggests models; rewards update its belief
        self.bandit = MAB(
            arms=["lstm", "arima", "xgb"],
            learning_policy=LearningPolicy.UCB1(alpha=1.25)
        )
        # Seed with a neutral start
        self.bandit.fit(decisions=["arima"], rewards=[0.0])

    def suggest_model(self, context: dict | None = None) -> str:
        return str(self.bandit.predict())

    def reward(self, arm: str, metric: float) -> None:
        """
        Higher reward => better. If you pass MSE, use the NEGATIVE value.
        e.g. reward(arm, -mse)
        """
        self.bandit.partial_fit(decisions=[arm], rewards=[float(metric)])

    def quick_tune(self, trainable, space: dict, num_samples=16):
        result = tune.run(
            trainable,
            config=space,
            num_samples=int(num_samples),
            metric="val_loss",
            mode="min"
        )
        return {
            "best_config": result.best_config,
            "best_metric": result.best_result.get("val_loss"),
        }
