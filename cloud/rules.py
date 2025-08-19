""""Tiny rule engine used by cloud-side orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List


@dataclass
class Rule:
    name: str
    check: Callable[[Dict[str, Any]], bool]


class RuleEngine:
    def __init__(self, rules: List[Rule] | None = None):
        self.rules: List[Rule] = rules or []

    def add_rule(self, rule: Rule) -> None:
        self.rules.append(rule)

    def evaluate(self, context: Dict[str, Any]) -> List[str]:
        triggered: List[str] = []
        for rule in self.rules:
            try:
                if rule.check(context):
                    triggered.append(rule.name)
            except Exception:
                # For demo purposes we silently ignore faulty rules
                pass
        return triggered
