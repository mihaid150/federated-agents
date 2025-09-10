from __future__ import annotations
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Dict, Optional, Mapping
from common.commands_base import CommandRecordBase, EnvelopeBase, InvalidEnvelope

class FogCommandCode(IntEnum):
    RETRAIN_FOG   = 2001
    THROTTLE      = 2002
    SUGGEST_MODEL = 2003
    SELECT_FOG    = 2004   # fog-agent accepts this from cloud’s selection

FOG_CODEBOOK: Mapping[str, int] = {c.name: int(c.value) for c in FogCommandCode}

@dataclass(frozen=True)
class FogCommandRecord(CommandRecordBase):
    def validate(self) -> None:  # type: ignore[override]
        super().validate(FOG_CODEBOOK)

@dataclass(frozen=True)
class FogEnvelope(EnvelopeBase):
    command: FogCommandRecord = field(default_factory=lambda: FogCommandRecord(cmd="RETRAIN_FOG"))
    payload: Optional[Dict[str, Any]] = None

    def validate(self) -> None:
        self._require(self.type in ("command", "ack", "error"), f"bad type {self.type}")
        self._require(bool(self.origin), "origin required")
        self.command.validate()

    @classmethod
    def parse_obj(cls, d: Dict[str, Any]) -> "FogEnvelope":
        try:
            cmd_d = d["command"]
            rec = FogCommandRecord(cmd=str(cmd_d["cmd"]).upper(), code=cmd_d.get("code"))
        except Exception as e:
            raise InvalidEnvelope(f"invalid command record: {e}")
        env = cls(
            v=int(d.get("v", 1)),
            type=str(d.get("type", "command")),
            id=str(d.get("id", "")) or cls().id,
            ts=int(d.get("ts", 0)) or cls().ts,
            origin=str(d.get("origin", "")),
            target=d.get("target"),
            round_id=d.get("round_id"),
            qos=d.get("qos", 1),
            command=rec,
            payload=d.get("payload"),
        )
        env.validate()
        return env
