from __future__ import annotations
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Dict, Optional, Mapping, Type
from common.commands_base import CommandRecordBase, EnvelopeBase, InvalidEnvelope

# ---------- Cloud command enum codebook ----------
class CloudCommandCode(IntEnum):
    SELECT_FOG = 1001
    GLOBAL_THROTTLE = 1005

CLOUD_CODEBOOK: Mapping[str, int] = {c.name: int(c.value) for c in CloudCommandCode}

# ---------- Command record (cloud-private) ----------
@dataclass(frozen=True)
class CloudCommandRecord(CommandRecordBase):
    def validate(self, codebook: Mapping[str, int]) -> None:  # type: ignore[override]
        super().validate(CLOUD_CODEBOOK)

# ----------- Envelope (cloud-private)
@dataclass(frozen=True)
class CloudEnvelope(EnvelopeBase):
    command: CloudCommandRecord = field(default_factory=lambda: CloudCommandRecord(cmd="SELECT_FOG"))
    payload: Optional[Dict[str, Any]] = None

    def validate(self) -> None:
        self._require(self.type in ("command", "ack", "error"), f"bad type {self.type}")
        self._require(bool(self.origin), "origin required")
        self.command.validate()

    # Builders
    @classmethod
    def make(cls, cmd: str, *, origin: str, target: Optional[str] = None, round_id: Optional[int] = None,
             payload: Optional[Dict[str, Any]] = None, code: Optional[int] = None) -> CloudEnvelope:
        rec = CloudCommandRecord(cmd=cmd.upper(), code=code or CLOUD_CODEBOOK.get(cmd.upper()))
        env = cls(origin=origin, target=target, round_id=round_id, command=rec, payload=payload)
        env.validate()
        return env

    # Parsing from dict (for tests or cross-node)
    @classmethod
    def parse_obj(cls, d: Dict[str, Any]) -> "CloudEnvelope":
        try:
            cmd_d = d["command"]
            rec = CloudCommandRecord(cmd=str(cmd_d["cmd"]).upper(), code=cmd_d.get("code"))
        except Exception as e:
            raise InvalidEnvelope(f"invalid command record {d.get('command')!r}: {e}")
        env = cls(
            v=int(d.get("v", 1)),
            type=str(d.get("type", "command")),
            id=str(d.get("id", "")) or cls().id,  # default if missing
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