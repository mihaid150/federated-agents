from __future__ import annotations
from fog.fog_service import FogService, FogConfig

def build_fog_service() -> FogService:
    cfg = FogConfig()
    return FogService(cfg)

if __name__ == "__main__":
    build_fog_service().start()
