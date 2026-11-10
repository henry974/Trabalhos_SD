from dataclasses import dataclass, asdict
import json

SCHEMA_VERSION = 1

@dataclass(frozen=True)
class TelemetryEvent:
    event_id: str        # uuid4, usado na deduplicação (T3.3)
    factory_id: str      # "F-017", também é a chave de partição
    sensor_id: str       # "F-017-S2"
    ts: str              # ISO 8601 UTC, event time (hora da medição)
    co2_ppm: float | None
    nox_ppm: float | None
    pm25_ugm3: float | None
    schema_version: int = SCHEMA_VERSION

    def to_json(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")