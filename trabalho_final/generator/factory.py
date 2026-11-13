import random
import uuid
from datetime import datetime, timedelta, timezone

from schema import TelemetryEvent
from fraud import Fraud


class HonestFactory:
    def __init__(self, factory_id: str, seed: int, size: float = 1.0,
             fraud: Fraud | None = None):
        self.factory_id = factory_id
        self.sensor_id = f"{factory_id}-S1"
        self.size = size
        self.rng = random.Random(seed)
        self.fraud = fraud

    def load_at(self, ts: datetime) -> float:
        """Quão ocupada a fábrica está (0 a 1) naquele horário."""
        weekend = ts.weekday() >= 5
        if weekend:
            return 0.3
        if 6 <= ts.hour < 22:
            return 0.9   # horário de produção
        return 0.4       # noite

    def reading_at(self, ts: datetime) -> TelemetryEvent | None:
        # 1% das vezes a leitura se perde (queda de rede)
        if self.rng.random() < 0.01:
            return None

        load = self.load_at(ts)
        noise = lambda sigma: self.rng.gauss(0, sigma)

        co2 = 350 + 300 * load * self.size + noise(8)
        nox = 20 + 40 * load * self.size + noise(2)
        pm25 = 8 + 10 * load * self.size + noise(1)

        if self.fraud is not None:
            co2, nox, pm25 = self.fraud.apply(ts, co2, nox, pm25)
            
        return TelemetryEvent(
            event_id=str(uuid.uuid4()),
            factory_id=self.factory_id,
            sensor_id=self.sensor_id,
            ts=ts.isoformat(),
            co2_ppm=round(co2, 1),
            nox_ppm=round(nox, 1),
            pm25_ugm3=round(pm25, 1),
        )