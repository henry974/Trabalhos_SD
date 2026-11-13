from datetime import datetime

# Nível de poluição "de fundo", mesmo sem produção.
# Só a parte ACIMA disso vem da fábrica, e é essa parte que a fraude esconde.
BASELINE = {"co2": 350, "nox": 20, "pm25": 8}


class Fraud:
    """Molde de toda fraude: recebe os valores verdadeiros e devolve os reportados."""

    def apply(self, ts: datetime, co2: float, nox: float, pm25: float):
        return co2, nox, pm25


class InspectionHoursDefeatDevice(Fraud):
    def __init__(self, intensity: float = 0.5, start: datetime | None = None):
        self.intensity = intensity   # 0 = não esconde nada, 1 = esconde tudo
        self.start = start           # antes dessa data a fábrica é honesta

    def _is_inspection_time(self, ts: datetime) -> bool:
        return ts.weekday() < 5 and 8 <= ts.hour < 17

    def apply(self, ts, co2, nox, pm25):
        if self.start is not None and ts < self.start:
            return co2, nox, pm25
        if not self._is_inspection_time(ts):
            return co2, nox, pm25

        keep = 1 - self.intensity    # fração da poluição que ainda aparece
        co2 = BASELINE["co2"] + (co2 - BASELINE["co2"]) * keep
        nox = BASELINE["nox"] + (nox - BASELINE["nox"]) * keep
        pm25 = BASELINE["pm25"] + (pm25 - BASELINE["pm25"]) * keep
        return co2, nox, pm25