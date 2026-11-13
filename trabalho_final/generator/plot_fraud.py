from collections import defaultdict
from datetime import datetime, timedelta, timezone
import matplotlib.pyplot as plt

from factory import HonestFactory
from fraud import InspectionHoursDefeatDevice

start = datetime(2026, 11, 2, tzinfo=timezone.utc)   # segunda-feira

def average_by_hour(factory):
    values = defaultdict(list)
    for minute in range(7 * 24 * 60):
        ts = start + timedelta(minutes=minute)
        event = factory.reading_at(ts)
        if event is not None:
            values[ts.hour].append(event.co2_ppm)
    return [sum(values[h]) / len(values[h]) for h in range(24)]

honest = HonestFactory("F-001", seed=42)
cheater = HonestFactory("F-002", seed=42,
                        fraud=InspectionHoursDefeatDevice(intensity=0.7))

plt.plot(range(24), average_by_hour(honest), label="honesta")
plt.plot(range(24), average_by_hour(cheater), label="fraudulenta")
plt.xlabel("hora do dia")
plt.ylabel("CO₂ médio (ppm)")
plt.legend()
plt.savefig("fraud.png")
print("salvo em fraud.png")