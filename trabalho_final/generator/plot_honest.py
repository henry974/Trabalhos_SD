from datetime import datetime, timedelta, timezone
import matplotlib.pyplot as plt

from factory import HonestFactory

factory = HonestFactory("F-001", seed=42)
start = datetime(2026, 11, 2, tzinfo=timezone.utc)   # uma segunda-feira

times, co2, nox = [], [], []
for minute in range(3 * 24 * 60):                    # 3 dias
    ts = start + timedelta(minutes=minute)
    event = factory.reading_at(ts)
    if event is None:
        continue
    times.append(ts)
    co2.append(event.co2_ppm)
    nox.append(event.nox_ppm)

fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, figsize=(10, 6))
ax1.plot(times, co2, linewidth=0.5)
ax1.set_ylabel("CO₂ (ppm)")
ax2.plot(times, nox, linewidth=0.5, color="orange")
ax2.set_ylabel("NOx (ppm)")
plt.savefig("honest.png")
print("salvo em honest.png")