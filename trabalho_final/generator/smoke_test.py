import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from schema import TelemetryEvent

producer = Producer({
    "bootstrap.servers": "localhost:29092",
    "acks": "all",
    "enable.idempotence": True,
})

def on_delivery(err, msg):
    if err:
        print("ERRO ao enviar:", err)
    else:
        print(f"OK: tópico={msg.topic()} partição={msg.partition()} offset={msg.offset()}")

event = TelemetryEvent(
    event_id=str(uuid.uuid4()),
    factory_id="F-017",
    sensor_id="F-017-S2",
    ts=datetime.now(timezone.utc).isoformat(),
    co2_ppm=412.3,
    nox_ppm=38.1,
    pm25_ugm3=12.4,
)

producer.produce(
    topic="telemetry.raw",
    key=event.factory_id,
    value=event.to_json(),
    callback=on_delivery,
)
producer.flush()