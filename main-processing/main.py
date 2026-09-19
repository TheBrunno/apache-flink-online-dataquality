import json
from datetime import datetime, timezone

from kafka import KafkaConsumer


consumer = KafkaConsumer(
    "financeiro",
    "nao-financeiro",
    bootstrap_servers="kafka:29092",
    group_id="main-processing",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)


def process(event, topic):
    event["processing_timestamp"] = datetime.now(timezone.utc).isoformat()
    event["source_topic"] = topic

    return event


print("Main Processing iniciado.")
print("Aguardando eventos...")

for message in consumer:
    processed_event = process(message.value, message.topic)

    print(
        f"[{message.topic}] "
        f"partition={message.partition} "
        f"offset={message.offset} "
        f"event={processed_event}"
    )