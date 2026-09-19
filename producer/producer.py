import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


BOOTSTRAP_SERVERS = "kafka:29092"

FINANCIAL_TOPIC = "financeiro"
NON_FINANCIAL_TOPIC = "nao-financeiro"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


def generate_transaction():
    transaction_id = f"TX-{uuid.uuid4().hex[:12].upper()}"

    return {
        "transaction_id": transaction_id,
        "account_id": f"ACC-{random.randint(10000, 99999)}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "amount": round(random.uniform(10, 5000), 2),
        "type": random.choice(["PIX", "TED", "TRANSFER"]),
        "status": random.choice(["COMPLETED", "PENDING"]),
    }


def generate_non_financial_event():
    return {
        "event_id": f"EV-{uuid.uuid4().hex[:12].upper()}",
        "account_id": f"ACC-{random.randint(10000, 99999)}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": random.choice(
            ["LOGIN", "LOGOUT", "PASSWORD_CHANGE", "ACCOUNT_ACCESS"]
        ),
        "device": random.choice(["WEB", "MOBILE", "ATM"]),
        "status": random.choice(["SUCCESS", "FAILED"]),
    }


def corrupt_financial_transaction(transaction):
    corruption = random.choice(
        [
            "null_amount",
            "negative_amount",
            "invalid_status",
            "invalid_type",
            "null_account",
            "duplicate",
        ]
    )

    if corruption == "null_amount":
        transaction["amount"] = None

    elif corruption == "negative_amount":
        transaction["amount"] = -abs(transaction["amount"])

    elif corruption == "invalid_status":
        transaction["status"] = "UNKNOWN"

    elif corruption == "invalid_type":
        transaction["type"] = "INVALID"

    elif corruption == "null_account":
        transaction["account_id"] = None

    elif corruption == "duplicate":
        return transaction, True

    return transaction, False


def corrupt_non_financial_event(event):
    corruption = random.choice(
        [
            "null_event_type",
            "invalid_status",
            "null_account",
            "invalid_device",
            "duplicate",
        ]
    )

    if corruption == "null_event_type":
        event["event_type"] = None

    elif corruption == "invalid_status":
        event["status"] = "UNKNOWN"

    elif corruption == "null_account":
        event["account_id"] = None

    elif corruption == "invalid_device":
        event["device"] = "UNKNOWN"

    elif corruption == "duplicate":
        return event, True

    return event, False


last_financial = None
last_non_financial = None


while True:
    if random.random() < 0.5:
        transaction = generate_transaction()

        invalid = random.random() < 0.10

        if invalid:
            transaction, duplicate = corrupt_financial_transaction(transaction)

            if duplicate and last_financial is not None:
                transaction = last_financial.copy()

        producer.send(FINANCIAL_TOPIC, value=transaction)

        last_financial = transaction.copy()

        print(
            f"[FINANCEIRO] "
            f"{transaction['transaction_id']} "
            f"amount={transaction['amount']} "
            f"status={transaction['status']}"
        )

    else:
        event = generate_non_financial_event()

        invalid = random.random() < 0.10

        if invalid:
            event, duplicate = corrupt_non_financial_event(event)

            if duplicate and last_non_financial is not None:
                event = last_non_financial.copy()

        producer.send(NON_FINANCIAL_TOPIC, value=event)

        last_non_financial = event.copy()

        print(
            f"[NAO-FINANCEIRO] "
            f"{event['event_id']} "
            f"type={event['event_type']} "
            f"status={event['status']}"
        )

    producer.flush()

    time.sleep(1)