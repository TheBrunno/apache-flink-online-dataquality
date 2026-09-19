import json

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource


env = StreamExecutionEnvironment.get_execution_environment()


financeiro_source = KafkaSource.builder() \
    .set_bootstrap_servers("kafka:29092") \
    .set_topics("financeiro") \
    .set_group_id("data-quality-flink") \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()


nao_financeiro_source = KafkaSource.builder() \
    .set_bootstrap_servers("kafka:29092") \
    .set_topics("nao-financeiro") \
    .set_group_id("data-quality-flink") \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()


def validate_financeiro(value):
    event = json.loads(value)
    problems = []

    if event.get("account_id") is None:
        problems.append("ACCOUNT_ID_NULL")

    if event.get("amount") is None:
        problems.append("AMOUNT_NULL")
    elif event["amount"] < 0:
        problems.append("AMOUNT_NEGATIVE")

    if event.get("status") not in ["COMPLETED", "PENDING", "FAILED"]:
        problems.append("INVALID_STATUS")

    if event.get("type") not in ["PIX", "TED", "CARD"]:
        problems.append("INVALID_TYPE")

    if problems:
        return {
            "domain": "financeiro",
            "quality": "INVALID",
            "problems": problems,
            "event": event
        }

    return {
        "domain": "financeiro",
        "quality": "VALID",
        "problems": [],
        "event": event
    }


def validate_nao_financeiro(value):
    event = json.loads(value)
    problems = []

    if event.get("account_id") is None:
        problems.append("ACCOUNT_ID_NULL")

    if event.get("event_type") is None:
        problems.append("EVENT_TYPE_NULL")

    if event.get("status") not in ["SUCCESS", "FAILED"]:
        problems.append("INVALID_STATUS")

    if event.get("device") not in ["MOBILE", "WEB", "ATM"]:
        problems.append("INVALID_DEVICE")

    if problems:
        return {
            "domain": "nao-financeiro",
            "quality": "INVALID",
            "problems": problems,
            "event": event
        }

    return {
        "domain": "nao-financeiro",
        "quality": "VALID",
        "problems": [],
        "event": event
    }


financeiro_stream = env.from_source(
    financeiro_source,
    WatermarkStrategy.no_watermarks(),
    "financeiro"
)

nao_financeiro_stream = env.from_source(
    nao_financeiro_source,
    WatermarkStrategy.no_watermarks(),
    "nao-financeiro"
)


financeiro_quality = financeiro_stream.map(validate_financeiro)
nao_financeiro_quality = nao_financeiro_stream.map(validate_nao_financeiro)


financeiro_quality.print("DQ FINANCEIRO")
nao_financeiro_quality.print("DQ NAO-FINANCEIRO")


env.execute("Data Quality Monitoring")