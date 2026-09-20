import json

from pyflink.datastream.functions import WindowFunction
from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema, ByteArraySchema
from pyflink.common.typeinfo import Types
from pyflink.common.time import Time

from pyflink.datastream import StreamExecutionEnvironment

from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
    DeliveryGuarantee
)

from pyflink.datastream.window import TumblingProcessingTimeWindows


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

    return {
        "domain": "financeiro",
        "quality": "INVALID" if problems else "VALID",
        "timestamp": event.get("timestamp"),
        "event_id": event.get("transaction_id"),
        "problems": problems,
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

    return {
        "domain": "nao-financeiro",
        "quality": "INVALID" if problems else "VALID",
        "timestamp": event.get("timestamp"),
        "event_id": event.get("event_id"),
        "problems": problems,
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


financeiro_quality = financeiro_stream.map(
    validate_financeiro
)


nao_financeiro_quality = nao_financeiro_stream.map(
    validate_nao_financeiro
)


financeiro_quality.print("DQ FINANCEIRO")

nao_financeiro_quality.print("DQ NAO-FINANCEIRO")

violations = financeiro_quality.union(
    nao_financeiro_quality
) \
    .filter(lambda result: result["quality"] == "INVALID") \
    .map(lambda result: {
        "topic": "data-quality-events",
        "value": json.dumps(result).encode("utf-8")
    })


all_quality = financeiro_quality.union(
    nao_financeiro_quality
)


def to_metric(result):
    return (
        result["domain"],
        1,
        1 if result["quality"] == "VALID" else 0,
        1 if result["quality"] == "INVALID" else 0
    )


metric_events = all_quality.map(
    to_metric,
    output_type=Types.TUPLE([
        Types.STRING(),
        Types.INT(),
        Types.INT(),
        Types.INT()
    ])
)

class MetricsWindowFunction(WindowFunction):

    def apply(self, key, window, values):
        processed = 0
        valid = 0
        invalid = 0

        for value in values:
            processed += value[1]
            valid += value[2]
            invalid += value[3]

        quality_rate = valid / processed if processed > 0 else 0

        yield {
            "domain": key,
            "processed": processed,
            "valid": valid,
            "invalid": invalid,
            "quality_rate": quality_rate
        }

metrics = metric_events \
    .key_by(lambda value: value[0]) \
    .window(
        TumblingProcessingTimeWindows.of(
            Time.minutes(1)
        )
    ) \
    .apply(
        MetricsWindowFunction()
    )

metrics = metrics.map(
    lambda metric: {
        "topic": "data-quality-metrics",
        "value": json.dumps(metric).encode("utf-8")
    }
)


output_stream = violations.union(metrics)


quality_sink = KafkaSink.builder() \
    .set_bootstrap_servers("kafka:29092") \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
        .set_topic_selector(
            lambda value: value["topic"]
        )
        .set_value_serialization_schema(
            ByteArraySchema()
        )
        .build()
    ) \
    .set_delivery_guarantee(
        DeliveryGuarantee.NONE
    ) \
    .build()


output_stream.sink_to(quality_sink)


env.execute("Data Quality Monitoring")