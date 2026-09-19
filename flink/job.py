import json

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource


env = StreamExecutionEnvironment.get_execution_environment()

financeiro = KafkaSource.builder() \
    .set_bootstrap_servers("kafka:29092") \
    .set_topics("financeiro") \
    .set_group_id("data-quality-flink") \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

nao_financeiro = KafkaSource.builder() \
    .set_bootstrap_servers("kafka:29092") \
    .set_topics("nao-financeiro") \
    .set_group_id("data-quality-flink") \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

financeiro_stream = env.from_source(
    financeiro,
    WatermarkStrategy.no_watermarks(),
    "financeiro"
)

nao_financeiro_stream = env.from_source(
    nao_financeiro,
    WatermarkStrategy.no_watermarks(),
    "nao-financeiro"
)

financeiro_stream.print("FINANCEIRO")
nao_financeiro_stream.print("NAO-FINANCEIRO")

env.execute("Data Quality Monitoring")