"""Persistência dos resultados de qualidade em tabela Iceberg (lakehouse.quality.check_results)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType

if TYPE_CHECKING:
    from checks import CheckResult

CHECK_RESULTS_TABLE = "lakehouse.quality.check_results"

_SCHEMA = StructType(
    [
        StructField("check_id", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("check_description", StringType(), True),
        StructField("severity", StringType(), False),
        StructField("status", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("records_checked", LongType(), False),
        StructField("records_failed", LongType(), False),
        StructField("failure_rate", DoubleType(), False),
        StructField("details", StringType(), True),
        StructField("executed_at", TimestampType(), False),
        StructField("pipeline_run_id", StringType(), False),
        StructField("duration_seconds", DoubleType(), False),
    ]
)


def ensure_table(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.quality")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {CHECK_RESULTS_TABLE} (
            check_id          STRING,
            check_name        STRING,
            check_description STRING,
            severity          STRING,
            status            STRING,
            layer             STRING,
            table_name        STRING,
            records_checked   BIGINT,
            records_failed    BIGINT,
            failure_rate      DOUBLE,
            details           STRING,
            executed_at       TIMESTAMP,
            pipeline_run_id   STRING,
            duration_seconds  DOUBLE
        )
        USING iceberg
        PARTITIONED BY (days(executed_at))
        TBLPROPERTIES ('format-version' = '2')
        """
    )


def write_check_results(spark: SparkSession, results: list["CheckResult"]) -> int:
    """Acrescenta os resultados desta execução na tabela de log.

    Append-only de propósito: cada linha é um evento imutável ("nesta
    execução, este check deu tal resultado") — diferente das tabelas
    bronze/silver/gold, aqui não existe uma "versão atual" para fazer
    upsert; o histórico completo de execuções é o valor da tabela (é o que
    permite, por exemplo, plotar a taxa de falha de um check ao longo do
    tempo).
    """
    ensure_table(spark)
    if not results:
        return 0

    rows = [
        (
            r.check_id,
            r.check_name,
            r.check_description,
            r.severity,
            r.status,
            r.layer,
            r.table_name,
            int(r.records_checked),
            int(r.records_failed),
            float(r.failure_rate),
            json.dumps(r.details, default=str, ensure_ascii=False),
            r.executed_at,
            r.pipeline_run_id,
            float(r.duration_seconds),
        )
        for r in results
    ]
    df = spark.createDataFrame(rows, schema=_SCHEMA)
    df.writeTo(CHECK_RESULTS_TABLE).append()
    return len(rows)
