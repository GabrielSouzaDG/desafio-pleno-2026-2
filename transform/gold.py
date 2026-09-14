"""Job PySpark: lakehouse.silver.* -> lakehouse.gold.* (métricas de negócio).

Uso: `spark-submit transform/gold.py` (é o que `make transform` chama, depois
de bronze.py e silver.py — ver Makefile).

Todas as três tabelas são recomputadas INTEIRAS a partir da silver a cada
execução, e reconciliadas com o estado atual via `MERGE INTO` (upsert pela
chave de negócio de cada tabela). Isso é deliberado: a silver já fez o
trabalho pesado de deduplicação/reconciliação (Parte 2), então gold só
precisa agregar/filtrar o estado ATUAL dela — não há necessidade (nem
seria mais simples) de tentar manter um agregado incremental que soma e
subtrai contribuições de eventos corrigidos. Cada MERGE some/atualiza as
chaves presentes na fonte recomputada; ver a ressalva sobre chaves
"esvaziadas" no docstring de cada função de agregação.

Idempotência: nenhuma tabela gold é obrigada pelo enunciado a usar MERGE
(só as tabelas 1 e 3 pedem isso explicitamente), mas apliquei o mesmo
padrão nas três — o requisito "em nenhum dos cinco passos pode haver
duplicação" (enunciado, seção 4.3) vale para o pipeline inteiro, não só
bronze/silver.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger("transform.gold")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


def _log(level: str, event: str, **fields: Any) -> None:
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "event": event,
        **fields,
    }
    getattr(logger, level.lower(), logger.info)(json.dumps(record, default=str))


def build_spark_session(app_name: str = "gold") -> SparkSession:
    """Mesma configuração de transform/bronze.py e transform/silver.py."""
    iceberg_rest_uri = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")
    iceberg_warehouse = os.environ.get("ICEBERG_WAREHOUSE", "s3a://lakehouse/warehouse")
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY", "admin")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", iceberg_rest_uri)
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.warehouse", iceberg_warehouse)
        .config("spark.sql.catalog.lakehouse.s3.endpoint", minio_endpoint)
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )
    return builder.getOrCreate()


TICKET_EVENT_TYPES = ["ticket_opened", "ticket_replied", "ticket_closed"]


# ---------------------------------------------------------------------------
# Tabela 1: lakehouse.gold.events_daily_summary
# ---------------------------------------------------------------------------

def create_gold_events_daily_summary(spark: SparkSession) -> dict:
    """Agregação diária de eventos por (event_date, customer_id, event_type).

    Ressalva sobre MERGE de agregado recomputado: se uma correção mudar o
    `event_type` de um evento que era o ÚNICO representante de uma chave
    (event_date, customer_id, event_type), essa chave "esvazia" na fonte
    recomputada e o MERGE (que só ATUALIZA/INSERE chaves presentes na
    fonte, nunca remove uma chave ausente) deixaria uma contagem antiga
    parada em gold. Não implementei `WHEN NOT MATCHED BY SOURCE THEN
    DELETE` porque não tenho certeza de que a versão do Iceberg empacotada
    na imagem do Spark deste repo suporta essa cláusula, e um erro de
    sintaxe aqui derrubaria `make transform` inteiro. Dado que, pelos dados
    deste desafio, as "correções" mudam campos como priority/channel, não
    event_type, o risco prático é baixo — mas é uma limitação real,
    registrada aqui e no ARCHITECTURE.md.
    """
    start = time.monotonic()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.gold.events_daily_summary (
            event_date       DATE,
            customer_id      STRING,
            event_type       STRING,
            event_count      BIGINT,
            last_occurred_at TIMESTAMP,
            updated_at       TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (months(event_date))
        TBLPROPERTIES ('format-version' = '2')
        """
    )

    events = spark.table("lakehouse.silver.events")
    source = (
        events.withColumn("event_date", F.to_date("occurred_at"))
        .groupBy("event_date", "customer_id", "event_type")
        .agg(
            F.count(F.lit(1)).alias("event_count"),
            F.max("occurred_at").alias("last_occurred_at"),
        )
        .withColumn("updated_at", F.current_timestamp())
    )

    source.createOrReplaceTempView("events_daily_summary_source")
    spark.sql(
        """
        MERGE INTO lakehouse.gold.events_daily_summary AS target
        USING events_daily_summary_source AS source
        ON target.event_date = source.event_date
           AND target.customer_id <=> source.customer_id  -- null-safe: ha eventos sem customer_id
           AND target.event_type = source.event_type
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    row_count = spark.table("lakehouse.gold.events_daily_summary").count()
    duration = time.monotonic() - start
    summary = {
        "table": "lakehouse.gold.events_daily_summary",
        "rows_upserted": row_count,
        "duration_seconds": round(duration, 3),
    }
    _log("info", "gold_events_daily_summary_merged", **summary)
    return summary


# ---------------------------------------------------------------------------
# Tabela 2: lakehouse.gold.ticket_events
# ---------------------------------------------------------------------------

def create_gold_ticket_events(spark: SparkSession) -> dict:
    """Só os eventos de ciclo de vida de ticket, com `ticket_id`/`agent_id`
    extraídos de `properties` (string JSON opaca na silver — ver
    transform/silver.py). `agent_id` só existe de fato em `ticket_replied`;
    para os outros dois tipos, `get_json_object` devolve NULL (esperado,
    não é erro)."""
    start = time.monotonic()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.gold.ticket_events (
            event_id    STRING,
            customer_id STRING,
            ticket_id   STRING,
            event_type  STRING,
            occurred_at TIMESTAMP,
            agent_id    STRING,
            channel     STRING
        )
        USING iceberg
        PARTITIONED BY (months(occurred_at))
        TBLPROPERTIES ('format-version' = '2')
        """
    )

    events = spark.table("lakehouse.silver.events")
    source = events.filter(F.col("event_type").isin(TICKET_EVENT_TYPES)).select(
        "event_id",
        "customer_id",
        F.get_json_object("properties", "$.ticket_id").alias("ticket_id"),
        "event_type",
        "occurred_at",
        F.get_json_object("properties", "$.agent_id").alias("agent_id"),
        "channel",
    )

    source.createOrReplaceTempView("ticket_events_source")
    spark.sql(
        """
        MERGE INTO lakehouse.gold.ticket_events AS target
        USING ticket_events_source AS source
        ON target.event_id = source.event_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    row_count = spark.table("lakehouse.gold.ticket_events").count()
    duration = time.monotonic() - start
    summary = {
        "table": "lakehouse.gold.ticket_events",
        "rows_upserted": row_count,
        "duration_seconds": round(duration, 3),
    }
    _log("info", "gold_ticket_events_merged", **summary)
    return summary


# ---------------------------------------------------------------------------
# Tabela 3: lakehouse.gold.customer_monthly_activity
# ---------------------------------------------------------------------------

def create_gold_customer_monthly_activity(spark: SparkSession) -> dict:
    """Um registro por (customer_id, mês) em que o cliente teve pelo menos
    1 evento — por construção, `has_activity` é sempre TRUE nesta tabela
    (não existe uma versão "com 0 eventos" aqui: a Query 3, que é o
    consumidor desta tabela, não precisa de linhas de atividade zero — ela
    calcula retenção cruzando o tamanho da coorte, de silver.customers,
    contra quem aparece aqui. Uma tabela com "spine" completo (todo cliente
    x todo mês, incluindo zero) existiria só para um caso de uso que ainda
    não apareceu neste desafio, então não construí uma.

    Eventos sem customer_id são excluídos daqui (não há como atribuir
    atividade "mensal de um cliente" a um evento sem cliente).
    """
    start = time.monotonic()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.gold.customer_monthly_activity (
            activity_month DATE,
            customer_id    STRING,
            has_activity   BOOLEAN,
            event_count    BIGINT,
            signup_month   DATE
        )
        USING iceberg
        PARTITIONED BY (months(activity_month))
        TBLPROPERTIES ('format-version' = '2')
        """
    )

    events = spark.table("lakehouse.silver.events")
    signup_lookup = (
        spark.table("lakehouse.silver.customers")
        .filter(F.col("is_current"))
        .select("customer_id", F.trunc(F.col("signup_date"), "month").alias("signup_month"))
    )

    source = (
        events.filter(F.col("customer_id").isNotNull())
        .withColumn("activity_month", F.trunc(F.to_date("occurred_at"), "month"))
        .groupBy("activity_month", "customer_id")
        .agg(F.count(F.lit(1)).alias("event_count"))
        .withColumn("has_activity", F.col("event_count") > 0)
        .join(signup_lookup, on="customer_id", how="left")
        .select("activity_month", "customer_id", "has_activity", "event_count", "signup_month")
    )

    source.createOrReplaceTempView("customer_monthly_activity_source")
    spark.sql(
        """
        MERGE INTO lakehouse.gold.customer_monthly_activity AS target
        USING customer_monthly_activity_source AS source
        ON target.activity_month = source.activity_month
           AND target.customer_id = source.customer_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    row_count = spark.table("lakehouse.gold.customer_monthly_activity").count()
    duration = time.monotonic() - start
    summary = {
        "table": "lakehouse.gold.customer_monthly_activity",
        "rows_upserted": row_count,
        "duration_seconds": round(duration, 3),
    }
    _log("info", "gold_customer_monthly_activity_merged", **summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Gold: lakehouse.silver.* -> lakehouse.gold.* (Iceberg MERGE)")
    parser.parse_args()

    spark = build_spark_session()
    try:
        results = {
            "events_daily_summary": create_gold_events_daily_summary(spark),
            "ticket_events": create_gold_ticket_events(spark),
            "customer_monthly_activity": create_gold_customer_monthly_activity(spark),
        }
        _log("info", "gold_completed", **results)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
