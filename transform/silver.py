"""Job PySpark: lakehouse.bronze.* -> lakehouse.silver.* (dedup via MERGE INTO).

Uso: `spark-submit transform/silver.py` (é o que `make transform` chama,
depois de bronze.py — ver Makefile).

silver.events
-------------
Uma linha por `event_id`, sempre a versão mais recente (`updated_at` mais
alto). `properties` continua sendo uma STRING JSON opaca (mesma decisão de
bronze.py, pelo mesmo motivo: as chaves variam por `event_type` e ganham
campos novos com o schema drift — tipar isso como STRUCT seria frágil).

Silver relê `lakehouse.bronze.events` INTEIRA a cada execução (não só o
que entrou hoje), porque bronze é append-only e pode ter acumulado
duplicatas de execuções anteriores (isso é intencional — ver bronze.py).
Um `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY updated_at DESC)`
reduz esse histórico completo a um candidato por `event_id`, e um
`MERGE INTO` reconcilia esse candidato com o que já está na silver,
atualizando só quando `updated_at` realmente avançou.

Escala: reler bronze inteira a cada run é aceitável para o volume deste
desafio (dataset pequeno de propósito), mas não escala — com 100x mais
dado, o `ROW_NUMBER` sobre a tabela inteira vira o gargalo (shuffle sobre
tudo, toda execução). O que eu faria diferente: filtrar bronze por
`_ingested_at > watermark_do_ultimo_run_da_silver` antes do dedup, para o
MERGE processar só o incremento — ver ARCHITECTURE.md.

silver.customers (SCD Tipo 2)
------------------------------
Mantém o HISTÓRICO de mudanças de `plan`, `segment`, `is_active` e
`country` por cliente, para responder "qual era o plano do cliente na data
do evento" (ver `PLAN_AS_OF_EVENT_QUERY` no final deste módulo).

Implementado com DOIS `MERGE INTO` (não dá para fazer em um só: um único
`MERGE INTO ... ON customer_id` só pode ou casar com a versão atual, ou
não casar com nada — não existe "casar E inserir uma linha nova" na mesma
cláusula. Por isso, primeiro se FECHA a versão atual dos clientes que
mudaram (`valid_to`, `is_current = false`); só depois, num segundo
`MERGE INTO` sobre o estado já commitado pelo primeiro, se ABRE a nova
versão current — tanto para quem acabou de ser fechado quanto para
cliente novo, que nunca teve nenhuma linha):

  MERGE #1 (fecha)  -> só mexe em quem já tinha uma versão current E mudou
  MERGE #2 (abre)   -> INSERT pra quem não tem versão current (novo OU
                        acabou de ser fechado pelo #1)

Limitação conhecida: como a silver dedup bronze pro estado MAIS RECENTE por
cliente antes de comparar, se a silver.customers NUNCA tivesse rodado antes
de duas cargas diferentes (ex.: batch1 e batch2) já estarem acumuladas em
bronze, uma transição intermediária seria perdida (só a versão final vira
uma linha). Isso não acontece na sequência de avaliação do desafio (a
silver roda entre um batch e outro), mas é uma limitação real que num
sistema de produção eu resolveria processando bronze por
`_ingested_at`/`_batch_id` incrementalmente, não pelo "estado mais recente
global" — ver ARCHITECTURE.md.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger("transform.silver")
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


def build_spark_session(app_name: str = "silver") -> SparkSession:
    """Mesma configuração de transform/bronze.py — ver docstring lá para o
    porquê dos defaults apontarem para os hostnames internos do compose."""
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


# ---------------------------------------------------------------------------
# silver.events
# ---------------------------------------------------------------------------

def _events_dedup_window() -> "Window":
    # Construida dentro de uma funcao (nao no nivel do modulo) de proposito:
    # Window.partitionBy(...) precisa de uma SparkSession ja ativa
    # (pyspark.sql.utils.get_active_spark_context), e codigo de modulo roda
    # no import, antes de build_spark_session() existir.
    return Window.partitionBy("event_id").orderBy(
        # updated_at eh o criterio de negocio (versao mais recente vence).
        # _ingested_at/_batch_id so entram para desempatar duplicatas EXATAS
        # (mesmo event_id, mesmo updated_at -- ex.: bronze relendo o mesmo
        # arquivo raw de novo numa segunda execucao no mesmo dia) de forma
        # deterministica, para o ROW_NUMBER nunca depender de ordem arbitraria
        # do Spark. Como duplicatas exatas tem os MESMOS valores de negocio,
        # qual delas "vence" aqui nao muda nenhum campo do resultado final.
        F.col("updated_at").desc(),
        F.col("_ingested_at").desc(),
        F.col("_batch_id").desc(),
    )


def _log_properties_schema_snapshot(spark: SparkSession, df: DataFrame) -> None:
    """Best-effort: usa schema_of_json numa amostra de `properties` e loga
    o schema inferido -- um sinal de observabilidade de schema drift (o
    texto do schema muda quando um campo novo aparece), sem exigir que a
    coluna properties vire STRUCT de verdade (ela continua STRING; ver
    docstring do modulo)."""
    try:
        sample = df.filter(F.col("properties").isNotNull()).select("properties").limit(1).collect()
        if not sample:
            return
        inferred = spark.sql("SELECT schema_of_json(:sample) AS s", args={"sample": sample[0]["properties"]}).collect()
        _log("info", "properties_schema_snapshot", inferred_schema=inferred[0]["s"])
    except Exception as exc:  # observabilidade nao pode derrubar o job
        _log("warning", "properties_schema_snapshot_failed", error=str(exc))


def create_silver_events(spark: SparkSession) -> dict:
    start = time.monotonic()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.silver.events (
            event_id                STRING,
            customer_id             STRING,
            has_customer_id         BOOLEAN,
            event_type              STRING,
            occurred_at             TIMESTAMP,
            updated_at              TIMESTAMP,
            channel                 STRING,
            properties              STRING,
            properties_valid_json   BOOLEAN,
            _silver_loaded_at       TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (months(occurred_at))
        TBLPROPERTIES ('format-version' = '2')
        """
    )

    bronze = spark.table("lakehouse.bronze.events")
    bronze_rows_read = bronze.count()

    # occurred_at/updated_at ja chegam como TIMESTAMP (instante UTC) desde
    # o bronze -- ver transform/bronze.py:_parse_iso_utc. Nao ha o que
    # reconverter aqui, so propagar os tipos ja corretos.
    deduped = (
        bronze.withColumn("_rn", F.row_number().over(_events_dedup_window()))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    transformed = (
        deduped.withColumn("event_type", F.lower(F.col("event_type")))
        .withColumn("has_customer_id", F.col("customer_id").isNotNull())
        .withColumn(
            "properties_valid_json",
            F.col("properties").isNull() | F.get_json_object(F.col("properties"), "$").isNotNull(),
        )
        .withColumn("_silver_loaded_at", F.current_timestamp())
        .select(
            "event_id",
            "customer_id",
            "has_customer_id",
            "event_type",
            "occurred_at",
            "updated_at",
            "channel",
            "properties",
            "properties_valid_json",
            "_silver_loaded_at",
        )
        .cache()
    )

    candidates_after_dedup = transformed.count()
    _log_properties_schema_snapshot(spark, transformed)

    transformed.createOrReplaceTempView("silver_events_source")
    spark.sql(
        """
        MERGE INTO lakehouse.silver.events AS target
        USING silver_events_source AS source
        ON target.event_id = source.event_id
        WHEN MATCHED AND source.updated_at > target.updated_at THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    transformed.unpersist()

    final_count = spark.table("lakehouse.silver.events").count()
    duration = time.monotonic() - start
    summary = {
        "table": "lakehouse.silver.events",
        "bronze_rows_read": bronze_rows_read,
        "distinct_event_id_candidates": candidates_after_dedup,
        "silver_row_count_after_merge": final_count,
        "duration_seconds": round(duration, 3),
    }
    _log("info", "silver_events_merged", **summary)
    return summary


# ---------------------------------------------------------------------------
# silver.customers (SCD Tipo 2)
# ---------------------------------------------------------------------------

# campos cuja mudanca dispara uma nova versao. company_name e signup_date
# ficam de fora de proposito: sao (na pratica) imutaveis, e o enunciado
# pede explicitamente so plan/segment/is_active/country.
_SCD2_TRACKED_FIELDS = ["plan", "segment", "is_active", "country"]

def _customers_dedup_window() -> "Window":
    # mesmo motivo de _events_dedup_window(): precisa de SparkSession ativa.
    return Window.partitionBy("customer_id").orderBy(
        F.col("updated_at").desc(),
        F.col("_ingested_at").desc(),
        F.col("_batch_id").desc(),
    )


def _fields_changed(fields: list[str]) -> Column:
    """NOT (src.f1 <=> cur.f1 AND src.f2 <=> cur.f2 AND ...) -- `<=>` eh a
    igualdade null-safe do Spark (NULL <=> NULL = true), entao um campo que
    era e continua NULL nao conta como "mudou"."""
    all_equal = F.lit(True)
    for f in fields:
        all_equal = all_equal & F.expr(f"src.{f} <=> cur.{f}")
    return ~all_equal


def create_silver_customers(spark: SparkSession) -> dict:
    start = time.monotonic()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.silver.customers (
            customer_id       STRING,
            company_name      STRING,
            plan              STRING,
            segment           STRING,
            signup_date       DATE,
            country           STRING,
            is_active         BOOLEAN,
            updated_at        TIMESTAMP,
            valid_from        TIMESTAMP,
            valid_to          TIMESTAMP,
            is_current        BOOLEAN,
            _record_version   INT,
            _silver_loaded_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (months(valid_from))
        TBLPROPERTIES ('format-version' = '2')
        """
    )

    bronze = spark.table("lakehouse.bronze.customers")
    bronze_rows_read = bronze.count()

    # estado mais recente conhecido por cliente, olhando bronze inteira
    # (ver limitacao documentada no topo do modulo)
    latest_bronze = (
        bronze.withColumn("_rn", F.row_number().over(_customers_dedup_window()))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select(
            "customer_id", "company_name", "plan", "segment",
            "signup_date", "country", "is_active", "updated_at",
        )
    )

    current = spark.table("lakehouse.silver.customers").filter(F.col("is_current"))
    max_versions = (
        spark.table("lakehouse.silver.customers")
        .groupBy("customer_id")
        .agg(F.max("_record_version").alias("max_version"))
    )

    joined = latest_bronze.alias("src").join(
        current.alias("cur"),
        F.col("src.customer_id") == F.col("cur.customer_id"),
        "left",
    )

    is_new = F.col("cur.customer_id").isNull()
    is_changed = _fields_changed(_SCD2_TRACKED_FIELDS)
    is_newer = F.col("src.updated_at") > F.col("cur.updated_at")
    needs_new_version = is_new | (is_changed & is_newer)

    # MERGE #1 -- fecha a versao current de quem mudou (nunca mexe em
    # cliente novo: nao ha nada pra fechar)
    to_close = joined.filter(~is_new & is_changed & is_newer).select(
        F.col("src.customer_id").alias("customer_id"),
        F.col("src.updated_at").alias("new_valid_to"),
    )
    to_close_count = to_close.count()

    if to_close_count > 0:
        to_close.createOrReplaceTempView("customers_to_close")
        spark.sql(
            """
            MERGE INTO lakehouse.silver.customers AS target
            USING customers_to_close AS source
            ON target.customer_id = source.customer_id AND target.is_current = true
            WHEN MATCHED THEN UPDATE SET
                valid_to = source.new_valid_to,
                is_current = false
            """
        )

    # MERGE #2 -- abre a nova versao current: cliente novo (nunca teve
    # linha) OU cliente que acabou de ser fechado pelo MERGE #1 acima. A
    # condicao "ON ... AND target.is_current = true" garante que, apos o
    # MERGE #1 ja commitado, nenhum desses dois casos "casa" com o target
    # (nao ha mais linha is_current=true pra eles) -> WHEN NOT MATCHED
    # dispara o INSERT pros dois. Cliente sem mudanca real permanece
    # casando (sua linha current nao foi tocada pelo MERGE #1) -> nenhuma
    # clausula MATCHED aqui -> no-op, que eh exatamente o requisito "d".
    new_versions = (
        joined.filter(needs_new_version)
        .select("src.*")
        .join(max_versions, on="customer_id", how="left")
        .withColumn("_record_version", F.coalesce(F.col("max_version"), F.lit(0)) + F.lit(1))
        .withColumn("valid_from", F.col("updated_at"))
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("_silver_loaded_at", F.current_timestamp())
        .drop("max_version")
        .select(
            "customer_id", "company_name", "plan", "segment", "signup_date",
            "country", "is_active", "updated_at", "valid_from", "valid_to",
            "is_current", "_record_version", "_silver_loaded_at",
        )
    )
    new_versions_count = new_versions.count()

    if new_versions_count > 0:
        new_versions.createOrReplaceTempView("customers_new_versions")
        spark.sql(
            """
            MERGE INTO lakehouse.silver.customers AS target
            USING customers_new_versions AS source
            ON target.customer_id = source.customer_id AND target.is_current = true
            WHEN NOT MATCHED THEN INSERT *
            """
        )

    final_count = spark.table("lakehouse.silver.customers").count()
    current_count = spark.table("lakehouse.silver.customers").filter(F.col("is_current")).count()

    duration = time.monotonic() - start
    summary = {
        "table": "lakehouse.silver.customers",
        "bronze_rows_read": bronze_rows_read,
        "distinct_customer_candidates": latest_bronze.count(),
        "versions_closed": to_close_count,
        "versions_opened": new_versions_count,
        "silver_row_count_total": final_count,
        "silver_row_count_current": current_count,
        "duration_seconds": round(duration, 3),
    }
    _log("info", "silver_customers_merged", **summary)
    return summary


# ---------------------------------------------------------------------------
# Como consultar "qual era o plano do cliente na data do evento"
# ---------------------------------------------------------------------------
# valid_from eh inclusivo, valid_to eh exclusivo (valid_to IS NULL = versao
# ainda vigente) -- as duas datas formam um intervalo meio-aberto
# [valid_from, valid_to) sem gap nem overlap entre versoes consecutivas do
# mesmo cliente, porque valid_to da versao antiga = valid_from da versao
# nova (ver o UPDATE em create_silver_customers).
PLAN_AS_OF_EVENT_QUERY = """
SELECT
    e.event_id,
    e.customer_id,
    e.occurred_at,
    c.plan    AS plan_at_event_time,
    c.segment AS segment_at_event_time
FROM lakehouse.silver.events e
JOIN lakehouse.silver.customers c
  ON c.customer_id = e.customer_id
 AND c.valid_from <= e.occurred_at
 AND (c.valid_to > e.occurred_at OR c.valid_to IS NULL)
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Silver: lakehouse.bronze.* -> lakehouse.silver.* (Iceberg MERGE)")
    parser.add_argument(
        "--only",
        choices=["events", "customers", "both"],
        default="both",
        help=(
            "Roda so uma das duas cargas. `make transform` chama sem essa flag "
            "(default 'both'); a DAG do Airflow usa 'events'/'customers' para "
            "que task_silver_events dependa so de task_bronze_events."
        ),
    )
    args = parser.parse_args()

    spark = build_spark_session()
    try:
        events_summary = create_silver_events(spark) if args.only in ("events", "both") else None
        customers_summary = create_silver_customers(spark) if args.only in ("customers", "both") else None
        _log("info", "silver_completed", events=events_summary, customers=customers_summary)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
