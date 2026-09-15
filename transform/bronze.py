"""Job PySpark: raw (MinIO) -> lakehouse.bronze.* (Iceberg, append-only).

Uso: `spark-submit transform/bronze.py [--ingestion-date YYYY-MM-DD]`
(é o que `make transform` chama dentro do container do Spark — ver Makefile).

Estratégia de leitura — "ler como string e parsear depois"
-------------------------------------------------------------
Em vez de `spark.read.json(path)` (que força o Spark a INFERIR um schema
estrutural único olhando todos os arquivos do batch), este job lê cada
arquivo como TEXTO puro (`spark.read.text`, uma linha = um JSON bruto) e
extrai só os campos que o bronze realmente precisa tipar, usando
`get_json_object` — uma função que devolve `null` se o campo não existir
em vez de falhar.

Por quê: a inferência estrutural do `spark.read.json()` tem dois problemas
reais para este dataset:
  1. `properties` tem chaves completamente diferentes por `event_type`
     (ticket_opened, login, feature_used, ...) — inferir isso como STRUCT
     produz um schema gigante, cheio de null, e frágil a qualquer eventype
     novo que apareça no futuro.
  2. schema drift de verdade (um campo novo, tipos que mudam) quebra a
     inferência se dois arquivos lidos juntos tiverem tipos incompatíveis
     para a mesma chave — e o pipeline não pode quebrar por causa disso
     (requisito explícito do desafio).

Lendo como texto e extraindo campo a campo, o bronze nunca falha por causa
de um campo novo: ele simplesmente não vira uma coluna tipada até alguém
decidir extrair (o valor continua acessível via `properties` ou via
`_raw_payload`, que preserva a linha JSON completa, exatamente como veio
da raw zone). Isso é o "schema flexível" pedido, de forma literal.

Idempotência
-------------
Bronze é append-only e ACEITA duplicatas de propósito: rodar esta função
duas vezes para o mesmo `ingestion_date` (ex.: reprocessamento, ou a raw
daquele dia crescer depois de um `make batch2` no mesmo dia) insere as
linhas de novo, com um `_batch_id` novo. A garantia de "resultado final
sem duplicata" não é responsabilidade do bronze — é a silver, via
`MERGE INTO` por chave de negócio + `updated_at` (usando `_batch_id`/
`_ingested_at` como critério de desempate quando duas linhas têm o mesmo
`updated_at`), que resolve isso. Bronze só garante que nenhum dado bruto
que chegou na raw fica de fora do lakehouse.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger("transform.bronze")
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


def build_spark_session(app_name: str = "bronze") -> SparkSession:
    """Cria a SparkSession já apontando para o catálogo Iceberg REST
    "lakehouse" e para o MinIO via S3A.

    Os defaults abaixo usam os hostnames INTERNOS da rede docker
    (`iceberg-rest`, `minio`) porque é assim que `make transform` roda
    este script (`docker compose exec spark spark-submit ...`) — dentro do
    container, "localhost" seria o próprio container do Spark, não os
    outros serviços. Esses mesmos valores já estão em
    conf/spark/spark-defaults.conf (montado no container), então declarar
    de novo aqui é redundante nesse caminho de execução — mas deixa o
    script autocontido e testável fora do compose (ex.: `spark-submit
    --master local[*] transform/bronze.py` no host, com
    ICEBERG_REST_URI=http://localhost:8181 e
    MINIO_ENDPOINT=http://localhost:9000 no ambiente).
    """
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


def _parse_iso_utc(col: Column) -> Column:
    """Converte uma string ISO-8601 UTC (terminada em 'Z', com ou sem
    fração de segundo) para TIMESTAMP. `coalesce` tenta o formato com
    microssegundos (como ingestion/postgres_client.py grava `updated_at`
    de customers) e cai para o formato sem fração (como a Mock API grava
    os eventos) — assim a mesma função serve para as duas fontes sem
    precisar saber de antemão qual delas está sendo processada."""
    with_fraction = F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
    without_fraction = F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss'Z'")
    return F.coalesce(with_fraction, without_fraction)


def _glob_has_matches(spark: SparkSession, path_pattern: str) -> bool:
    """Verifica se o glob (ex.: .../ingestion_date=2026-08-31/part-*.json.gz)
    casa com pelo menos um arquivo, sem disparar uma leitura completa.
    Evita que o job quebre com AnalysisException num dia sem dado (ex.:
    ingestão ainda não rodou para essa data) — nesse caso, logamos e
    devolvemos uma carga vazia em vez de derrubar o spark-submit."""
    hadoop_conf = spark._jsc.hadoopConfiguration()
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(path_pattern)
    fs = jvm_path.getFileSystem(hadoop_conf)
    statuses = fs.globStatus(jvm_path)
    return statuses is not None and len(statuses) > 0


def _raw_path(source: str, ingestion_date: str) -> str:
    bucket = os.environ.get("RAW_BUCKET", "lakehouse")
    return f"s3a://{bucket}/raw/{source}/ingestion_date={ingestion_date}/part-*.json.gz"


def _read_raw_lines(spark: SparkSession, source: str, ingestion_date: str) -> DataFrame:
    path = _raw_path(source, ingestion_date)
    if not _glob_has_matches(spark, path):
        _log("warning", "bronze_no_raw_files", source=source, ingestion_date=ingestion_date, path=path)
        return spark.createDataFrame([], "value STRING")

    df = spark.read.text(path).withColumnRenamed("value", "_raw_payload")
    df = df.withColumn("_source_file", F.input_file_name())
    # linhas em branco (ex.: newline sobrando no fim do gzip) não viram
    # registro nenhum -- get_json_object nelas so devolveria null em tudo.
    return df.filter(F.length(F.trim(F.col("_raw_payload"))) > 0)


def _with_control_columns(df: DataFrame, batch_id: str) -> DataFrame:
    return df.withColumn("_ingested_at", F.current_timestamp()).withColumn("_batch_id", F.lit(batch_id))


def create_bronze_events(spark: SparkSession, ingestion_date: str) -> dict:
    """Lê `raw/events/ingestion_date=<data>/part-*.json.gz`, extrai os
    campos conhecidos e faz append em `lakehouse.bronze.events`.
    Retorna um resumo (contagens) para uso pelos checks de qualidade."""
    batch_id = str(uuid.uuid4())
    raw = _read_raw_lines(spark, "events", ingestion_date)

    parsed = raw.select(
        F.get_json_object("_raw_payload", "$.event_id").alias("event_id"),
        F.get_json_object("_raw_payload", "$.customer_id").alias("customer_id"),
        F.get_json_object("_raw_payload", "$.event_type").alias("event_type"),
        _parse_iso_utc(F.get_json_object("_raw_payload", "$.occurred_at")).alias("occurred_at"),
        _parse_iso_utc(F.get_json_object("_raw_payload", "$.updated_at")).alias("updated_at"),
        F.get_json_object("_raw_payload", "$.channel").alias("channel"),
        # properties fica como string JSON bruta -- suas chaves variam por
        # event_type e ganham campos novos (ex.: sla_minutes) com o schema
        # drift; nao ha necessidade de tipar isso para o bronze funcionar,
        # e assim nenhum campo novo dentro de properties pode quebrar o job.
        F.get_json_object("_raw_payload", "$.properties").alias("properties"),
        "_raw_payload",
        "_source_file",
    )

    events = _with_control_columns(parsed, batch_id)
    events.cache()
    records_read = events.count()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.events (
            event_id      STRING,
            customer_id   STRING,
            event_type    STRING,
            occurred_at   TIMESTAMP,
            updated_at    TIMESTAMP,
            channel       STRING,
            properties    STRING,
            _raw_payload  STRING,
            _source_file  STRING,
            _ingested_at  TIMESTAMP,
            _batch_id     STRING
        )
        USING iceberg
        PARTITIONED BY (days(occurred_at))
        TBLPROPERTIES ('format-version' = '2')
        """
    )

    events.select(
        "event_id",
        "customer_id",
        "event_type",
        "occurred_at",
        "updated_at",
        "channel",
        "properties",
        "_raw_payload",
        "_source_file",
        "_ingested_at",
        "_batch_id",
    ).writeTo("lakehouse.bronze.events").append()

    events.unpersist()
    summary = {
        "table": "lakehouse.bronze.events",
        "ingestion_date": ingestion_date,
        "batch_id": batch_id,
        "records_read": records_read,
        "records_written": records_read,  # bronze nao filtra nem dedup: 1 linha lida = 1 linha escrita
    }
    _log("info", "bronze_events_written", **summary)
    return summary


def create_bronze_customers(spark: SparkSession, ingestion_date: str) -> dict:
    """Lê `raw/customers/ingestion_date=<data>/part-*.json.gz`, extrai os
    campos conhecidos e faz append em `lakehouse.bronze.customers`.
    Retorna um resumo (contagens) para uso pelos checks de qualidade."""
    batch_id = str(uuid.uuid4())
    raw = _read_raw_lines(spark, "customers", ingestion_date)

    parsed = raw.select(
        F.get_json_object("_raw_payload", "$.customer_id").alias("customer_id"),
        F.get_json_object("_raw_payload", "$.company_name").alias("company_name"),
        F.get_json_object("_raw_payload", "$.plan").alias("plan"),
        F.get_json_object("_raw_payload", "$.segment").alias("segment"),
        F.to_date(F.get_json_object("_raw_payload", "$.signup_date")).alias("signup_date"),
        F.get_json_object("_raw_payload", "$.country").alias("country"),
        F.get_json_object("_raw_payload", "$.is_active").cast("boolean").alias("is_active"),
        _parse_iso_utc(F.get_json_object("_raw_payload", "$.updated_at")).alias("updated_at"),
        "_raw_payload",
        "_source_file",
    )

    customers = _with_control_columns(parsed, batch_id)
    customers.cache()
    records_read = customers.count()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.customers (
            customer_id   STRING,
            company_name  STRING,
            plan          STRING,
            segment       STRING,
            signup_date   DATE,
            country       STRING,
            is_active     BOOLEAN,
            updated_at    TIMESTAMP,
            _raw_payload  STRING,
            _source_file  STRING,
            _ingested_at  TIMESTAMP,
            _batch_id     STRING
        )
        USING iceberg
        PARTITIONED BY (days(updated_at))
        TBLPROPERTIES ('format-version' = '2')
        """
    )

    customers.select(
        "customer_id",
        "company_name",
        "plan",
        "segment",
        "signup_date",
        "country",
        "is_active",
        "updated_at",
        "_raw_payload",
        "_source_file",
        "_ingested_at",
        "_batch_id",
    ).writeTo("lakehouse.bronze.customers").append()

    customers.unpersist()
    summary = {
        "table": "lakehouse.bronze.customers",
        "ingestion_date": ingestion_date,
        "batch_id": batch_id,
        "records_read": records_read,
        "records_written": records_read,
    }
    _log("info", "bronze_customers_written", **summary)
    return summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bronze: raw (MinIO) -> lakehouse.bronze.* (Iceberg)")
    parser.add_argument(
        "--ingestion-date",
        dest="ingestion_date",
        default=None,
        help="Data de ingestao (YYYY-MM-DD) a processar. Default: hoje (UTC) ou env var INGESTION_DATE.",
    )
    parser.add_argument(
        "--only",
        choices=["events", "customers", "both"],
        default="both",
        help=(
            "Roda so uma das duas cargas. `make transform` chama sem essa flag "
            "(default 'both', comportamento inalterado); a DAG do Airflow "
            "(dags/lakehouse_pipeline.py) usa 'events'/'customers' para que "
            "task_bronze_events dependa so de task_ingest_api, sem esperar "
            "a ingestao do Postgres terminar."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    ingestion_date = args.ingestion_date or os.environ.get("INGESTION_DATE") or datetime.now(timezone.utc).strftime(
        "%Y-%m-%d"
    )

    spark = build_spark_session()
    try:
        events_summary = create_bronze_events(spark, ingestion_date) if args.only in ("events", "both") else None
        customers_summary = (
            create_bronze_customers(spark, ingestion_date) if args.only in ("customers", "both") else None
        )
        _log(
            "info",
            "bronze_completed",
            ingestion_date=ingestion_date,
            events=events_summary,
            customers=customers_summary,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
