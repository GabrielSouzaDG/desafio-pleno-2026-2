"""Testes das funções de transformação (bronze, silver) com SparkSession local.

Diferente de tests/test_ingestion.py, aqui não há mock: os testes chamam
`create_silver_events`/`create_silver_customers` de verdade (o mesmo
código que `make transform` roda), contra um catálogo Iceberg LOCAL
(`type=hadoop`, warehouse num diretório temporário) em vez do catálogo
REST + MinIO do docker-compose. Isso testa o `MERGE INTO`/SCD2 de verdade
(é SQL Iceberg de verdade rodando), só sem precisar dos containers de
infra no ar -- por isso "SparkSession local".

Requer PySpark + os jars do Iceberg no classpath. O host deste repo não
tem isso instalado (só o container do Spark tem); por isso este arquivo
roda via:

    docker compose exec spark python3 -m pytest /home/iceberg/work/tests/test_transforms.py -v

(`make test` continua rodando só test_ingestion.py no host, que é pure
Python -- ver Makefile).
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from transform.silver import create_silver_customers, create_silver_events

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("iceberg_warehouse")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test_transforms")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # catalogo LOCAL (hadoop), sem depender de iceberg-rest/MinIO --
        # e o que torna este teste de verdade "SparkSession local".
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", f"file://{warehouse}")
        # o container do Spark deste repo tem spark-defaults.conf com
        # spark.sql.catalog.lakehouse.io-impl=S3FileIO (para o catalogo
        # REST real, ver conf/spark/spark-defaults.conf) -- isso vaza pra
        # qualquer SparkSession criada nesse container, inclusive esta.
        # Sem sobrescrever aqui, o S3FileIO tenta interpretar o warehouse
        # local (file://...) como uma URI S3 e quebra com
        # "Invalid S3 URI, cannot determine scheme". HadoopFileIO e o
        # correto para um catalogo hadoop/filesystem local.
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def _reset_tables(spark):
    """Cada teste começa com bronze/silver vazios. A SparkSession é
    session-scoped (cara de criar), então o isolamento entre testes vem
    de derrubar as tabelas antes de cada um, não de recriar a sessão."""
    for table in (
        "lakehouse.bronze.events",
        "lakehouse.bronze.customers",
        "lakehouse.silver.events",
        "lakehouse.silver.customers",
    ):
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    yield


# ---------------------------------------------------------------------------
# Helpers de seed (schema espelha transform/bronze.py)
# ---------------------------------------------------------------------------

_BRONZE_EVENTS_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("event_type", StringType()),
        StructField("occurred_at", TimestampType()),
        StructField("updated_at", TimestampType()),
        StructField("channel", StringType()),
        StructField("properties", StringType()),
        StructField("_raw_payload", StringType()),
        StructField("_source_file", StringType()),
        StructField("_ingested_at", TimestampType()),
        StructField("_batch_id", StringType()),
    ]
)

_BRONZE_CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType()),
        StructField("company_name", StringType()),
        StructField("plan", StringType()),
        StructField("segment", StringType()),
        StructField("signup_date", DateType()),
        StructField("country", StringType()),
        StructField("is_active", BooleanType()),
        StructField("updated_at", TimestampType()),
        StructField("_raw_payload", StringType()),
        StructField("_source_file", StringType()),
        StructField("_ingested_at", TimestampType()),
        StructField("_batch_id", StringType()),
    ]
)


def _seed_bronze_events(spark: SparkSession, rows: list[dict]) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.events (
            event_id STRING, customer_id STRING, event_type STRING,
            occurred_at TIMESTAMP, updated_at TIMESTAMP, channel STRING,
            properties STRING, _raw_payload STRING, _source_file STRING,
            _ingested_at TIMESTAMP, _batch_id STRING
        ) USING iceberg PARTITIONED BY (days(occurred_at))
        """
    )
    spark.createDataFrame(rows, schema=_BRONZE_EVENTS_SCHEMA).writeTo("lakehouse.bronze.events").append()


def _seed_bronze_customers(spark: SparkSession, rows: list[dict]) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.customers (
            customer_id STRING, company_name STRING, plan STRING, segment STRING,
            signup_date DATE, country STRING, is_active BOOLEAN, updated_at TIMESTAMP,
            _raw_payload STRING, _source_file STRING, _ingested_at TIMESTAMP, _batch_id STRING
        ) USING iceberg PARTITIONED BY (days(updated_at))
        """
    )
    spark.createDataFrame(rows, schema=_BRONZE_CUSTOMERS_SCHEMA).writeTo("lakehouse.bronze.customers").append()


def _event_row(event_id: str, updated_at: datetime, **overrides) -> dict:
    row = {
        "event_id": event_id,
        "customer_id": "c_1",
        "event_type": "login",
        "occurred_at": datetime(2026, 6, 1),
        "updated_at": updated_at,
        "channel": "web",
        "properties": "{}",
        "_raw_payload": "{}",
        "_source_file": "test",
        "_ingested_at": updated_at,
        "_batch_id": "b1",
    }
    row.update(overrides)
    return row


def _customer_row(customer_id: str, updated_at: datetime, plan: str = "pro", **overrides) -> dict:
    row = {
        "customer_id": customer_id,
        "company_name": "Acme",
        "plan": plan,
        "segment": "MidMarket",
        "signup_date": date(2025, 1, 1),
        "country": "BR",
        "is_active": True,
        "updated_at": updated_at,
        "_raw_payload": "{}",
        "_source_file": "test",
        "_ingested_at": updated_at,
        "_batch_id": "b1",
    }
    row.update(overrides)
    return row


# ---------------------------------------------------------------------------
# silver.events: deduplicação
# ---------------------------------------------------------------------------

def test_deduplication_keeps_latest(spark):
    base = datetime(2026, 6, 1)
    _seed_bronze_events(
        spark,
        [
            _event_row("e_dup", updated_at=base, channel="web"),
            _event_row("e_dup", updated_at=base + timedelta(hours=2), channel="mobile"),
        ],
    )

    create_silver_events(spark)

    rows = spark.table("lakehouse.silver.events").filter("event_id = 'e_dup'").collect()
    assert len(rows) == 1
    assert rows[0]["channel"] == "mobile"  # versao com updated_at mais recente venceu
    assert rows[0]["updated_at"] == base + timedelta(hours=2)


# ---------------------------------------------------------------------------
# silver.customers: SCD Tipo 2
# ---------------------------------------------------------------------------

def test_scd2_new_customer(spark):
    _seed_bronze_customers(spark, [_customer_row("c_new", updated_at=datetime(2026, 1, 1))])

    create_silver_customers(spark)

    rows = spark.table("lakehouse.silver.customers").filter("customer_id = 'c_new'").collect()
    assert len(rows) == 1
    assert rows[0]["is_current"] is True
    assert rows[0]["valid_to"] is None
    assert rows[0]["_record_version"] == 1


def test_scd2_plan_change(spark):
    t1 = datetime(2026, 1, 1)
    t2 = datetime(2026, 3, 1)

    _seed_bronze_customers(spark, [_customer_row("c_upgrade", updated_at=t1, plan="free")])
    create_silver_customers(spark)

    _seed_bronze_customers(spark, [_customer_row("c_upgrade", updated_at=t2, plan="pro")])
    create_silver_customers(spark)

    rows = (
        spark.table("lakehouse.silver.customers")
        .filter("customer_id = 'c_upgrade'")
        .orderBy("_record_version")
        .collect()
    )
    assert len(rows) == 2

    old, new = rows
    assert old["plan"] == "free"
    assert old["is_current"] is False
    assert old["_record_version"] == 1
    # intervalo meio-aberto: valid_to da versao antiga == valid_from da nova,
    # sem gap nem overlap (ver transform/silver.py:PLAN_AS_OF_EVENT_QUERY).
    assert old["valid_to"] == new["valid_from"] == t2

    assert new["plan"] == "pro"
    assert new["is_current"] is True
    assert new["valid_to"] is None
    assert new["_record_version"] == 2


def test_scd2_no_change(spark):
    t1 = datetime(2026, 1, 1)
    t2 = datetime(2026, 3, 1)

    _seed_bronze_customers(spark, [_customer_row("c_stable", updated_at=t1, plan="pro")])
    create_silver_customers(spark)

    # updated_at mais recente chega, mas os 4 campos rastreados
    # (plan/segment/is_active/country) sao identicos -- nao deve abrir
    # nova versao nem tocar a existente.
    _seed_bronze_customers(spark, [_customer_row("c_stable", updated_at=t2, plan="pro")])
    create_silver_customers(spark)

    rows = spark.table("lakehouse.silver.customers").filter("customer_id = 'c_stable'").collect()
    assert len(rows) == 1
    assert rows[0]["_record_version"] == 1
    assert rows[0]["updated_at"] == t1  # linha nao foi tocada pela segunda execucao


# ---------------------------------------------------------------------------
# Idempotência
# ---------------------------------------------------------------------------

def test_idempotency(spark):
    base = datetime(2026, 6, 1)
    _seed_bronze_events(spark, [_event_row(f"e_{i}", updated_at=base) for i in range(5)])

    create_silver_events(spark)
    first = {r["event_id"]: r["updated_at"] for r in spark.table("lakehouse.silver.events").collect()}

    create_silver_events(spark)  # bronze nao mudou entre as duas chamadas
    second = {r["event_id"]: r["updated_at"] for r in spark.table("lakehouse.silver.events").collect()}

    assert len(first) == len(second) == 5
    assert first == second  # mesmo resultado, byte a byte, nas duas execucoes
