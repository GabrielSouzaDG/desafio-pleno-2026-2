"""Verificações de qualidade de dados sobre as tabelas silver/gold (Iceberg via Spark).

Uso: `spark-submit quality/checks.py [--pipeline-run-id ID]` (é o que
`make quality` chama — ver Makefile). Roda os 5 checks obrigatórios,
persiste cada resultado em `lakehouse.quality.check_results` (ver
quality/quality_log.py) e levanta `PipelineQualityException` se algum
check `critical` falhou — o sinal para quem orquestra (Makefile, DAG) de
que o pipeline não deve prosseguir.

Como conectar isso a alertas de verdade
-----------------------------------------
Hoje os resultados só vão para a tabela de log — não bastam pra acordar
ninguém às 3h da manhã. Três integrações reais, em ordem de esforço:

1. Slack (webhook, mais simples): depois de `persist_results`, filtrar os
   resultados com `severity == 'critical' and status == 'failed'` e fazer
   um `requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)` para
   cada um. `build_slack_alert_payload()` abaixo já monta esse payload —
   só falta plugar a URL real (variável de ambiente, nunca hardcoded) e a
   chamada HTTP. Dá pra chamar isso direto no fim de `QualityRunner.run()`,
   ou de forma desacoplada (um job separado que lê a tabela de log e
   manda o que for novo — mais resiliente a falha de rede no meio do
   pipeline).

2. PagerDuty (incidente de verdade, on-call): usar a Events API v2
   (`POST https://events.pagerduty.com/v2/enqueue`) com
   `routing_key` (chave de integração do serviço), `event_action: "trigger"`,
   `dedup_key = f"{check_name}:{table_name}"` (para o MESMO problema não
   abrir um incidente novo a cada execução — reabre/atualiza o existente
   até alguém resolver) e o `details` do CheckResult no payload. Isso
   entraria no mesmo ponto do Slack, só que reservado para os `critical`
   que realmente devem acordar alguém (o Slack pode incluir `warning`
   também, sem abrir incidente).

3. Airflow (`on_failure_callback`): como a Parte 5 modela cada check (ou o
   `quality.checks` inteiro) como uma task, o jeito idiomático é a própria
   task falhar (deixar `PipelineQualityException` subir) e usar
   `on_failure_callback=notify_slack_and_pagerduty` na definição da task —
   o Airflow já te dá o `context` (dag_id, task_id, execution_date,
   exception) pra montar a mensagem sem precisar reprocessar nada. É
   melhor que colocar a chamada de alerta dentro do try/except do script:
   fica visível na DAG, e reaproveita os retries/SLAs que o Airflow já
   oferece em vez de reinventar isso aqui.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import quality_log

logger = logging.getLogger("quality.checks")
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


def build_spark_session(app_name: str = "quality") -> SparkSession:
    """Mesma configuração de transform/bronze.py, silver.py e gold.py."""
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


@dataclass
class CheckResult:
    check_name: str
    check_description: str
    severity: str  # 'critical' | 'warning' | 'info'
    status: str  # 'passed' | 'failed' | 'warning'
    layer: str  # 'bronze' | 'silver' | 'gold'
    table_name: str
    records_checked: int
    records_failed: int
    details: dict
    duration_seconds: float
    check_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    executed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    pipeline_run_id: str = ""

    @property
    def failure_rate(self) -> float:
        return (self.records_failed / self.records_checked) if self.records_checked else 0.0


class PipelineQualityException(Exception):
    """Levantada quando pelo menos um check CRITICAL falhou -- sinaliza que
    o pipeline não deve prosseguir para a próxima etapa."""


# ---------------------------------------------------------------------------
# 1. CHECK_UNIQUENESS (critical)
# ---------------------------------------------------------------------------

def check_uniqueness(spark: SparkSession) -> list[CheckResult]:
    results = []

    start = time.monotonic()
    events = spark.table("lakehouse.silver.events")
    total_events = events.count()
    dup_events = events.groupBy("event_id").agg(F.count(F.lit(1)).alias("n")).filter(F.col("n") > 1)
    dup_events_count = dup_events.count()
    examples = [r["event_id"] for r in dup_events.select("event_id").limit(10).collect()]
    results.append(
        CheckResult(
            check_name="CHECK_UNIQUENESS",
            check_description="event_id deve ser unico em silver.events",
            severity="critical",
            status="failed" if dup_events_count > 0 else "passed",
            layer="silver",
            table_name="lakehouse.silver.events",
            records_checked=total_events,
            records_failed=dup_events_count,
            details={"duplicate_event_id_examples": examples},
            duration_seconds=time.monotonic() - start,
        )
    )

    start = time.monotonic()
    customers = spark.table("lakehouse.silver.customers")
    total_customers = customers.count()
    dup_customers = (
        customers.groupBy("customer_id", "valid_from").agg(F.count(F.lit(1)).alias("n")).filter(F.col("n") > 1)
    )
    dup_customers_count = dup_customers.count()
    examples2 = [
        {"customer_id": r["customer_id"], "valid_from": str(r["valid_from"])}
        for r in dup_customers.select("customer_id", "valid_from").limit(10).collect()
    ]
    results.append(
        CheckResult(
            check_name="CHECK_UNIQUENESS",
            check_description="(customer_id, valid_from) deve ser unico em silver.customers",
            severity="critical",
            status="failed" if dup_customers_count > 0 else "passed",
            layer="silver",
            table_name="lakehouse.silver.customers",
            records_checked=total_customers,
            records_failed=dup_customers_count,
            details={"duplicate_key_examples": examples2},
            duration_seconds=time.monotonic() - start,
        )
    )

    return results


# ---------------------------------------------------------------------------
# 2. CHECK_REFERENTIAL_INTEGRITY (warning, escala para critical acima de 20%)
# ---------------------------------------------------------------------------

def check_referential_integrity(spark: SparkSession) -> list[CheckResult]:
    start = time.monotonic()

    # customer_id NULO e um caso ja conhecido/esperado (flag has_customer_id
    # na silver) -- nao e violacao de integridade referencial, entao fica
    # de fora daqui. O que checamos e customer_id PREENCHIDO que nao bate
    # com nenhum cliente conhecido (orfao de verdade).
    events = spark.table("lakehouse.silver.events").filter(F.col("customer_id").isNotNull())
    customers = spark.table("lakehouse.silver.customers").select("customer_id").distinct()

    total = events.count()
    orphans = events.join(customers, on="customer_id", how="left_anti")
    orphan_count = orphans.count()
    failure_rate = (orphan_count / total) if total else 0.0
    sample_ids = [r["customer_id"] for r in orphans.select("customer_id").distinct().limit(10).collect()]

    if failure_rate > 0.20:
        severity, status = "critical", "failed"
    elif failure_rate > 0.05:
        severity, status = "warning", "warning"
    else:
        severity, status = "warning", "passed"

    return [
        CheckResult(
            check_name="CHECK_REFERENTIAL_INTEGRITY",
            check_description="events.customer_id (quando preenchido) deve existir em silver.customers",
            severity=severity,
            status=status,
            layer="silver",
            table_name="lakehouse.silver.events",
            records_checked=total,
            records_failed=orphan_count,
            details={
                "orphan_customer_id_sample": sample_ids,
                "threshold_warning": 0.05,
                "threshold_critical": 0.20,
            },
            duration_seconds=time.monotonic() - start,
        )
    ]


# ---------------------------------------------------------------------------
# 3. CHECK_VOLUMETRY (warning)
# ---------------------------------------------------------------------------

VOLUME_HISTORY_TABLE = "lakehouse.quality.volume_history"


def _ensure_volume_history_table(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.quality")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {VOLUME_HISTORY_TABLE} (
            table_name   STRING,
            metric_date  DATE,
            record_count BIGINT,
            recorded_at  TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (months(metric_date))
        TBLPROPERTIES ('format-version' = '2')
        """
    )


def check_volumetry(spark: SparkSession) -> list[CheckResult]:
    start = time.monotonic()
    _ensure_volume_history_table(spark)

    # recalcula o volume diario inteiro a partir da silver (mesma logica
    # de "recompute + MERGE" usada em gold.py) -- cada dia com evento fica
    # com 1 linha na tabela de historico, idempotente entre execucoes.
    daily_volume = (
        spark.table("lakehouse.silver.events")
        .withColumn("metric_date", F.to_date("occurred_at"))
        .groupBy("metric_date")
        .agg(F.count(F.lit(1)).alias("record_count"))
        .withColumn("table_name", F.lit("lakehouse.silver.events"))
        .withColumn("recorded_at", F.current_timestamp())
        .select("table_name", "metric_date", "record_count", "recorded_at")
    )
    daily_volume.createOrReplaceTempView("volume_history_source")
    spark.sql(
        f"""
        MERGE INTO {VOLUME_HISTORY_TABLE} AS target
        USING volume_history_source AS source
        ON target.table_name = source.table_name AND target.metric_date = source.metric_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    history = spark.table(VOLUME_HISTORY_TABLE).filter(F.col("table_name") == "lakehouse.silver.events")
    latest_rows = history.orderBy(F.col("metric_date").desc()).limit(1).collect()

    if not latest_rows:
        return [
            CheckResult(
                check_name="CHECK_VOLUMETRY",
                check_description="volume diario de silver.events vs. media dos ultimos 7 dias",
                severity="warning",
                status="passed",
                layer="silver",
                table_name="lakehouse.silver.events",
                records_checked=0,
                records_failed=0,
                details={"reason": "sem dado em silver.events ainda"},
                duration_seconds=time.monotonic() - start,
            )
        ]

    today = latest_rows[0]["metric_date"]
    today_volume = latest_rows[0]["record_count"]

    baseline_df = history.filter(
        (F.col("metric_date") < F.lit(today)) & (F.col("metric_date") >= F.date_sub(F.lit(today), 7))
    )
    baseline_days = baseline_df.count()
    baseline_avg = baseline_df.agg(F.avg("record_count").alias("avg")).collect()[0]["avg"]

    if baseline_days == 0 or not baseline_avg:
        status = "passed"
        pct_change = None
        reason = "historico insuficiente (nenhum dia anterior registrado ainda) para comparar"
    else:
        pct_change = (today_volume - baseline_avg) / baseline_avg
        status = "warning" if (pct_change <= -0.30 or pct_change >= 2.0) else "passed"
        reason = None

    return [
        CheckResult(
            check_name="CHECK_VOLUMETRY",
            check_description="volume diario de silver.events vs. media dos ultimos 7 dias",
            severity="warning",
            status=status,
            layer="silver",
            table_name="lakehouse.silver.events",
            records_checked=today_volume,
            records_failed=(today_volume if status == "warning" else 0),
            details={
                "metric_date": str(today),
                "today_volume": today_volume,
                "baseline_avg_7d": baseline_avg,
                "baseline_days_available": baseline_days,
                "pct_change": pct_change,
                "reason": reason,
            },
            duration_seconds=time.monotonic() - start,
        )
    ]


# ---------------------------------------------------------------------------
# 4. CHECK_DOMAIN_VALUES (critical)
# ---------------------------------------------------------------------------

VALID_EVENT_TYPES = [
    "ticket_opened",
    "ticket_replied",
    "ticket_closed",
    "login",
    "feature_used",
    "subscription_changed",
]
VALID_PLANS = ["free", "pro", "enterprise"]


def check_domain_values(spark: SparkSession) -> list[CheckResult]:
    results = []

    start = time.monotonic()
    events = spark.table("lakehouse.silver.events")
    total_events = events.count()
    invalid_events = events.filter(~F.col("event_type").isin(VALID_EVENT_TYPES))
    invalid_events_count = invalid_events.count()
    examples = [r["event_type"] for r in invalid_events.select("event_type").distinct().limit(10).collect()]
    results.append(
        CheckResult(
            check_name="CHECK_DOMAIN_VALUES",
            check_description=f"event_type deve estar em {VALID_EVENT_TYPES}",
            severity="critical",
            status="failed" if invalid_events_count > 0 else "passed",
            layer="silver",
            table_name="lakehouse.silver.events",
            records_checked=total_events,
            records_failed=invalid_events_count,
            details={"invalid_event_type_values_found": examples, "valid_values": VALID_EVENT_TYPES},
            duration_seconds=time.monotonic() - start,
        )
    )

    start = time.monotonic()
    customers = spark.table("lakehouse.silver.customers")
    total_customers = customers.count()
    invalid_plan = customers.filter(~F.col("plan").isin(VALID_PLANS))
    invalid_plan_count = invalid_plan.count()
    examples2 = [r["plan"] for r in invalid_plan.select("plan").distinct().limit(10).collect()]
    results.append(
        CheckResult(
            check_name="CHECK_DOMAIN_VALUES",
            check_description=f"plan deve estar em {VALID_PLANS}",
            severity="critical",
            status="failed" if invalid_plan_count > 0 else "passed",
            layer="silver",
            table_name="lakehouse.silver.customers",
            records_checked=total_customers,
            records_failed=invalid_plan_count,
            details={"invalid_plan_values_found": examples2, "valid_values": VALID_PLANS},
            duration_seconds=time.monotonic() - start,
        )
    )

    return results


# ---------------------------------------------------------------------------
# 5. CHECK_FRESHNESS (critical)
# ---------------------------------------------------------------------------

def check_freshness(spark: SparkSession, reference_time: datetime | None = None) -> list[CheckResult]:
    """`reference_time` é injetável (default: agora, em UTC) para permitir
    testar/rodar contra uma data de referência diferente de "agora" — útil
    num backfill, ou ao reprocessar um dia antigo pela DAG (Parte 5), onde
    "as últimas 48h" devem ser relativas à data de execução, não ao
    relógio real de quando o backfill roda.

    Nota honesta: como o dataset deste desafio é sintético e fixo (para
    de ser gerado em ~2026-08-31), este check vai legitimamente reportar
    "failed" quando rodado contra o relógio real depois dessa data — e
    isso está correto, não é um bug do check. Um dataset que parou de ser
    atualizado HÁ MAIS de 48h está, de fato, stale; um pipeline de
    produção real com essa fonte congelada deveria mesmo alertar.
    """
    start = time.monotonic()
    reference_time = reference_time or datetime.now(timezone.utc)

    events = spark.table("lakehouse.silver.events")
    latest_row = events.agg(F.max("occurred_at").alias("latest")).collect()[0]
    latest = latest_row["latest"]

    if latest is None:
        status = "failed"
        hours_since = None
    else:
        latest_utc = latest if latest.tzinfo else latest.replace(tzinfo=timezone.utc)
        hours_since = (reference_time - latest_utc).total_seconds() / 3600.0
        status = "passed" if hours_since <= 48 else "failed"

    return [
        CheckResult(
            check_name="CHECK_FRESHNESS",
            check_description="occurred_at mais recente em silver.events deve estar dentro das ultimas 48h",
            severity="critical",
            status=status,
            layer="silver",
            table_name="lakehouse.silver.events",
            records_checked=1,
            records_failed=0 if status == "passed" else 1,
            details={
                "latest_occurred_at": str(latest),
                "reference_time": reference_time.isoformat(),
                "hours_since_latest": hours_since,
                "threshold_hours": 48,
            },
            duration_seconds=time.monotonic() - start,
        )
    ]


# ---------------------------------------------------------------------------
# Orquestração
# ---------------------------------------------------------------------------

ALL_CHECKS = [check_uniqueness, check_referential_integrity, check_volumetry, check_domain_values, check_freshness]


class QualityRunner:
    """`spark` é passado explicitamente em cada método (em vez de guardado
    em `__init__`) para casar com o padrão do resto do pipeline
    (bronze/silver/gold recebem `spark` como parâmetro, não como estado de
    instância) e deixar cada chamada independente/testável."""

    def run_all_checks(self, spark: SparkSession, pipeline_run_id: str | None = None) -> list[CheckResult]:
        pipeline_run_id = pipeline_run_id or str(uuid.uuid4())
        results: list[CheckResult] = []
        for check_fn in ALL_CHECKS:
            for r in check_fn(spark):
                r.pipeline_run_id = pipeline_run_id
                results.append(r)
                _log(
                    "info" if r.status == "passed" else ("warning" if r.status == "warning" else "error"),
                    "quality_check_result",
                    check_name=r.check_name,
                    table_name=r.table_name,
                    status=r.status,
                    severity=r.severity,
                    records_checked=r.records_checked,
                    records_failed=r.records_failed,
                    failure_rate=round(r.failure_rate, 4),
                    pipeline_run_id=pipeline_run_id,
                )
        return results

    def persist_results(self, spark: SparkSession, results: list[CheckResult]) -> None:
        written = quality_log.write_check_results(spark, results)
        _log("info", "quality_results_persisted", rows_written=written, table=quality_log.CHECK_RESULTS_TABLE)

    def evaluate_pipeline_stop(self, results: list[CheckResult]) -> bool:
        return any(r.severity == "critical" and r.status == "failed" for r in results)

    def run(self, spark: SparkSession, pipeline_run_id: str | None = None) -> list[CheckResult]:
        """Roda tudo, persiste, e levanta PipelineQualityException se algum
        check critico falhou. É o que `make quality`/a DAG chamam."""
        results = self.run_all_checks(spark, pipeline_run_id)
        self.persist_results(spark, results)
        if self.evaluate_pipeline_stop(results):
            failed = [f"{r.check_name}/{r.table_name}" for r in results if r.severity == "critical" and r.status == "failed"]
            raise PipelineQualityException(f"checks criticos falharam: {failed}. Pipeline nao deve prosseguir.")
        return results


def build_slack_alert_payload(result: CheckResult) -> dict:
    """Payload pronto para um Incoming Webhook do Slack. Uso real:

        webhook_url = os.environ["SLACK_QUALITY_WEBHOOK_URL"]
        requests.post(webhook_url, json=build_slack_alert_payload(r), timeout=10)

    para cada resultado com severity == 'critical' e status == 'failed'
    (ver a seção "Como conectar isso a alertas de verdade" no topo do
    módulo)."""
    return {
        "text": (
            f":rotating_light: *Quality check FAILED* — `{result.check_name}` em "
            f"`{result.table_name}` ({result.layer})\n"
            f"> {result.check_description}\n"
            f"*Severity:* {result.severity} | *Registros afetados:* "
            f"{result.records_failed}/{result.records_checked} ({result.failure_rate:.1%})\n"
            f"*Pipeline run:* {result.pipeline_run_id}\n"
            f"*Detalhes:* ```{json.dumps(result.details, default=str, ensure_ascii=False)}```"
        )
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Quality: roda os checks obrigatorios e persiste em Iceberg")
    parser.add_argument("--pipeline-run-id", dest="pipeline_run_id", default=None)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    spark = build_spark_session()
    try:
        runner = QualityRunner()
        pipeline_run_id = args.pipeline_run_id or os.environ.get("PIPELINE_RUN_ID")
        results = runner.run(spark, pipeline_run_id=pipeline_run_id)
        _log("info", "quality_completed", checks_run=len(results))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
