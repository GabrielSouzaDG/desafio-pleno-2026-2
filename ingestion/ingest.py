"""Ponto de entrada da ingestão: orquestra api_client + postgres_client -> raw zone (MinIO).

Uso: `python3 -m ingestion.ingest [--ingestion-date YYYY-MM-DD]`
(é o que `make ingest` chama — ver Makefile).

Fluxo por fonte (events, customers):
  1. lê o watermark atual (updated_at da última ingestão bem-sucedida);
  2. busca da fonte tudo com updated_at >= watermark;
  3. grava o payload bruto (sem transformação de negócio) em
     raw/<fonte>/ingestion_date=<data>/part-000.json.gz, em JSON Lines
     gzipado, fazendo merge idempotente com o que já existir na key (ver
     RawZoneWriter.write_jsonl_gz);
  4. só DEPOIS do upload confirmado, avança o watermark daquela fonte.

Se o processo cair entre os passos 2 e 4 (ou o upload falhar), o watermark
não avança e a próxima execução repete o mesmo intervalo — como o passo 3
faz merge deduplicado em vez de sobrescrever cegamente, reprocessar o
mesmo intervalo é seguro: nunca perde e nunca duplica dado na raw.
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable

import boto3
from botocore.exceptions import ClientError

from ingestion.api_client import APIClient
from ingestion.postgres_client import PostgresClient
from ingestion.watermark import WatermarkManager

logger = logging.getLogger("ingestion.ingest")
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


class RawZoneWriter:
    """Escreve arquivos JSON Lines gzip na raw zone do MinIO/S3.

    Endpoint/credenciais via env vars, com defaults sensatos para dev local
    (casam com o docker-compose deste repo):
      MINIO_ENDPOINT=http://localhost:9000, MINIO_ACCESS_KEY=admin,
      MINIO_SECRET_KEY=minioadmin, MINIO_BUCKET=lakehouse
    """

    def __init__(
        self,
        endpoint_url: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        bucket: str | None = None,
    ):
        self.endpoint_url = endpoint_url or os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
        self.access_key = access_key or os.environ.get("MINIO_ACCESS_KEY", "admin")
        self.secret_key = secret_key or os.environ.get("MINIO_SECRET_KEY", "minioadmin")
        self.bucket = bucket or os.environ.get("MINIO_BUCKET", "lakehouse")

        self._s3 = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name="us-east-1",
        )
        self._ensure_bucket()

    def _ensure_bucket(self) -> None:
        try:
            self._s3.head_bucket(Bucket=self.bucket)
        except ClientError:
            self._s3.create_bucket(Bucket=self.bucket)
            _log("info", "minio_bucket_created", bucket=self.bucket)

    @staticmethod
    def _object_key(source: str, ingestion_date: str, part: int = 0) -> str:
        return f"raw/{source}/ingestion_date={ingestion_date}/part-{part:03d}.json.gz"

    def _read_existing_lines(self, key: str) -> list[str]:
        try:
            obj = self._s3.get_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("NoSuchKey", "404"):
                return []
            raise
        raw_bytes = obj["Body"].read()
        with gzip.GzipFile(fileobj=io.BytesIO(raw_bytes), mode="rb") as gz:
            text = gz.read().decode("utf-8")
        return [line for line in text.split("\n") if line]

    def write_jsonl_gz(self, source: str, ingestion_date: str, records: list[dict], part: int = 0) -> dict:
        """Grava `records` (payload bruto, sem transformação de negócio) em
        `raw/<source>/ingestion_date=<data>/part-<NNN>.json.gz`.

        Idempotência via merge, não overwrite cego
        --------------------------------------------
        A raw zone é particionada por DIA de ingestão, mas o pipeline pode
        (e a sequência de avaliação do desafio efetivamente vai) rodar mais
        de uma vez no mesmo dia — inclusive com uma carga de dado nova no
        meio (o `make batch2`). Se cada execução simplesmente sobrescrevesse
        a key com o que ELA buscou (só o delta incremental desde o último
        watermark), a segunda execução do dia apagaria o que a primeira
        tinha gravado — perda de dado na raw, que o enunciado proíbe
        explicitamente.

        Por isso a escrita faz merge: lê o conteúdo já existente na key (se
        houver), remove duplicatas exatas linha-a-linha (`sort_keys=True` no
        dump garante que a mesma linha reaparecendo — mesmo watermark
        reprocessado, ou overlap de paginação da API — vira o mesmo texto e
        portanto dedupa; isso NÃO altera nenhum valor do payload, só a
        ordem de serialização das chaves), concatena o que é novo, e
        escreve o conjunto inteiro de volta com UM único `put_object`.

        Um único `put_object` é o que garante a atomicidade pedida
        (equivalente a um write-then-rename num filesystem POSIX): o objeto
        é montado inteiro em memória antes do upload, então não existe
        leitor vendo a key "pela metade" — ou o objeto novo (mesclado)
        substitui o antigo de uma vez, ou a chamada falha e o antigo
        permanece intacto. Se não há nada novo para adicionar e a key já
        existe, nem chega a fazer o PUT (no-op) — o que é a definição
        prática de idempotência aqui: rodar de novo sem dado novo não muda
        nada.
        """
        key = self._object_key(source, ingestion_date, part)
        existing_lines = self._read_existing_lines(key)
        seen = set(existing_lines)

        new_lines: list[str] = []
        for record in records:
            line = json.dumps(record, default=str, ensure_ascii=False, sort_keys=True)
            if line in seen:
                continue
            seen.add(line)
            new_lines.append(line)

        if not new_lines and existing_lines:
            _log("info", "raw_write_skipped_no_new_data", source=source, key=key)
            return {"key": key, "records_written": 0, "records_total": len(existing_lines), "skipped": True}

        merged_lines = existing_lines + new_lines
        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
            gz.write(("\n".join(merged_lines) + "\n").encode("utf-8"))
        buffer.seek(0)

        self._s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="application/gzip",
            ContentEncoding="gzip",
        )
        return {"key": key, "records_written": len(new_lines), "records_total": len(merged_lines), "skipped": False}


def _max_updated_at(records: list[dict], fallback: str) -> str:
    """Novo watermark = maior `updated_at` visto nesta leva. Sem registros
    novos, o watermark fica onde estava — não há nada mais recente para
    avançar para."""
    updated_ats = [r["updated_at"] for r in records if r.get("updated_at")]
    return max(updated_ats) if updated_ats else fallback


def _ingest_source(
    source: str,
    fetch_fn: Callable[[str], list[dict]],
    writer: RawZoneWriter,
    watermarks: WatermarkManager,
    ingestion_date: str,
) -> dict:
    """Roda o ciclo watermark -> fetch -> write -> avança watermark para
    uma única fonte. Devolve um resumo (registros, arquivo, duração)."""
    start = time.monotonic()
    since = watermarks.get_watermark(source)

    records = fetch_fn(since)
    write_result = writer.write_jsonl_gz(source=source, ingestion_date=ingestion_date, records=records)

    # o watermark só avança DEPOIS do upload confirmado (write_jsonl_gz
    # retornou sem lançar exceção) — se fetch ou write falharem antes daqui,
    # esta linha nunca roda e o watermark fica parado no valor anterior.
    new_watermark = _max_updated_at(records, fallback=since)
    watermarks.set_watermark(source, new_watermark)

    duration = time.monotonic() - start
    summary = {
        "source": source,
        "records_fetched": len(records),
        "records_written_to_raw": write_result["records_written"],
        "raw_key": write_result["key"],
        "raw_write_skipped": write_result["skipped"],
        "watermark_before": since,
        "watermark_after": new_watermark,
        "duration_seconds": round(duration, 3),
    }
    _log("info", "source_ingested", **summary)
    return summary


def ingest_events(ingestion_date: str) -> dict:
    """Ingestão só da fonte `events` (Mock API). Extraída de `run()` para
    que a DAG do Airflow (Parte 5) possa rodá-la como task independente,
    em paralelo com `ingest_customers` (task_ingest_api / task_ingest_postgres)."""
    writer = RawZoneWriter()
    watermarks = WatermarkManager()
    api_client = APIClient()
    return _ingest_source("events", api_client.fetch_events, writer, watermarks, ingestion_date)


def ingest_customers(ingestion_date: str) -> dict:
    """Ingestão só da fonte `customers` (PostgreSQL). Ver `ingest_events`."""
    writer = RawZoneWriter()
    watermarks = WatermarkManager()
    pg_client = PostgresClient()
    return _ingest_source("customers", pg_client.fetch_customers, writer, watermarks, ingestion_date)


def run(ingestion_date: str | None = None) -> dict:
    """Orquestra a ingestão completa (events + customers) para uma data.

    `ingestion_date` default = hoje (UTC). Aceitar um valor explícito
    permite reprocessar/testar um dia específico e é o parâmetro que a DAG
    do Airflow (Parte 5) passa como data de execução. É o que `make ingest`
    chama; a DAG chama `ingest_events`/`ingest_customers` diretamente.
    """
    started_at = time.monotonic()
    ingestion_date = ingestion_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    _log("info", "ingestion_started", ingestion_date=ingestion_date)

    results = [
        ingest_events(ingestion_date),
        ingest_customers(ingestion_date),
    ]

    total_duration = time.monotonic() - started_at
    summary = {
        "ingestion_date": ingestion_date,
        "sources": results,
        "total_records_fetched": sum(r["records_fetched"] for r in results),
        "files_written": [r["raw_key"] for r in results],
        "total_duration_seconds": round(total_duration, 3),
    }
    _log("info", "ingestion_completed", **summary)
    return summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingestao raw zone: Mock API (events) + PostgreSQL (customers) -> MinIO"
    )
    parser.add_argument(
        "--ingestion-date",
        dest="ingestion_date",
        default=None,
        help="Data de ingestao (YYYY-MM-DD). Default: hoje (UTC) ou env var INGESTION_DATE.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(ingestion_date=args.ingestion_date or os.environ.get("INGESTION_DATE"))
