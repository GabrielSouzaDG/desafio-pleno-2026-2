"""Cliente de extração incremental de crm.customers via psycopg2."""

from __future__ import annotations

import datetime as dt
import decimal
import json
import logging
import os
import time
from typing import Any

import psycopg2
import psycopg2.extras

logger = logging.getLogger("ingestion.postgres_client")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


def _log(level: str, event: str, **fields: Any) -> None:
    record = {
        "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
        "level": level,
        "event": event,
        **fields,
    }
    getattr(logger, level.lower(), logger.info)(json.dumps(record, default=str))


# updated_at >= watermark: mesmo raciocínio da ingestão de eventos (ver o
# docstring de ingestion/watermark.py) — é o updated_at que captura troca
# de plano, churn (is_active=false) e clientes novos entre duas cargas, não
# o signup_date (que é fixo e não muda quando o cadastro é alterado).
# O cast ::timestamptz deixa explícito para o Postgres como comparar o
# parâmetro (a lib manda como texto) em vez de depender de inferência.
_CUSTOMERS_QUERY = """
    SELECT customer_id, company_name, plan, segment, signup_date,
           country, is_active, updated_at
    FROM crm.customers
    WHERE updated_at >= %(since)s::timestamptz
    ORDER BY updated_at, customer_id
"""


def _to_jsonable(value: Any) -> Any:
    """Converte os tipos nativos que o psycopg2 devolve (date, datetime
    com tz, Decimal) para algo serializável em JSON, sem perder precisão
    nem mudar o valor semântico — timestamps sempre normalizados para UTC
    com sufixo 'Z' e precisão fixa de microssegundos, para que a
    comparação lexicográfica de strings (usada para achar o novo watermark
    em ingest.py) bata com a ordem cronológica real."""
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.timezone.utc)
        return value.astimezone(dt.timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    return value


class PostgresClient:
    """Cliente de leitura incremental de `crm.customers`.

    Parâmetros de conexão via env vars, com defaults que já casam com o
    docker-compose deste repo (funciona "de fábrica" rodando do host):
      POSTGRES_HOST=localhost, POSTGRES_PORT=5432, POSTGRES_DB=crm,
      POSTGRES_USER=app, POSTGRES_PASSWORD=app
    """

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        dbname: str | None = None,
        user: str | None = None,
        password: str | None = None,
    ):
        self.host = host or os.environ.get("POSTGRES_HOST", "localhost")
        self.port = int(port or os.environ.get("POSTGRES_PORT", 5432))
        self.dbname = dbname or os.environ.get("POSTGRES_DB", "crm")
        self.user = user or os.environ.get("POSTGRES_USER", "app")
        self.password = password or os.environ.get("POSTGRES_PASSWORD", "app")

    def _connect(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            connect_timeout=10,
        )

    def fetch_customers(self, since_watermark: str) -> list[dict]:
        """Lê `crm.customers` com `updated_at >= since_watermark` e devolve
        uma lista de dicts prontos para serialização em JSON (raw, sem
        nenhuma transformação de negócio — só conversão de tipo)."""
        start = time.monotonic()
        conn = self._connect()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(_CUSTOMERS_QUERY, {"since": since_watermark})
                rows = cur.fetchall()
        finally:
            conn.close()

        records = [{key: _to_jsonable(value) for key, value in row.items()} for row in rows]

        duration = time.monotonic() - start
        _log(
            "info",
            "postgres_fetch_completed",
            records_fetched=len(records),
            duration_seconds=round(duration, 3),
            since_watermark=since_watermark,
        )
        return records
