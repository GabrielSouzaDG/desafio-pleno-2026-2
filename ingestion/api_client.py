"""Cliente HTTP para a Mock API (/events) — paginação, retry e backoff.

Responsabilidade única: falar com a API e devolver os registros brutos,
página a página, exatamente como a API os entrega. Nenhuma regra de
negócio (dedup, tipagem, normalização) acontece aqui — isso é trabalho da
camada silver. A única "inteligência" deste módulo é sobreviver às
características intencionalmente hostis da API (rate limit, 503
intermitente, paginação).
"""

from __future__ import annotations

import json
import logging
import os
import random
import time
from datetime import datetime, timezone
from typing import Any

import requests
from pydantic import BaseModel, ConfigDict, ValidationError

logger = logging.getLogger("ingestion.api_client")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


def _log(level: str, event: str, **fields: Any) -> None:
    """Emite uma linha de log estruturada em JSON (fácil de agregar em
    qualquer stack de observabilidade — grep, jq, Loki, CloudWatch, etc.)."""
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "event": event,
        **fields,
    }
    getattr(logger, level.lower(), logger.info)(json.dumps(record, default=str))


class EventRecord(BaseModel):
    """Valida só o mínimo indispensável de um evento (chaves que o pipeline
    realmente precisa para rotear/particionar o dado).

    `extra="allow"` é a peça central da tolerância a schema drift: quando a
    API passa a mandar `source_app` ou `sla_minutes` a partir de uma certa
    data, esses campos não fazem essa validação falhar nem são descartados
    — eles simplesmente não são exigidos, e o dict bruto original (com eles
    dentro) é o que segue para a raw zone. O pipeline não precisa saber de
    antemão quais campos novos vão aparecer.
    """

    model_config = ConfigDict(extra="allow")

    event_id: str
    event_type: str
    occurred_at: str
    updated_at: str


class APIClientError(RuntimeError):
    """Erro não recuperável ao consultar a Mock API (orçamento de retries
    esgotado ou erro HTTP que não é 429/503)."""


class _RateLimiter:
    """Garante um teto de `max_rps` requisições/segundo ao servidor,
    espaçando as chamadas em vez de só reagir a 429 depois de já ter
    estourado o limite."""

    def __init__(self, max_rps: float):
        self._min_interval = 1.0 / max_rps
        self._last_call: float | None = None

    def wait(self) -> None:
        if self._last_call is not None:
            elapsed = time.monotonic() - self._last_call
            remaining = self._min_interval - elapsed
            if remaining > 0:
                time.sleep(remaining)
        self._last_call = time.monotonic()


class APIClient:
    """Cliente para `GET /events` da Mock API, com paginação, rate limiting
    e retry/backoff para 429 e 503.

    Parametrizável via env vars para não hardcodar nada de ambiente:
      - API_BASE_URL (default: http://localhost:8000)
      - API_KEY      (default: desafio-2026)
    """

    def __init__(
        self,
        base_url: str | None = None,
        api_key: str | None = None,
        page_size: int = 500,
        max_rps: float = 10.0,
        timeout_seconds: float = 30.0,
        max_retries_429: int = 10,
        max_retries_503: int = 5,
        session: requests.Session | None = None,
    ):
        self.base_url = (base_url or os.environ.get("API_BASE_URL", "http://localhost:8000")).rstrip("/")
        self.api_key = api_key or os.environ.get("API_KEY", "desafio-2026")
        self.page_size = page_size
        self.timeout_seconds = timeout_seconds
        self.max_retries_429 = max_retries_429
        self.max_retries_503 = max_retries_503

        self._rate_limiter = _RateLimiter(max_rps)
        self._session = session or requests.Session()
        self._session.headers.update({"X-API-Key": self.api_key})

    def fetch_events(self, since: str) -> list[dict]:
        """Busca TODAS as páginas de `/events` com `updated_at >= since`.

        `since` é sempre um watermark de `updated_at` — nunca de
        `occurred_at` (ver o docstring do módulo `watermark.py` para o
        porquê). Devolve os registros na ordem em que a API os entregou,
        sem deduplicar: a API sobrepõe ~2 registros entre páginas de
        propósito, e o mesmo `event_id` pode reaparecer em execuções
        diferentes (correções, late arrivals). Deduplicar aqui esconderia
        esse comportamento em vez de tratá-lo — quem resolve duplicidade e
        "última versão vence" é a camada silver, via MERGE INTO por
        (event_id, updated_at). A raw zone deve ser um espelho fiel do que
        a API respondeu.
        """
        start = time.monotonic()
        all_records: list[dict] = []
        total_retries = 0
        page = 1
        total_pages = 1  # corrigido assim que a 1a página responder

        while page <= total_pages:
            payload, retries = self._get_page(since=since, page=page)
            total_retries += retries

            records = payload.get("data", [])
            for raw in records:
                self._validate(raw)  # só loga; nunca filtra nem altera o dict
            all_records.extend(records)

            total_pages = payload.get("total_pages", page)
            page += 1

        duration = time.monotonic() - start
        _log(
            "info",
            "api_fetch_completed",
            pages_read=page - 1,
            records_fetched=len(all_records),
            retries=total_retries,
            duration_seconds=round(duration, 3),
            since_watermark=since,
        )
        return all_records

    def _get_page(self, since: str, page: int) -> tuple[dict, int]:
        """Busca uma página específica, absorvendo 429/503 com backoff.
        Retorna (payload_json, numero_de_retries_consumidos)."""
        retries_429 = 0
        retries_503 = 0

        while True:
            self._rate_limiter.wait()
            try:
                response = self._session.get(
                    f"{self.base_url}/events",
                    params={"since": since, "page": page, "page_size": self.page_size},
                    timeout=self.timeout_seconds,
                )
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
                # timeout/conexão caída tratados com o mesmo orçamento do 503:
                # são falhas transitórias de infraestrutura, não do dado.
                retries_503 += 1
                if retries_503 > self.max_retries_503:
                    _log("error", "api_network_error_exhausted", page=page, error=str(exc))
                    raise APIClientError(
                        f"falha de rede ao buscar page={page} apos {self.max_retries_503} tentativas: {exc}"
                    ) from exc
                _log("warning", "api_network_error_retry", page=page, attempt=retries_503, error=str(exc))
                time.sleep(self._backoff_delay(retries_503))
                continue

            if response.status_code == 429:
                retries_429 += 1
                if retries_429 > self.max_retries_429:
                    _log("error", "api_rate_limit_exhausted", page=page, attempts=retries_429)
                    raise APIClientError(f"429 persistente em page={page} apos {self.max_retries_429} tentativas")
                delay = self._retry_after_seconds(response) or self._backoff_delay(retries_429)
                _log("warning", "api_rate_limited", page=page, attempt=retries_429, sleep_seconds=round(delay, 2))
                time.sleep(delay)
                continue

            if response.status_code == 503:
                retries_503 += 1
                if retries_503 > self.max_retries_503:
                    _log("error", "api_unavailable_exhausted", page=page, attempts=retries_503)
                    raise APIClientError(f"503 persistente em page={page} apos {self.max_retries_503} tentativas")
                delay = self._backoff_delay(retries_503)
                _log("warning", "api_unavailable_retry", page=page, attempt=retries_503, sleep_seconds=round(delay, 2))
                time.sleep(delay)
                continue

            if response.status_code >= 400:
                _log(
                    "error",
                    "api_http_error",
                    page=page,
                    status_code=response.status_code,
                    body=response.text[:500],
                )
                response.raise_for_status()

            retries_total = retries_429 + retries_503
            return response.json(), retries_total

    def _validate(self, raw: dict) -> None:
        try:
            EventRecord.model_validate(raw)
        except ValidationError as exc:
            # registro com campos obrigatórios ausentes/estranhos: alertamos,
            # mas não removemos da raw — decidir o que fazer com dado "sujo"
            # é responsabilidade da camada de qualidade, não da ingestão.
            _log(
                "warning",
                "api_record_validation_warning",
                event_id=raw.get("event_id"),
                error=str(exc),
            )

    @staticmethod
    def _retry_after_seconds(response: requests.Response) -> float | None:
        header = response.headers.get("Retry-After")
        if header is None:
            return None
        try:
            return float(header)
        except ValueError:
            return None

    @staticmethod
    def _backoff_delay(attempt: int) -> float:
        """Backoff exponencial (base 2, teto 30s) com jitter para não
        sincronizar retries em rajada e reprovocar o 429/503."""
        base = min(2**attempt, 30)
        jitter = random.uniform(0, base * 0.25)
        return base + jitter
