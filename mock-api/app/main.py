"""
Mock API do desafio — simula uma API de produto/atendimento de verdade.

Comportamentos intencionais:
  * paginação obrigatória, com sobreposição de registros entre páginas
  * `since` é INCLUSIVO (>=), o que faz o registro de fronteira voltar a cada execução
  * 429 quando passa do rate limit
  * 503 intermitente em uma fração das chamadas
  * o payload cresce a partir de certa data (schema drift)

Controle de batch (simula o tempo passando entre duas execuções do pipeline):
  POST /admin/advance-batch  -> libera os dados do batch 2
  POST /admin/reset          -> volta para o batch 1
  GET  /admin/state          -> estado atual
"""

from __future__ import annotations

import json
import os
import random
import time
from collections import deque
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import JSONResponse

API_KEY = os.getenv("API_KEY", "desafio-2026")
DATA_FILE = Path(os.getenv("DATA_FILE", "/data/events.jsonl"))
STATE_FILE = Path(os.getenv("STATE_FILE", "/data/.batch_state"))
FLAKY_RATE = float(os.getenv("FLAKY_RATE", "0.02"))      # FLAKY_RATE=0 desliga o caos
RATE_LIMIT_RPS = int(os.getenv("RATE_LIMIT_RPS", "10"))
PAGE_OVERLAP = int(os.getenv("PAGE_OVERLAP", "2"))
MAX_PAGE_SIZE = 1000

app = FastAPI(
    title="Mock API — Desafio Lakehouse",
    description="Eventos de produto e atendimento. Autenticação via header `X-API-Key`.",
    version="1.0.0",
)

_events: list[dict[str, Any]] = []
_hits: deque[float] = deque()


# --------------------------------------------------------------- utilidades
def _load_events() -> None:
    global _events
    if not DATA_FILE.exists():
        raise RuntimeError(
            f"{DATA_FILE} não encontrado. Rode `make seed` antes de subir a API."
        )
    with DATA_FILE.open(encoding="utf-8") as fh:
        _events = [json.loads(line) for line in fh if line.strip()]
    _events.sort(key=lambda e: (e["updated_at"], e["event_id"]))


def _current_batch() -> int:
    try:
        return int(STATE_FILE.read_text().strip())
    except Exception:
        return 1


def _set_batch(value: int) -> None:
    try:
        STATE_FILE.write_text(str(value))
    except OSError:
        pass


def _check_key(api_key: str | None) -> None:
    if api_key != API_KEY:
        raise HTTPException(status_code=401, detail="X-API-Key ausente ou inválida.")


def _check_rate_limit() -> None:
    now = time.monotonic()
    while _hits and now - _hits[0] > 1.0:
        _hits.popleft()
    if len(_hits) >= RATE_LIMIT_RPS:
        raise HTTPException(
            status_code=429,
            detail=f"Rate limit de {RATE_LIMIT_RPS} req/s excedido. Tente novamente.",
            headers={"Retry-After": "1"},
        )
    _hits.append(now)


def _maybe_fail() -> None:
    if FLAKY_RATE > 0 and random.random() < FLAKY_RATE:
        raise HTTPException(status_code=503, detail="Serviço temporariamente indisponível.")


def _public(event: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in event.items() if not k.startswith("_")}


@app.on_event("startup")
def _startup() -> None:
    _load_events()
    if not STATE_FILE.exists():
        _set_batch(1)


# ------------------------------------------------------------------ rotas
@app.get("/health", tags=["infra"])
def health() -> dict[str, Any]:
    return {"status": "ok", "events_loaded": len(_events), "batch": _current_batch()}


@app.get("/events", tags=["dados"])
def get_events(
    since: str | None = Query(
        None,
        description="Filtro INCLUSIVO por updated_at, ISO-8601 UTC. Ex.: 2026-08-01T00:00:00Z",
    ),
    until: str | None = Query(None, description="Filtro exclusivo por updated_at."),
    page: int = Query(1, ge=1),
    page_size: int = Query(500, ge=1, le=MAX_PAGE_SIZE),
    x_api_key: str | None = Header(None, alias="X-API-Key"),
) -> JSONResponse:
    _check_key(x_api_key)
    _check_rate_limit()
    _maybe_fail()

    batch = _current_batch()
    rows = [e for e in _events if e.get("_batch", 1) <= batch]
    if since:
        rows = [e for e in rows if e["updated_at"] >= since]
    if until:
        rows = [e for e in rows if e["updated_at"] < until]

    total = len(rows)
    total_pages = max(1, -(-total // page_size))

    start = (page - 1) * page_size
    if page > 1:
        start = max(0, start - PAGE_OVERLAP)   # sobreposição proposital entre páginas
    chunk = rows[start:(page - 1) * page_size + page_size]

    return JSONResponse({
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "total_records": total,
        "has_next": page < total_pages,
        "data": [_public(e) for e in chunk],
    })


@app.get("/events/{event_id}", tags=["dados"])
def get_event(event_id: str, x_api_key: str | None = Header(None, alias="X-API-Key")):
    _check_key(x_api_key)
    matches = [_public(e) for e in _events if e["event_id"] == event_id]
    if not matches:
        raise HTTPException(status_code=404, detail="event_id não encontrado.")
    return {"versions": matches}


@app.get("/admin/state", tags=["admin"])
def admin_state(x_api_key: str | None = Header(None, alias="X-API-Key")):
    _check_key(x_api_key)
    batch = _current_batch()
    visible = sum(1 for e in _events if e.get("_batch", 1) <= batch)
    return {"batch": batch, "visible_records": visible, "total_records": len(_events)}


@app.post("/admin/advance-batch", tags=["admin"])
def admin_advance(x_api_key: str | None = Header(None, alias="X-API-Key")):
    _check_key(x_api_key)
    _set_batch(2)
    return admin_state(x_api_key)


@app.post("/admin/reset", tags=["admin"])
def admin_reset(x_api_key: str | None = Header(None, alias="X-API-Key")):
    _check_key(x_api_key)
    _set_batch(1)
    return admin_state(x_api_key)
