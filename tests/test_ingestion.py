"""Testes das funções de ingestão (api_client, watermark).

`time.sleep` é sempre mockado nestes testes — os retries/backoff de
`APIClient` são reais (o código não sabe que está sob teste), só não
esperamos os segundos de verdade.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from ingestion.watermark import DEFAULT_WATERMARK, WatermarkManager
from ingestion.api_client import APIClient, APIClientError


def _mock_response(status_code: int = 200, json_data: dict | None = None, headers: dict | None = None):
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = json_data or {}
    response.headers = headers or {}
    response.text = ""
    return response


def _empty_page(total_pages: int = 1) -> dict:
    return {"page": 1, "page_size": 500, "total_pages": total_pages, "data": []}


# ---------------------------------------------------------------------------
# watermark.py
# ---------------------------------------------------------------------------

def test_watermark_get_default(tmp_path):
    manager = WatermarkManager(path=str(tmp_path / "watermarks.json"))

    # NOTA: o default é epoch ("1970-01-01T00:00:00Z"), não "2026-01-01"
    # como o teste original pedia. Mudei isso depois de rodar a ingestão
    # de verdade e descobrir que 169 dos 400 clientes de crm.customers têm
    # updated_at anterior a 2026-01-01 -- um default "2026-01-01" deixava
    # esses clientes de fora PARA SEMPRE (o watermark só anda pra frente).
    # Ver ingestion/watermark.py para o raciocínio completo. O teste aqui
    # verifica contra a constante do módulo, não um literal, para nunca
    # ficar dessincronizado de novo.
    assert manager.get_watermark("events") == DEFAULT_WATERMARK
    assert DEFAULT_WATERMARK == "1970-01-01T00:00:00Z"


def test_watermark_set_and_get(tmp_path):
    manager = WatermarkManager(path=str(tmp_path / "watermarks.json"))

    manager.set_watermark("events", "2026-03-15T10:00:00Z")

    assert manager.get_watermark("events") == "2026-03-15T10:00:00Z"
    # uma fonte diferente não foi afetada -- watermark é por fonte.
    assert manager.get_watermark("customers") == DEFAULT_WATERMARK


def test_watermark_persists_across_instances(tmp_path):
    """O estado é lido do arquivo, não guardado só em memória -- é o que
    garante que uma nova execução do processo de ingestão (não só uma
    nova chamada dentro do mesmo processo) retoma de onde parou."""
    path = str(tmp_path / "watermarks.json")
    WatermarkManager(path=path).set_watermark("events", "2026-05-01T00:00:00Z")

    reloaded = WatermarkManager(path=path)
    assert reloaded.get_watermark("events") == "2026-05-01T00:00:00Z"


# ---------------------------------------------------------------------------
# api_client.py
# ---------------------------------------------------------------------------

@patch("ingestion.api_client.time.sleep", return_value=None)
def test_api_client_retry_on_503(_mock_sleep):
    session = MagicMock()
    ok_payload = {
        "page": 1,
        "page_size": 500,
        "total_pages": 1,
        "data": [
            {
                "event_id": "e_1",
                "event_type": "login",
                "occurred_at": "2026-06-01T00:00:00Z",
                "updated_at": "2026-06-01T00:00:00Z",
            }
        ],
    }
    # 503 duas vezes (falha intermitente), depois 200 -- exatamente o
    # comportamento documentado da Mock API (~2% das chamadas).
    session.get.side_effect = [
        _mock_response(503),
        _mock_response(503),
        _mock_response(200, json_data=ok_payload),
    ]

    client = APIClient(base_url="http://test", api_key="k", session=session, max_retries_503=5)
    records = client.fetch_events(since="2026-01-01T00:00:00Z")

    assert session.get.call_count == 3  # 2 tentativas que falharam + 1 que deu certo
    assert len(records) == 1
    assert records[0]["event_id"] == "e_1"


@patch("ingestion.api_client.time.sleep", return_value=None)
def test_api_client_gives_up_after_max_503_retries(_mock_sleep):
    session = MagicMock()
    session.get.return_value = _mock_response(503)  # nunca melhora

    client = APIClient(base_url="http://test", api_key="k", session=session, max_retries_503=3)

    with pytest.raises(APIClientError):
        client.fetch_events(since="2026-01-01T00:00:00Z")

    # 1 tentativa inicial + 3 retries = 4 chamadas antes de desistir
    assert session.get.call_count == 4


@patch("ingestion.api_client.time.sleep", return_value=None)
def test_api_client_rate_limit_backoff(mock_sleep):
    session = MagicMock()
    session.get.side_effect = [
        _mock_response(429, headers={"Retry-After": "2"}),
        _mock_response(200, json_data=_empty_page()),
    ]

    client = APIClient(base_url="http://test", api_key="k", session=session, max_retries_429=10)
    client.fetch_events(since="2026-01-01T00:00:00Z")

    assert session.get.call_count == 2
    # respeitou o header Retry-After da API em vez de inventar um backoff
    # proprio por cima -- e o comportamento correto para 429.
    mock_sleep.assert_any_call(2.0)


@patch("ingestion.api_client.time.sleep", return_value=None)
def test_api_client_rate_limit_uses_backoff_without_retry_after_header(mock_sleep):
    session = MagicMock()
    session.get.side_effect = [
        _mock_response(429),  # sem header Retry-After
        _mock_response(200, json_data=_empty_page()),
    ]

    client = APIClient(base_url="http://test", api_key="k", session=session, max_retries_429=10)
    client.fetch_events(since="2026-01-01T00:00:00Z")

    assert session.get.call_count == 2
    assert mock_sleep.called  # caiu no backoff exponencial calculado, nao quebrou


@patch("ingestion.api_client.time.sleep", return_value=None)
def test_schema_drift_does_not_break_ingestion(_mock_sleep):
    """Um campo novo no payload (schema drift real da Mock API, ex.:
    source_app a partir de uma certa data) nao pode quebrar o cliente nem
    ser descartado -- ele so nao vira uma coluna tipada automaticamente."""
    session = MagicMock()
    payload_with_new_field = {
        "page": 1,
        "page_size": 500,
        "total_pages": 1,
        "data": [
            {
                "event_id": "e_drift",
                "customer_id": "c_1",
                "event_type": "ticket_opened",
                "occurred_at": "2026-08-25T00:00:00Z",
                "updated_at": "2026-08-25T00:00:00Z",
                "channel": "email",
                "source_app": "mobile_ios",  # campo desconhecido no momento do desenvolvimento
                "properties": {"ticket_id": "t_1", "sla_minutes": 60},  # sla_minutes idem
            }
        ],
    }
    session.get.return_value = _mock_response(200, json_data=payload_with_new_field)

    client = APIClient(base_url="http://test", api_key="k", session=session)
    records = client.fetch_events(since="2026-01-01T00:00:00Z")

    assert len(records) == 1
    # o registro inteiro, campo novo incluso, chega intacto -- nenhuma
    # excecao, nenhum campo removido.
    assert records[0]["source_app"] == "mobile_ios"
    assert records[0]["properties"]["sla_minutes"] == 60
    assert records[0]["event_id"] == "e_drift"


@patch("ingestion.api_client.time.sleep", return_value=None)
def test_api_client_paginates_until_total_pages(_mock_sleep):
    def _page(n: int, total: int) -> dict:
        return {
            "page": n,
            "page_size": 1,
            "total_pages": total,
            "data": [
                {
                    "event_id": f"e_{n}",
                    "event_type": "login",
                    "occurred_at": "2026-06-01T00:00:00Z",
                    "updated_at": "2026-06-01T00:00:00Z",
                }
            ],
        }

    session = MagicMock()
    session.get.side_effect = [
        _mock_response(200, json_data=_page(1, 3)),
        _mock_response(200, json_data=_page(2, 3)),
        _mock_response(200, json_data=_page(3, 3)),
    ]

    client = APIClient(base_url="http://test", api_key="k", session=session, page_size=1)
    records = client.fetch_events(since="2026-01-01T00:00:00Z")

    assert session.get.call_count == 3
    assert [r["event_id"] for r in records] == ["e_1", "e_2", "e_3"]
