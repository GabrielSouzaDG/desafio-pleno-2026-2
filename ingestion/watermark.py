"""Controle de watermark (estado de ingestão incremental) por fonte.

Por que o watermark é baseado em `updated_at` e não em `occurred_at`
---------------------------------------------------------------------
`occurred_at` é quando o evento aconteceu de fato no produto/atendimento.
`updated_at` é quando aquele registro ficou visível (ou foi corrigido) na
API. Os dois só coincidem no caso feliz.

A Mock API entrega, de propósito:
  - "late arrivals": eventos cujo `occurred_at` é de até 3 dias atrás, mas
    que só aparecem (via `updated_at`) num lote posterior;
  - "correções": o mesmo `event_id` reaparece depois com `updated_at` mais
    recente e valores diferentes (ex.: prioridade do ticket mudou).

Se a ingestão incremental filtrasse por `occurred_at >= watermark`, um late
arrival ou uma correção com `occurred_at` antigo NUNCA seria capturado numa
execução futura — porque o `occurred_at` dele já ficou para trás do
watermark, mesmo ele tendo acabado de "chegar" na API. `updated_at`, ao
contrário, é monotonicamente não decrescente por definição (é o timestamp
de quando o dado ficou visível): usar `updated_at` como watermark garante
que toda correção e todo late arrival sejam pegos na próxima execução
incremental, não importa o quão antigo seja o `occurred_at` deles.

Onde o estado é persistido
---------------------------
Arquivo JSON local (default `/tmp/watermarks.json`, sobrescrevível via env
var `WATERMARK_FILE`). Decisão deliberada de simplicidade: neste desafio a
ingestão roda em um único processo/host, sem necessidade de coordenação
distribuída, e um arquivo local evita introduzir mais uma dependência de
infraestrutura (uma tabela extra, uma conexão extra) só para guardar um
timestamp por fonte.

Em produção, com múltiplos workers, múltiplas execuções concorrentes ou a
necessidade de auditar/reprocessar o histórico de watermarks, eu moveria
isso para uma tabela de controle no PostgreSQL (transacional, fácil de
consultar) ou para uma tabela Iceberg de metadados como
`lakehouse.control.watermarks` (fica no mesmo lakehouse, versionada,
consultável via Trino) — qualquer uma das duas sobrevive à perda do disco
local de um worker específico, o que o arquivo local não garante.
"""

from __future__ import annotations

import json
import os
import threading
import time

DEFAULT_WATERMARK = "1970-01-01T00:00:00Z"
# Epoch, de propósito. Descobri rodando a ingestão de verdade contra o
# ambiente que crm.customers tem updated_at voltando até quase 2 anos antes
# de "hoje" (não só os ~120 dias do período dos eventos) — um default como
# "2026-01-01" (que parecia seguro olhando só a janela dos eventos)
# deixaria de fora, para sempre, todo cliente cujo updated_at cai antes
# dessa data: o watermark só anda para frente, então uma vez que ele
# ultrapassa esses registros mais antigos, eles nunca mais são buscados.
# Usar o epoch como default garante que a primeira execução é, na prática,
# uma carga completa da fonte — que é o comportamento correto para "nunca
# ingerido antes".
DEFAULT_PATH = os.environ.get("WATERMARK_FILE", "/tmp/watermarks.json")


class _FileLock:
    """Lock exclusivo baseado em arquivo sentinela (criação atômica via
    O_CREAT|O_EXCL). Funciona entre PROCESSOS diferentes, não só entre
    threads — importante porque `make ingest` pode ser disparado por um
    cron, pelo Airflow ou manualmente, e não há garantia de que seja
    sempre o mesmo processo Python guardando o estado.
    """

    def __init__(self, target_path: str, timeout: float = 10.0, poll_interval: float = 0.05):
        self._lock_path = f"{target_path}.lock"
        self._timeout = timeout
        self._poll_interval = poll_interval

    def __enter__(self) -> "_FileLock":
        deadline = time.monotonic() + self._timeout
        while True:
            try:
                fd = os.open(self._lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)
                return self
            except FileExistsError:
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"nao foi possivel obter o lock {self._lock_path} em {self._timeout}s "
                        "(outro processo pode ter morrido sem liberar o lock)"
                    )
                time.sleep(self._poll_interval)

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            os.remove(self._lock_path)
        except FileNotFoundError:
            pass


class WatermarkManager:
    """Guarda, por fonte (ex.: "events", "customers"), o `updated_at` da
    última ingestão incremental bem-sucedida.

    Thread-safe (lock em memória) e safe entre processos (file lock),
    com escrita atômica (grava em arquivo temporário e faz `os.replace`)
    para nunca deixar o arquivo de estado corrompido a meio de uma escrita.
    """

    def __init__(self, path: str | None = None):
        self._path = path or DEFAULT_PATH
        self._thread_lock = threading.Lock()
        target_dir = os.path.dirname(self._path)
        if target_dir:
            os.makedirs(target_dir, exist_ok=True)

    def get_watermark(self, source: str) -> str:
        """Devolve o watermark da fonte, ou `DEFAULT_WATERMARK` se a fonte
        ainda nunca foi ingerida."""
        with self._thread_lock, _FileLock(self._path):
            state = self._read()
            return state.get(source, DEFAULT_WATERMARK)

    def set_watermark(self, source: str, timestamp: str) -> None:
        """Atualiza o watermark da fonte. Deve ser chamado só depois que a
        ingestão daquela fonte terminou com sucesso (senão uma falha no
        meio do processo avançaria o watermark sem os dados terem sido
        de fato gravados na raw zone — perda silenciosa de dado)."""
        with self._thread_lock, _FileLock(self._path):
            state = self._read()
            state[source] = timestamp
            self._write(state)

    def _read(self) -> dict:
        if not os.path.exists(self._path):
            return {}
        try:
            with open(self._path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            # arquivo ausente/corrompido: melhor recomeçar do watermark
            # default do que travar a ingestão inteira por causa do estado.
            return {}

    def _write(self, state: dict) -> None:
        tmp_path = f"{self._path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, sort_keys=True)
        os.replace(tmp_path, self._path)  # atômico no mesmo filesystem
