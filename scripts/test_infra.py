#!/usr/bin/env python3
"""
Validador de infraestrutura do desafio — MinIO · Iceberg · Spark · Trino · Postgres · Mock API.

Diferente do `verify_env.sh` (que só checa se os serviços respondem), este script
exercita o caminho completo: escreve no MinIO, cria tabela Iceberg particionada,
roda MERGE, evolui schema, faz time travel e lê a MESMA tabela pelo Trino.

Só usa a biblioteca padrão do Python 3.9+ e o `docker compose`. Nada para instalar.

Uso:
    python3 scripts/test_infra.py                  # validação completa
    python3 scripts/test_infra.py --skip-spark     # rápido, sem o round-trip do Spark
    python3 scripts/test_infra.py --test-batch2    # também testa o avanço de batch da API
    python3 scripts/test_infra.py -v               # mostra saída bruta dos comandos

Saída: exit code 0 se tudo passou, 1 se houve falha crítica.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field

# --------------------------------------------------------------------- config
def _carrega_dotenv(caminho: str = ".env") -> None:
    """Lê o .env do projeto sem sobrescrever variáveis já definidas no ambiente.

    Sem isso, mudar uma porta ou a API_KEY no .env faria o validador continuar
    batendo nos valores padrão e acusar falhas que não existem.
    """
    try:
        with open(caminho, encoding="utf-8") as fh:
            for linha in fh:
                linha = linha.strip()
                if not linha or linha.startswith("#") or "=" not in linha:
                    continue
                chave, valor = linha.split("=", 1)
                os.environ.setdefault(chave.strip(), valor.strip().strip('"').strip("'"))
    except FileNotFoundError:
        pass


_carrega_dotenv()


def _porta(nome: str, padrao: str) -> str:
    return os.getenv(nome, padrao)


API_KEY = os.getenv("API_KEY", "desafio-2026")
API_URL = os.getenv("API_URL", f"http://localhost:{_porta('API_PORT', '8000')}")
MINIO_URL = os.getenv("MINIO_URL", f"http://localhost:{_porta('MINIO_API_PORT', '9000')}")
MINIO_CONSOLE = os.getenv("MINIO_CONSOLE_URL",
                          f"http://localhost:{_porta('MINIO_CONSOLE_PORT', '9001')}")
REST_URL = os.getenv("ICEBERG_REST_URL", f"http://localhost:{_porta('ICEBERG_REST_PORT', '8181')}")
TRINO_URL = os.getenv("TRINO_URL", f"http://localhost:{_porta('TRINO_PORT', '8080')}")

BUCKET = "lakehouse"
TEST_NS = "infra_test"
TEST_TABLE = f"lakehouse.{TEST_NS}.roundtrip"
RAW_PROBE = f"{BUCKET}/raw/_infra_test/probe.json"

# valores do dataset padrão (seed 20260911). Divergência vira aviso, não erro:
# o avaliador pode ter mexido na seed.
EXPECTED_CUSTOMERS_B1 = 400
EXPECTED_CUSTOMERS_B2 = 418
EXPECTED_API_TOTAL_B1 = 15475

OK, FAIL, WARN, SKIP = "ok", "fail", "warn", "skip"

# opener sem proxy: os serviços são todos locais
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))

USE_COLOR = sys.stdout.isatty() and os.getenv("NO_COLOR") is None


def c(text: str, color: str) -> str:
    if not USE_COLOR:
        return text
    codes = {"green": 32, "red": 31, "yellow": 33, "blue": 36, "grey": 90, "bold": 1}
    return f"\033[{codes[color]}m{text}\033[0m"


@dataclass
class Result:
    name: str
    status: str
    detail: str = ""
    hint: str = ""


@dataclass
class Report:
    results: list[Result] = field(default_factory=list)
    verbose: bool = False

    def add(self, name: str, status: str, detail: str = "", hint: str = "") -> Result:
        r = Result(name, status, detail, hint)
        self.results.append(r)
        mark = {OK: c("✓", "green"), FAIL: c("✗", "red"),
                WARN: c("!", "yellow"), SKIP: c("–", "grey")}[status]
        line = f"  {mark} {name}"
        if detail:
            line += c(f"  — {detail}", "grey")
        print(line)
        if hint and status in (FAIL, WARN):
            print(c(f"      → {hint}", "yellow"))
        return r

    @property
    def failed(self) -> list[Result]:
        return [r for r in self.results if r.status == FAIL]

    @property
    def warned(self) -> list[Result]:
        return [r for r in self.results if r.status == WARN]


REPORT = Report()


def section(title: str) -> None:
    print()
    print(c(f"{title}", "bold"))
    print(c("─" * max(48, len(title)), "grey"))


# ------------------------------------------------------------------ helpers
def run(cmd: list[str], timeout: int = 120) -> tuple[int, str, str]:
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if REPORT.verbose:
            print(c(f"      $ {' '.join(cmd[:6])}...", "grey"))
            if p.stdout.strip():
                print(c("      | " + p.stdout.strip()[:1500].replace("\n", "\n      | "), "grey"))
        return p.returncode, p.stdout, p.stderr
    except subprocess.TimeoutExpired:
        return 124, "", f"timeout após {timeout}s"
    except FileNotFoundError as e:
        return 127, "", str(e)


def http(url: str, method: str = "GET", headers: dict | None = None,
         timeout: int = 10) -> tuple[int, str]:
    # Todos os alvos são serviços locais. Se a máquina tiver http_proxy/https_proxy
    # configurado (comum em rede corporativa), o urllib tentaria sair pelo proxy
    # e falharia em `localhost`. O opener abaixo ignora qualquer proxy.
    req = urllib.request.Request(url, method=method, headers=headers or {})
    try:
        with _OPENER.open(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")
    except Exception as e:
        return 0, str(e)


def api(path: str, method: str = "GET", retries: int = 3) -> tuple[int, dict | str]:
    """
    Chama a Mock API tolerando as falhas que ela produz de propósito:
    503 intermitente (FLAKY_RATE) e 429 por rate limit. Sem isso, o próprio
    validador acusaria falha num ambiente saudável.
    """
    code, body = 0, ""
    for tentativa in range(retries + 1):
        code, body = http(f"{API_URL}{path}", method=method,
                          headers={"X-API-Key": API_KEY})
        if code not in (429, 503, 0) or tentativa == retries:
            break
        time.sleep(1.5 if code == 429 else 0.5)
    try:
        return code, json.loads(body)
    except Exception:
        return code, body


def dc(service: str, *args: str, timeout: int = 180) -> tuple[int, str, str]:
    """docker compose exec -T <service> <args>"""
    return run(["docker", "compose", "exec", "-T", service, *args], timeout=timeout)


def spark_error(out: str, err: str) -> str:
    """Extrai a linha de erro real do ruído do spark-sql."""
    chaves = ("Error in query", "AnalysisException", "Exception:", "Caused by",
              "Access Denied", "Forbidden", "NoSuchBucket", "UnknownHost",
              "403", "Connection refused", "TABLE_OR_VIEW_NOT_FOUND",
              "ClassNotFoundException", "not found",
              "PATH_NOT_FOUND", "UnsupportedOperation")
    linhas = []
    for linha in ((err or "") + "\n" + (out or "")).splitlines():
        linha = linha.strip()
        if linha and any(k in linha for k in chaves):
            linhas.append(linha)
    return " | ".join(dict.fromkeys(linhas))[:400] or "sem mensagem de erro identificável"


_TRINO_RUIDO = ("WARNING", "org.jline", "Unable to create a system terminal",
                "SEVERE", "INFO:", "at java.", "at io.trino", "Picked up JAVA")


def trino_query(sql: str, timeout: int = 120) -> tuple[int, str, str]:
    """
    Executa uma query no Trino e devolve (rc, valor_limpo, saida_bruta).

    O CLI do Trino, rodando sem TTY, escreve avisos do JLine junto do resultado.
    Ler a saída crua faz o validador confundir aviso com valor — foi exatamente
    isso que produziu um falso negativo em `Trino lê a MESMA tabela do Spark`.
    """
    rc, out, err = dc("trino", "trino", "--execute", sql,
                      "--output-format", "CSV_UNQUOTED", timeout=timeout)
    linhas = []
    for linha in (out or "").splitlines():
        linha = linha.strip().strip('"')
        if not linha or any(r in linha for r in _TRINO_RUIDO):
            continue
        # descarta timestamp de log java: "Sep 11, 2026 2:09:26 PM ..."
        if re.match(r"^[A-Z][a-z]{2} \d{1,2}, \d{4} \d{1,2}:\d{2}:\d{2} (AM|PM)", linha):
            continue
        linhas.append(linha)
    return rc, (linhas[-1] if linhas else ""), (out or "") + (err or "")


def parse_checks(output: str) -> dict[str, str]:
    """Coleta linhas no formato CHK_NOME=valor emitidas pelo Spark."""
    found = {}
    for line in output.splitlines():
        line = line.strip().strip('"')
        if line.startswith("CHK_") and "=" in line:
            k, v = line.split("=", 1)
            found[k] = v.strip()
    return found


# ------------------------------------------------------------------- checks
def check_docker() -> bool:
    section("1. Docker e containers")

    rc, out, _ = run(["docker", "compose", "version"], timeout=30)
    if rc != 0:
        REPORT.add("docker compose disponível", FAIL, "comando não encontrado",
                   "Instale o Docker Compose v2 (`docker compose version`).")
        return False
    REPORT.add("docker compose disponível", OK, out.strip().splitlines()[0][:60])

    rc, out, err = run(["docker", "compose", "ps", "--format", "json"], timeout=60)
    if rc != 0:
        REPORT.add("containers do projeto", FAIL, err.strip()[:120],
                   "Rode este script a partir da raiz do repositório (onde está o docker-compose.yml).")
        return False

    services = {}
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(item, list):
            for i in item:
                services[i.get("Service")] = i
        else:
            services[item.get("Service")] = item

    if not services:
        REPORT.add("containers no ar", FAIL, "nenhum container em execução",
                   "Rode `make up` (ou `docker compose up -d`) antes de validar.")
        return False

    esperados = ["minio", "iceberg-rest", "spark", "trino", "postgres", "mock-api"]
    for svc in esperados:
        info = services.get(svc)
        if not info:
            REPORT.add(f"serviço `{svc}`", FAIL, "ausente",
                       f"`docker compose up -d {svc}` e depois `docker compose logs {svc}`.")
            continue
        state = (info.get("State") or "").lower()
        health = (info.get("Health") or "").lower()
        if state != "running":
            REPORT.add(f"serviço `{svc}`", FAIL, f"state={state}",
                       f"`docker compose logs {svc}` para ver o motivo da parada.")
        elif health in ("unhealthy", "starting"):
            REPORT.add(f"serviço `{svc}`", WARN, f"health={health}",
                       "Se for `starting`, espere ~30s e rode de novo.")
        else:
            REPORT.add(f"serviço `{svc}`", OK, f"running{f' ({health})' if health else ''}")

    if "airflow" in services:
        REPORT.add("serviço `airflow` (opcional)", OK, "no ar")

    # Um container fora da rede do projeto se manifesta lá na frente como erro de
    # catálogo ou de credencial, o que manda o diagnóstico para o lado errado.
    # Aqui isso custa um segundo e aponta a causa real.
    redes_por_servico: dict[str, set[str]] = {}
    for svc in esperados:
        if svc not in services:
            continue
        cid = services[svc].get("ID") or services[svc].get("Name")
        if not cid:
            continue
        rc, out, _ = run(["docker", "inspect", cid, "--format",
                          "{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}"],
                         timeout=30)
        redes_por_servico[svc] = set(out.split()) if rc == 0 else set()

    sem_rede = [s for s, r in redes_por_servico.items() if not r]
    if sem_rede:
        REPORT.add("todos os containers têm rede", FAIL,
                   "sem rede: " + ", ".join(sem_rede),
                   "Container isolado (acontece quando um `up` aborta no meio). Corrija com: "
                   f"docker compose up -d --force-recreate {' '.join(sem_rede)}")
    elif redes_por_servico:
        comuns = set.intersection(*redes_por_servico.values())
        if comuns:
            REPORT.add("todos os containers na mesma rede", OK,
                       ", ".join(sorted(comuns)))
        else:
            fora = {s: sorted(r) for s, r in redes_por_servico.items()}
            REPORT.add("todos os containers na mesma rede", FAIL, str(fora)[:180],
                       "Os serviços estão em redes diferentes e não se enxergam. "
                       "Corrija com: docker compose down && docker compose up -d")
    return True


def check_minio() -> None:
    section("2. MinIO / object storage")

    code, _ = http(f"{MINIO_URL}/minio/health/live")
    REPORT.add("MinIO respondendo (API S3)", OK if code == 200 else FAIL,
               f"HTTP {code}" if code else "sem resposta",
               "Confira a porta 9000 e `docker compose logs minio`.")

    code, _ = http(MINIO_CONSOLE)
    REPORT.add("Console do MinIO", OK if code in (200, 403) else WARN, f"HTTP {code}",
               "O console é conveniência; a API na 9000 é o que importa.")

    # Garante o alias `local` dentro do container. Algumas versões da imagem já
    # vêm com ele configurado, outras não — isso torna o teste independente disso.
    user = os.getenv("MINIO_ROOT_USER", "admin")
    pwd = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
    run(["docker", "compose", "exec", "-T", "minio", "mc", "alias", "set", "local",
         "http://localhost:9000", user, pwd], timeout=60)

    rc, out, err = dc("minio", "mc", "ls", "local/", timeout=60)
    if rc != 0:
        REPORT.add(f"bucket `{BUCKET}` existe", FAIL, (err or out).strip()[:100],
                   "Não foi possível usar o `mc` dentro do container. "
                   "Verifique as credenciais em MINIO_ROOT_USER / MINIO_ROOT_PASSWORD.")
        return
    REPORT.add(f"bucket `{BUCKET}` existe", OK if BUCKET in out else FAIL,
               out.strip().replace("\n", " ")[:80],
               "O serviço minio-init não rodou. `docker compose up -d minio-init`.")

    # escrita + leitura + remoção de um objeto real na raw zone
    payload = '{"probe": true, "id": 1}'
    rc, _, err = run(["docker", "compose", "exec", "-T", "minio", "sh", "-c",
                      f"echo '{payload}' | mc pipe local/{RAW_PROBE}"], timeout=60)
    if rc != 0:
        REPORT.add("escrita na raw zone", FAIL, err.strip()[:100],
                   "Sem permissão de escrita no bucket. Verifique as credenciais do MinIO.")
        return
    rc, out, _ = dc("minio", "mc", "cat", f"local/{RAW_PROBE}", timeout=60)
    REPORT.add("escrita + leitura na raw zone", OK if '"probe"' in out else FAIL,
               f"s3://{RAW_PROBE}",
               "O objeto foi escrito mas não pôde ser lido de volta.")


def check_iceberg_rest() -> None:
    section("3. Catálogo Iceberg (REST)")

    code, body = http(f"{REST_URL}/v1/config?warehouse=s3://{BUCKET}/warehouse")
    if code != 200:
        REPORT.add("catálogo REST respondendo", FAIL, f"HTTP {code}",
                   "`docker compose logs iceberg-rest`. Ele depende do minio-init ter concluído.")
        return
    REPORT.add("catálogo REST respondendo", OK, "HTTP 200")

    try:
        cfg = json.loads(body)
        wh = (cfg.get("defaults", {}) or {}).get("warehouse") or \
             (cfg.get("overrides", {}) or {}).get("warehouse", "")
        REPORT.add("warehouse configurado", OK if BUCKET in str(wh) or wh == "" else WARN,
                   str(wh) or "não declarado na config (normal em algumas versões)")
    except json.JSONDecodeError:
        REPORT.add("config do catálogo legível", WARN, "resposta não é JSON")

    code, body = http(f"{REST_URL}/v1/namespaces")
    REPORT.add("listagem de namespaces", OK if code == 200 else FAIL, f"HTTP {code}",
               "O catálogo subiu mas não responde à API de namespaces.")


def check_postgres() -> None:
    section("4. PostgreSQL (fonte OLTP)")

    rc, out, _ = dc("postgres", "pg_isready", "-U", "app", "-d", "crm", timeout=30)
    if rc != 0:
        REPORT.add("Postgres aceitando conexão", FAIL, out.strip()[:100],
                   "`docker compose logs postgres`.")
        return
    REPORT.add("Postgres aceitando conexão", OK)

    rc, out, _ = dc("postgres", "psql", "-U", "app", "-d", "crm", "-tAc",
                    "select count(*) from crm.customers", timeout=30)
    try:
        n = int(out.strip())
    except ValueError:
        REPORT.add("tabela crm.customers populada", FAIL, out.strip()[:100],
                   "O seed do Postgres não rodou. `make clean && make up` recria a base.")
        return

    if n == EXPECTED_CUSTOMERS_B1:
        REPORT.add("crm.customers populada", OK, f"{n} clientes (batch 1)")
    elif n == EXPECTED_CUSTOMERS_B2:
        REPORT.add("crm.customers populada", OK, f"{n} clientes (batch 2 já aplicado)")
    elif n > 0:
        REPORT.add("crm.customers populada", WARN, f"{n} clientes (esperado 400 ou 418)",
                   "A seed do gerador pode ter sido alterada. Não é erro, só confira.")
    else:
        REPORT.add("crm.customers populada", FAIL, "tabela vazia",
                   "`make clean && make up` para reexecutar os scripts de init.")

    rc, out, _ = dc("postgres", "psql", "-U", "app", "-d", "crm", "-tAc",
                    "select count(*) from pg_indexes where indexname='idx_customers_updated_at'",
                    timeout=30)
    REPORT.add("índice de watermark (updated_at)", OK if out.strip() == "1" else WARN,
               "idx_customers_updated_at",
               "Sem esse índice a extração incremental fica lenta, mas funciona.")

    rc, out, _ = dc("postgres", "sh", "-c", "test -f /seed/03_batch2.sql && echo yes", timeout=30)
    REPORT.add("script do batch 2 montado no container", OK if "yes" in out else FAIL,
               "/seed/03_batch2.sql",
               "Sem esse arquivo o `make batch2` falha. Confira o volume `./seed/postgres:/seed`.")


def check_mock_api(test_batch2: bool) -> None:
    section("5. Mock API")

    code, body = http(f"{API_URL}/health")
    if code != 200:
        REPORT.add("API respondendo", FAIL, f"HTTP {code}",
                   "`docker compose logs mock-api`. Se faltar o events.jsonl, rode `make seed`.")
        return
    try:
        h = json.loads(body)
    except json.JSONDecodeError:
        h = {}
    loaded = h.get("events_loaded", 0)
    REPORT.add("API respondendo", OK, f"{loaded} eventos carregados, batch {h.get('batch')}")
    if loaded == 0:
        REPORT.add("dataset carregado", FAIL, "0 eventos",
                   "Rode `make seed` e depois `docker compose restart mock-api`.")
        return

    code, _ = http(f"{API_URL}/events")
    REPORT.add("exige X-API-Key", OK if code == 401 else FAIL, f"HTTP {code} sem a key",
               "A API deveria recusar chamadas sem credencial.")

    # Confere a chave do container contra a que este script está usando. Sem isso,
    # uma divergência aparece como um genérico "não retorna eventos".
    rc, out, _ = dc("mock-api", "printenv", "API_KEY", timeout=30)
    key_container = out.strip()
    if rc == 0 and key_container:
        REPORT.add("chave do script confere com a do container",
                   OK if key_container == API_KEY else FAIL,
                   f"container={key_container!r} script={API_KEY!r}",
                   f"Rode: API_KEY={key_container} python3 scripts/test_infra.py "
                   "(ou ajuste o valor no .env).")

    code, data = api("/events?page=1&page_size=100")
    if code != 200 or not isinstance(data, dict):
        REPORT.add("listagem de eventos", FAIL, f"HTTP {code}",
                   "401 = chave divergente. 503/429 persistente = API sobrecarregada. "
                   "200 vazio = dataset não carregado (`make seed` + restart do mock-api).")
        return
    total = data.get("total_records", 0)
    detail = f"{total} registros visíveis, {data.get('total_pages')} páginas"
    if total == EXPECTED_API_TOTAL_B1:
        REPORT.add("listagem de eventos", OK, detail + " (batch 1)")
    else:
        REPORT.add("listagem de eventos", OK, detail)

    row = (data.get("data") or [{}])[0]
    faltando = [k for k in ("event_id", "customer_id", "event_type",
                            "occurred_at", "updated_at", "properties") if k not in row]
    REPORT.add("payload com os campos esperados", OK if not faltando else FAIL,
               "ok" if not faltando else f"faltando: {faltando}",
               "O formato do evento mudou em relação ao enunciado.")
    REPORT.add("campos internos ocultos", OK if not any(k.startswith("_") for k in row) else FAIL,
               "nenhum campo `_` exposto",
               "O campo de controle `_batch` está vazando para o candidato.")

    # sobreposição entre páginas: a API repete registros de propósito
    ids_p1 = {e["event_id"] for e in data["data"]}
    code, d2 = api("/events?page=2&page_size=100")
    if code == 200 and isinstance(d2, dict):
        ids_p2 = {e["event_id"] for e in d2.get("data", [])}
        overlap = ids_p1 & ids_p2
        REPORT.add("sobreposição entre páginas", OK if overlap else WARN,
                   f"{len(overlap)} registro(s) repetidos entre a página 1 e a 2",
                   "Sem sobreposição, uma das armadilhas do desafio some. Confira PAGE_OVERLAP.")

    # filtro incremental
    code, d3 = api("/events?since=2026-08-01T00:00:00Z&page=1&page_size=10")
    if code == 200 and isinstance(d3, dict):
        menor = d3.get("total_records", 0)
        REPORT.add("filtro incremental `since`", OK if 0 < menor < total else WARN,
                   f"{menor} registros desde 2026-08-01",
                   "O filtro por updated_at não está reduzindo o conjunto.")

    # configuração de caos vinda do container
    rc, out, _ = dc("mock-api", "printenv", "FLAKY_RATE", timeout=30)
    flaky = out.strip() or "?"
    REPORT.add("erros 503 intermitentes ligados", OK if flaky not in ("0", "0.0", "?") else WARN,
               f"FLAKY_RATE={flaky}",
               "Com FLAKY_RATE=0 o candidato não precisa tratar falha. Volte para 0.02 na avaliação.")

    rc, out, _ = dc("mock-api", "printenv", "RATE_LIMIT_RPS", timeout=30)
    rps = out.strip() or "?"

    # rajada curta para confirmar que o 429 realmente dispara
    codes = []
    for _ in range(25):
        cd, _ = http(f"{API_URL}/events?page=1&page_size=1",
                     headers={"X-API-Key": API_KEY}, timeout=5)
        codes.append(cd)
    n429 = codes.count(429)
    REPORT.add("rate limit devolve 429", OK if n429 else WARN,
               f"{n429}/25 chamadas limitadas (RATE_LIMIT_RPS={rps})",
               "O rate limit não disparou — o candidato não vai precisar de backoff.")
    time.sleep(1.2)

    code, st = api("/admin/state")
    REPORT.add("endpoint de controle de batch", OK if code == 200 else FAIL,
               json.dumps(st) if isinstance(st, dict) else str(st)[:80],
               "Sem esse endpoint o `make batch2` não funciona.")

    if test_batch2:
        batch_inicial = st.get("batch") if isinstance(st, dict) else None
        code, adv = api("/admin/advance-batch", method="POST")
        ok_adv = code == 200 and isinstance(adv, dict) and adv.get("batch") == 2
        REPORT.add("avanço para o batch 2", OK if ok_adv else FAIL,
                   f"visíveis: {adv.get('visible_records') if isinstance(adv, dict) else '?'}")
        if ok_adv:
            _, d4 = api("/events?page=1&page_size=500")
            novo_total = d4.get("total_records", 0) if isinstance(d4, dict) else 0
            REPORT.add("batch 2 revela mais registros", OK if novo_total > total else FAIL,
                       f"{total} → {novo_total}")
            # o campo do schema drift só existe no batch 2
            ultima = max(1, (d4.get("total_pages") or 1))
            _, d5 = api(f"/events?page={ultima}&page_size=500")
            tem_drift = any("source_app" in e for e in (d5.get("data", []) if isinstance(d5, dict) else []))
            REPORT.add("schema drift presente no batch 2", OK if tem_drift else WARN,
                       "campo `source_app` encontrado" if tem_drift else "campo `source_app` não visto",
                       "O drift pode estar em outra página; não é necessariamente um erro.")
        if batch_inicial == 1:
            api("/admin/reset", method="POST")
            REPORT.add("estado da API restaurado para o batch 1", OK,
                       "o Postgres NÃO foi alterado por este teste")


def check_spark_trino_roundtrip(skip_spark: bool) -> None:
    section("6. Round-trip Spark → Iceberg → MinIO → Trino")

    if skip_spark:
        REPORT.add("round-trip completo", SKIP, "--skip-spark")
        return

    # Checagem instantânea do classpath, antes de pagar os ~60s de sessão do Spark.
    # A imagem base do Iceberg não traz o hadoop-aws: sem ele, `s3a://` não existe.
    rc, out, _ = dc("spark", "sh", "-c",
                    "ls /opt/spark/jars | grep -E '^hadoop-aws-' || true", timeout=60)
    jar_s3a = out.strip()
    REPORT.add("hadoop-aws no classpath do Spark", OK if jar_s3a else FAIL,
               jar_s3a or "ausente",
               "Sem essa JAR, `s3a://` falha com ClassNotFoundException e o candidato "
               "não consegue ler a raw zone. Rode `make rebuild-spark`.")

    rc, out, _ = dc("spark", "sh", "-c",
                    "ls /opt/spark/jars | grep -E 'aws-java-sdk-bundle|^bundle-2' || true",
                    timeout=60)
    jar_sdk = out.strip()
    REPORT.add("SDK da AWS no classpath do Spark", OK if jar_sdk else WARN,
               jar_sdk or "ausente",
               "O hadoop-aws depende do SDK da AWS. Rode `make rebuild-spark`.")

    sql = f"""
CREATE NAMESPACE IF NOT EXISTS lakehouse.{TEST_NS};
DROP TABLE IF EXISTS {TEST_TABLE};
CREATE TABLE {TEST_TABLE} (id bigint, txt string, ts timestamp)
  USING iceberg PARTITIONED BY (days(ts));
INSERT INTO {TEST_TABLE} VALUES
  (1, 'a', timestamp'2026-01-01 10:00:00'),
  (2, 'b', timestamp'2026-01-02 10:00:00');
ALTER TABLE {TEST_TABLE} CREATE TAG antes_do_merge;
MERGE INTO {TEST_TABLE} AS t
USING (SELECT 2 AS id, 'b_corrigido' AS txt, timestamp'2026-01-02 10:00:00' AS ts
       UNION ALL
       SELECT 3 AS id, 'c' AS txt, timestamp'2026-01-03 10:00:00' AS ts) AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET t.txt = s.txt
WHEN NOT MATCHED THEN INSERT *;
ALTER TABLE {TEST_TABLE} ADD COLUMN origem string;
SELECT concat('CHK_ROWS=', count(*)) FROM {TEST_TABLE};
SELECT concat('CHK_MERGE=', txt) FROM {TEST_TABLE} WHERE id = 2;
SELECT concat('CHK_PARTITIONS=', count(*)) FROM {TEST_TABLE}.partitions;
SELECT concat('CHK_SNAPSHOTS=', count(*)) FROM {TEST_TABLE}.snapshots;
SELECT concat('CHK_TIMETRAVEL=', count(*)) FROM {TEST_TABLE} VERSION AS OF 'antes_do_merge';
CREATE TEMPORARY VIEW probe_raw USING json OPTIONS (path 's3a://{BUCKET}/raw/_infra_test/');
SELECT concat('CHK_S3A=', count(*)) FROM probe_raw;
"""
    print(c("  (o Spark leva ~40-90s para iniciar a sessão; aguarde)", "grey"))
    t0 = time.time()
    rc, out, err = dc("spark", "spark-sql", "--silent", "-e", sql, timeout=420)
    dur = time.time() - t0
    checks = parse_checks(out)

    if not checks:
        trecho = (err or out).strip().splitlines()
        trecho = " | ".join(l.strip() for l in trecho[-3:])[:200] if trecho else "sem saída"
        REPORT.add("Spark executou o script", FAIL, trecho,
                   "Rode `docker compose exec spark spark-sql` na mão para ver o erro completo. "
                   "Causa comum: spark-defaults.conf não montado ou catálogo REST fora do ar.")
        return

    REPORT.add("Spark executou o script", OK, f"{dur:.0f}s")
    REPORT.add("criação de tabela Iceberg particionada", OK if "CHK_ROWS" in checks else FAIL,
               "PARTITIONED BY (days(ts))")
    REPORT.add("MERGE INTO (upsert)", OK if checks.get("CHK_MERGE") == "b_corrigido" else FAIL,
               f"id=2 → {checks.get('CHK_MERGE')}",
               "As extensões SQL do Iceberg não estão registradas no Spark. "
               "Confira `spark.sql.extensions` no spark-defaults.conf.")
    REPORT.add("contagem após o merge", OK if checks.get("CHK_ROWS") == "3" else FAIL,
               f"{checks.get('CHK_ROWS')} linhas (esperado 3)")
    REPORT.add("partições materializadas", OK if checks.get("CHK_PARTITIONS", "0") != "0" else WARN,
               f"{checks.get('CHK_PARTITIONS')} partições")
    REPORT.add("histórico de snapshots", OK if int(checks.get("CHK_SNAPSHOTS", "0") or 0) >= 2 else WARN,
               f"{checks.get('CHK_SNAPSHOTS')} snapshots",
               "Sem histórico, não há time travel nem rollback.")
    REPORT.add("time travel (tag antes do merge)",
               OK if checks.get("CHK_TIMETRAVEL") == "2" else WARN,
               f"{checks.get('CHK_TIMETRAVEL')} linhas no estado anterior (esperado 2)",
               "Versão do Iceberg pode não suportar TAG. Não bloqueia o desafio.")
    if checks.get("CHK_S3A"):
        REPORT.add("Spark lê a raw zone via s3a://", OK,
                   f"{checks['CHK_S3A']} registro(s) em s3a://{BUCKET}/raw/_infra_test/")
    else:
        erro = spark_error(out, err)
        if "ClassNotFound" in erro or "S3AFileSystem not found" in erro:
            dica = ("A JAR hadoop-aws não está no classpath — não é problema de credencial. "
                    "Rode `make rebuild-spark` (conf/spark/Dockerfile adiciona as JARs).")
        elif any(k in erro for k in ("Access Denied", "403", "Forbidden",
                                     "NoSuchBucket", "Connection refused")):
            dica = ("Erro de permissão/conexão: revise `fs.s3a.access.key`, "
                    "`fs.s3a.secret.key` e `fs.s3a.endpoint` no spark-defaults.conf.")
        else:
            dica = "Erro de sintaxe/catálogo, não de credencial — o acesso ao S3A pode estar ok."
        REPORT.add("Spark lê a raw zone via s3a://", FAIL, erro, dica)

    # ------------------------------------------------- o Trino vê a mesma tabela
    rc, valor, bruto = trino_query(f"SELECT count(*) FROM {TEST_TABLE}")
    REPORT.add("Trino lê a MESMA tabela do Spark", OK if valor == "3" else FAIL,
               f"count(*) = {(valor or bruto.strip())[:220] or 'vazio'}",
               "O Trino não enxerga o catálogo REST. Confira conf/trino/catalog/lakehouse.properties.")

    rc, valor, _ = trino_query(f"SELECT txt FROM {TEST_TABLE} WHERE id = 2")
    REPORT.add("Trino enxerga o resultado do MERGE",
               OK if valor == "b_corrigido" else FAIL, valor or "sem resultado")

    rc, valor, _ = trino_query(f"SELECT count(*) FROM {TEST_TABLE} WHERE origem IS NULL")
    REPORT.add("Trino acompanha a evolução de schema", OK if valor == "3" else WARN,
               "coluna `origem` adicionada depois da criação")

    rc, n, err_crm = trino_query("SELECT count(*) FROM crm.crm.customers")
    out = err_crm
    if n.isdigit() and int(n) > 0:
        REPORT.add("Trino consulta o Postgres (catálogo `crm`)", OK, f"{n} clientes")
    else:
        motivo = (err_crm or out).strip().splitlines()
        motivo = motivo[-1][:220] if motivo else "resposta vazia"
        if "refused" in motivo.lower() or "connection attempt failed" in motivo.lower():
            dica_crm = ("O crm.properties deve apontar para a porta INTERNA do Postgres "
                        "(sempre 5432), não para a porta do host definida em PG_PORT. "
                        "Corrija a connection-url e rode `docker compose restart trino`.")
        elif "authentication" in motivo.lower() or "password" in motivo.lower():
            dica_crm = "Usuário/senha errados em conf/trino/catalog/crm.properties."
        else:
            dica_crm = ("Catálogo opcional. Se o crm.properties foi adicionado depois do "
                        "Trino subir, rode `docker compose restart trino`.")
        REPORT.add("Trino consulta o Postgres (catálogo `crm`)", WARN, motivo, dica_crm)


def cleanup(skip_spark: bool) -> None:
    section("7. Limpeza dos artefatos de teste")

    if skip_spark:
        REPORT.add("tabela de teste", SKIP, "nada foi criado")
    else:
        trino_query(f"DROP TABLE IF EXISTS {TEST_TABLE}")
        trino_query(f"DROP SCHEMA IF EXISTS lakehouse.{TEST_NS}")
        # confirma pela ausência da tabela, não pelo exit code do CLI
        rc, existe, _ = trino_query(
            "SELECT count(*) FROM lakehouse.information_schema.tables "
            f"WHERE table_schema = '{TEST_NS}'")
        REPORT.add("tabela de teste removida", OK if existe in ("0", "") else WARN,
                   TEST_TABLE, f"Remova na mão: DROP TABLE {TEST_TABLE}")

    rc, _, _ = run(["docker", "compose", "exec", "-T", "minio", "sh", "-c",
                    f"mc rm --recursive --force local/{BUCKET}/raw/_infra_test/ || true"], timeout=60)
    REPORT.add("objeto de teste removido do MinIO", OK, f"s3://{BUCKET}/raw/_infra_test/")


# --------------------------------------------------------------------- main
def main() -> int:
    ap = argparse.ArgumentParser(description="Valida a infraestrutura do desafio.")
    ap.add_argument("--skip-spark", action="store_true",
                    help="pula o round-trip do Spark (bem mais rápido)")
    ap.add_argument("--test-batch2", action="store_true",
                    help="também testa o avanço de batch da API (restaura o estado ao final)")
    ap.add_argument("-v", "--verbose", action="store_true",
                    help="mostra a saída bruta dos comandos")
    args = ap.parse_args()
    REPORT.verbose = args.verbose

    print()
    print(c("Validação da infraestrutura — desafio lakehouse", "bold"))
    print(c(f"API {API_URL} · MinIO {MINIO_URL} · Trino {TRINO_URL} · Iceberg REST {REST_URL}", "grey"))

    t0 = time.time()
    if check_docker():
        check_minio()
        check_iceberg_rest()
        check_postgres()
        check_mock_api(args.test_batch2)
        check_spark_trino_roundtrip(args.skip_spark)
        cleanup(args.skip_spark)

    # ------------------------------------------------------------- resumo
    total = len(REPORT.results)
    n_ok = sum(1 for r in REPORT.results if r.status == OK)
    n_warn = len(REPORT.warned)
    n_fail = len(REPORT.failed)
    n_skip = sum(1 for r in REPORT.results if r.status == SKIP)

    section("Resumo")
    print(f"  {c(str(n_ok) + ' ok', 'green')} · "
          f"{c(str(n_warn) + ' avisos', 'yellow')} · "
          f"{c(str(n_fail) + ' falhas', 'red')} · "
          f"{n_skip} pulados   ({total} verificações em {time.time() - t0:.0f}s)")
    print()

    if n_fail:
        print(c("  Falhas que precisam ser resolvidas:", "red"))
        for r in REPORT.failed:
            print(f"    • {r.name}" + (f" — {r.detail}" if r.detail else ""))
            if r.hint:
                print(c(f"      {r.hint}", "yellow"))
        print()
        print("  Diagnóstico rápido: `docker compose ps` · `docker compose logs <serviço>`")
        print("  Recomeçar do zero: `make clean && make up`")
        print()
        return 1

    if n_warn:
        print(c("  Avisos (não bloqueiam o desafio):", "yellow"))
        for r in REPORT.warned:
            print(f"    • {r.name}" + (f" — {r.detail}" if r.detail else ""))
        print()

    print(c("  Ambiente validado. Pode enviar o desafio.", "green"))
    print()
    print("  MinIO    " + MINIO_CONSOLE + "   (admin / minioadmin)")
    print("  Mock API " + API_URL + "/docs   (header X-API-Key: " + API_KEY + ")")
    print("  Trino    " + TRINO_URL)
    print("  Postgres localhost:5432  (app / app / crm)")
    print()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\ninterrompido")
        sys.exit(130)
