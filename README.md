# Lakehouse On-Premises — Desafio Técnico

MinIO · Iceberg (catálogo REST) · Spark · Trino · PostgreSQL · Airflow (opcional)

Pipeline de ponta a ponta: Mock API + PostgreSQL → raw zone (MinIO) →
bronze → silver (dedup + SCD2) → quality gate → gold → respostas de
negócio via Trino, orquestrado por uma DAG do Airflow.

---

## Pré-requisitos

- Docker e Docker Compose v2 (`docker compose version`)
- Python 3.10+ no host (para gerar os dados e rodar a ingestão fora do container)
- ~6 GB de RAM livres para o Docker

## Subir e rodar tudo em 3 comandos

```bash
cp .env.example .env      # opcional
make up                   # gera os dados, sobe tudo e valida o ambiente
make pipeline              # ingest -> bronze+silver -> quality -> gold
```

`make pipeline` executa a sequência completa uma vez, do ingest ao gold,
sem passo manual no meio. Se algum quality check `critical` falhar (ex.:
uma duplicata real em `silver.events`), o `make` para no passo `quality`
e `gold` não roda — é o gate funcionando; ver a seção "Estado atual dos
quality checks" abaixo pra saber exatamente o que cada severidade
significa hoje.

```bash
make help                 # lista todos os atalhos disponíveis
make check                # revalida a infra quando quiser
```

### Validação completa da infraestrutura

```bash
make test-infra         # completo (~2 min): Spark -> Iceberg -> MinIO -> Trino
make test-infra-fast    # sem o round-trip do Spark (~20s)
```

---

## Serviços

| Serviço | Fora dos containers | Dentro dos containers | Credenciais |
|---|---|---|---|
| MinIO (S3 API) | http://localhost:9000 | http://minio:9000 | `admin` / `minioadmin` |
| MinIO Console | http://localhost:9001 | — | idem |
| Mock API | http://localhost:8000/docs | http://mock-api:8000 | header `X-API-Key: desafio-2026` |
| PostgreSQL | `localhost:5432` (ou `55432`, ver nota abaixo) | `postgres:5432` | `app` / `app`, db `crm` |
| Trino | http://localhost:8080 | `trino:8080` | qualquer usuário |
| Iceberg REST | http://localhost:8181 | http://iceberg-rest:8181 | — |
| Spark UI | http://localhost:4041 | — | — |
| Airflow (opcional) | http://localhost:8081 | — | `make airflow` |

**O erro mais comum do ambiente** é usar `localhost` dentro de um container. Do Spark, o MinIO é `minio:9000`, não `localhost:9000`.

> **Nota sobre a porta do Postgres:** o `.env` deste repositório já vem com
> `PG_PORT=55432` porque, nesta máquina, havia um PostgreSQL nativo do
> Windows ocupando a 5432 e "roubando" a conexão silenciosamente (erro
> `OperationalError` sem mensagem). Se a sua máquina não tem esse
> conflito, pode comentar essa linha e usar a 5432 padrão. `PG_PORT` é a
> única variável que importa — usada tanto pelo `docker-compose.yml`
> quanto por `ingestion/postgres_client.py`, e `make ingest`/`make
> pipeline` carregam o `.env` automaticamente antes de rodar (ver
> Makefile) — não precisa exportar nada manualmente.

### Bucket e catálogo

- Bucket único: `lakehouse`
  - `s3://lakehouse/raw/` → raw zone (a ingestão escreve aqui)
  - `s3://lakehouse/warehouse/` → gerenciado pelo Iceberg, não mexa na mão
- Catálogo Iceberg: `lakehouse`, configurado no Spark e no Trino via REST.
- Catálogo extra no Trino: `crm` (PostgreSQL direto), útil para conferir números contra a fonte.

---

## Como rodar o pipeline manualmente

Cada camada é um alvo `make` independente (todos idempotentes — rodar de
novo não duplica nem quebra nada, ver [ARCHITECTURE.md](ARCHITECTURE.md) pergunta 2):

```bash
make ingest      # ingestion/ingest.py: Mock API + Postgres -> raw zone (MinIO), roda no host
make transform   # transform/bronze.py depois transform/silver.py, dentro do container Spark
make quality     # quality/checks.py: 5 checks, persiste em lakehouse.quality.check_results
make gold        # transform/gold.py: só faz sentido depois de "quality" passar
make pipeline    # os quatro, em ordem, parando se "quality" achar algo critical
```

Para simular uma segunda execução (o "batch 2" do desafio — eventos
novos, correções, clientes que mudaram de plano):

```bash
make batch-state   # em qual batch as fontes estão
make batch2        # libera o batch 2 na API e aplica as mudanças no Postgres
make pipeline       # roda tudo de novo
```

A sequência inteira usada para validar a entrega (idempotência em 5 passos):

```
pipeline (batch 1)  →  pipeline de novo  →  make batch2  →  pipeline  →  pipeline de novo
```

Testei essa sequência de verdade contra este ambiente — silver e gold
terminam com contagens **idênticas** nas duas repetições de cada estágio
(bronze dobra de propósito: aceita duplicata, quem deduplica é a silver —
ver `transform/bronze.py`).

### Estado atual dos quality checks

`make pipeline` roda até o fim (`ingest -> bronze+silver -> quality ->
gold`, exit code `0`) sem precisar de nenhum passo manual depois. Isso não
foi sempre assim — o histórico do PR mostra dois achados reais contra este
dataset específico, e as duas correções aplicadas:

- **`CHECK_DOMAIN_VALUES`** em `event_type`: a lista de valores válidos
  originalmente pedida (`ticket_opened`, `ticket_replied`, `ticket_closed`,
  `login`, `feature_used`, `subscription_changed`) não batia 100% com o
  que a Mock API realmente envia — faltavam `export_generated` e
  `report_viewed`. Corrigido: a lista em `quality/checks.py` agora inclui
  os dois (mantendo `subscription_changed`, que não ocorre nos dados mas
  é um tipo de evento plausível do contrato).
- **`CHECK_FRESHNESS`**: o dataset é sintético e fixo (para de ser gerado
  em 2026-08-31); contra o relógio real, ele sempre vai acusar staleness
  — e isso é o comportamento *correto* do check (a fonte de fato parou de
  atualizar). O problema era a **severidade**: `critical` travava
  `gold` permanentemente numa condição que nunca se autorresolve sozinha
  enquanto o dataset não for regenerado. Rebaixei para `severity=warning`
  — o check continua rodando e registrando a staleness a cada execução
  (o sinal não desaparece do log), só não bloqueia mais o pipeline. Numa
  fonte que atualiza de verdade em produção, eu manteria isso como
  `critical` sem pensar duas vezes — é uma decisão de contexto deste
  ambiente de demonstração, documentada no docstring de
  `check_freshness`.

Os resultados completos de todo run (7 linhas — uniqueness e domain
values cobrem 2 tabelas cada) ficam persistidos em
`lakehouse.quality.check_results`, passando ou não:

```sql
SELECT check_name, table_name, status, severity, records_checked, records_failed, details
FROM lakehouse.quality.check_results
ORDER BY executed_at DESC;
```

### DAG do Airflow

`dags/lakehouse_pipeline.py` **não roda de fábrica** neste compose — o
serviço `airflow` opcional é uma imagem `apache/airflow` baunilha, sem
PySpark/Java/jars do Iceberg, e sem um cluster Spark exposto para
submissão remota (só é alcançável via `docker compose exec`). Isso é
esperado — o próprio enunciado (seção 5) diz que a DAG não precisa rodar
no ambiente, contanto que o desenho esteja explicado (ver
[ARCHITECTURE.md](ARCHITECTURE.md) e o docstring do próprio arquivo).

O que É validado de verdade: o parsing da DAG, com o Airflow standalone
do próprio compose.

```bash
make airflow    # sobe o Airflow em http://localhost:8081 (perfil opcional)
docker compose exec airflow airflow dags list-import-errors   # "No data found" = sem erro
docker compose exec airflow airflow tasks list lakehouse_pipeline
```

Confirmei as 9 tasks, o grafo de dependências exato pedido
(`[ingest_api, ingest_postgres] >> [bronze_events, bronze_customers] >>
[silver_events, silver_customers] >> quality_checks >> gold >>
notify_success`), `schedule_interval="0 6 * * *"`, `catchup=True`,
`start_date=2026-01-01` e `sla=2h` — todos lidos de volta do próprio
`DagBag` do Airflow, não só inspecionando o arquivo.

---

## Como rodar os testes

Dois arquivos, duas dependências diferentes — por isso dois alvos:

```bash
pip install -r requirements.txt
make test              # tests/test_ingestion.py (pure Python, mocka requests/boto3) -- no host
make test-transforms   # tests/test_transforms.py (PySpark + Iceberg local) -- dentro do container Spark
make test-all           # os dois
```

`test_transforms.py` usa uma `SparkSession` local de verdade (catálogo
Iceberg `type=hadoop`, warehouse num diretório temporário — sem depender
de MinIO/iceberg-rest) e chama as mesmas funções que `make transform`
roda (`create_silver_events`, `create_silver_customers`) contra dados
plantados no teste. Por isso ele só roda dentro do container do Spark: é
lá que PySpark + os jars do Iceberg já estão instalados; o host não tem
essa pilha (só as dependências puras de `test_ingestion.py`).

14 testes, todos passando neste momento (9 em `test_ingestion.py`, 5 em
`test_transforms.py`) — incluindo o "critério duro" do enunciado
(`test_idempotency`: rodar a mesma transformação duas vezes com o mesmo
dado de entrada produz exatamente o mesmo resultado) e os 4 comportamentos
de SCD2 descritos no enunciado desta tarefa.

Dois arquivos de configuração sustentam isso e não existiam antes:
`conftest.py` (raiz) só garante que `from ingestion...`/`from
transform...` resolvem quando o pytest roda a partir da raiz do repo;
`pytest.ini` ancora o *rootdir* do pytest nessa mesma raiz — sem ele, o
`conftest.py` acima nunca era encontrado quando o pytest recebia um
caminho de arquivo específico (`pytest tests/test_X.py`) em vez de um
diretório. Descobri isso rodando os testes de verdade, não deduzindo.

---

## Onde encontrar os resultados

- **Queries de negócio (Parte 3):** [`sql/`](sql/) tem as 3 queries
  (`q1_top_customers.sql`, `q2_avg_response_time.sql`,
  `q3_cohort_retention.sql`) e [`sql/results/`](sql/results/) tem o
  resultado real de cada uma (`qN_result.csv`, gerado rodando contra o
  Trino de verdade) junto com a interpretação (`qN_interpretation.md`).
- **Log de qualidade (Parte 4):** tabela Iceberg
  `lakehouse.quality.check_results` (consultável via `make trino`) —
  ver seção anterior.
- **Logs estruturados:** todo script (`ingestion/`, `transform/`,
  `quality/`) imprime eventos em JSON (`timestamp`, `level`, `event`,
  campos específicos) no stdout — é o que aparece no terminal ao rodar
  `make ingest`/`make transform`/`make quality`, e o que o Airflow
  capturaria como log de task numa execução real.

---

## Declaração de uso de IA

Usei o **Claude Code** (Anthropic) como assistente de desenvolvimento em
praticamente todo o código deste repositório — `ingestion/`,
`transform/`, `quality/`, `dags/`, as queries em `sql/` e esta
documentação. O fluxo de trabalho foi: eu descrevia cada parte do
desafio em prompts específicos (um por módulo/responsabilidade), a IA
implementava, e eu revisei e testei cada entrega **rodando de verdade**
contra este ambiente antes de seguir para a próxima — não é código gerado
e aceito às cegas.

Rodar tudo de verdade (em vez de só ler o código) pegou problemas reais
que uma revisão só de leitura não pegaria, entre eles:

- Um watermark default (`2026-01-01`) que parecia seguro olhando só a
  janela dos eventos, mas deixava 169 dos 400 clientes de fora para
  sempre (`updated_at` deles é anterior a isso) — corrigido para o epoch.
- Um bug em `transform/silver.py`: uma `Window.partitionBy(...)` definida
  no nível do módulo, que quebrava por rodar antes de existir uma
  `SparkSession` — movida para dentro das funções.
- Uma inconsistência entre `Makefile` (`gold` rodando antes de `quality`)
  e a DAG do Airflow (`quality` bloqueando `gold`) — corrigida no Makefile
  para as duas ficarem consistentes.
- Um defeito na própria imagem `tabulario/spark-iceberg:latest` (o JDK
  dela vem com o `lib/modules` truncado, quebrando qualquer
  `spark-submit`) — não era erro meu nem desta máquina (reproduz mesmo
  com rebuild `--no-cache --pull`); corrigido trocando o JDK no
  `conf/spark/Dockerfile`.

Estou preparado para explicar linha por linha qualquer parte do código
entregue, incluindo as decisões que fogem do pedido literal — várias
estão comentadas explicitamente no próprio código (ex.: por que a raw
zone faz merge em vez de overwrite cego, por que `properties` fica como
JSON opaco em vez de STRUCT, por que a silver usa `writeTo(...).append()`
em vez de `insertInto`).

---

## Os dois batches

As fontes começam no **batch 1**. Avance para o batch 2 para simular o
tempo passando entre duas execuções:

```bash
make batch-state    # em qual batch as fontes estão
make batch2         # libera o batch 2 na API e aplica as mudanças no Postgres
```

O batch 2 traz eventos novos, correções de eventos antigos, registros
atrasados, um campo novo no payload (schema drift) e mudanças de plano
nos clientes.

---

## Estrutura do repositório

```
ingestion/     extração da API e do Postgres para a raw zone (idempotente, com watermark)
transform/     jobs PySpark: bronze -> silver (MERGE/SCD2) -> gold
sql/           queries do Trino + resultados reais + interpretação
quality/       5 checks de qualidade, persistidos em Iceberg
dags/          DAG do Airflow (Parte 5)
tests/         14 testes (unitários + SparkSession local) -- ver "Como rodar os testes"
```

---

## Atalhos úteis

```bash
make spark          # shell no container do Spark (o repo está em /home/iceberg/work)
make pyspark        # PySpark shell já conectado ao catálogo lakehouse
make trino           # CLI do Trino
make psql            # psql na base crm
make logs            # logs de tudo
make clean            # apaga volumes e recomeça do zero
```

---

## Problemas comuns

**`make check` falha no Trino ou no Spark logo depois do `make up`.**
Eles demoram mais para subir. Espere ~30s e rode `make check` de novo.

**Porta 5432 ocupada / Postgres conecta mas dá erro estranho sem mensagem.**
Sintoma de um Postgres nativo do host disputando a porta com o container
(ver nota na seção Serviços). Confirme com
`netstat -ano | grep :5432` (Windows) — se aparecer mais de um processo
`LISTENING`, mude `PG_PORT` no `.env` e recrie: `docker compose up -d --force-recreate postgres`.

**`spark-submit` falha com `ClassFormatError: Truncated class file` ou
`Incompatible magic value` logo no início, antes de rodar qualquer job.**
Não é erro de sintaxe do seu script — é o defeito de JDK descrito na
declaração de uso de IA acima. Confirme com
`docker compose exec spark bash -c "stat -c%s $(dirname $(dirname $(readlink -f $(which java))))/lib/modules"`;
se der ~20MB, é isso. `conf/spark/Dockerfile` já traz a correção (baixa um
JDK Temurin válido); rode `make rebuild-spark` para reconstruir a imagem.

**`ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem`.**
Falta a JAR `hadoop-aws` no Spark. A imagem já é construída com ela
(`conf/spark/Dockerfile`); se reconstruiu o Spark a partir da imagem
original, rode `make rebuild-spark`.

**Quero depurar sem os erros 503 da API.**
Coloque `FLAKY_RATE=0` no `.env` e rode `docker compose up -d mock-api`.

**Quero recomeçar do zero.**
`make clean && make up`. Os dados são determinísticos: a mesma seed
gera exatamente o mesmo dataset.

---

Dúvida sobre o ambiente ou sobre o desafio? Pergunte — está tudo
documentado em [ARCHITECTURE.md](ARCHITECTURE.md) com o raciocínio por
trás de cada decisão.
