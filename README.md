# Ambiente do Desafio — Lakehouse On-Premises

MinIO · Iceberg (catálogo REST) · Spark · Trino · PostgreSQL · Airflow (opcional)

Este repositório sobe **toda a infraestrutura pronta**. Você não deve gastar tempo configurando o ambiente — o desafio é sobre o pipeline, não sobre YAML.

---

## Pré-requisitos

- Docker e Docker Compose v2 (`docker compose version`)
- Python 3.10+ no host (só para gerar os dados e, se quiser, rodar a ingestão fora do container)
- ~6 GB de RAM livres para o Docker

## Subir em 3 comandos

```bash
cp .env.example .env      # opcional
make up                   # gera os dados, sobe tudo e valida
make check                # revalida quando quiser
```

A primeira execução baixa as imagens e pode levar alguns minutos. Ao final, `make check` deve mostrar todas as verificações em verde, incluindo um teste real de escrita Spark → Iceberg → MinIO → leitura pelo Trino.

```bash
make help                 # lista todos os atalhos disponíveis
```

### Validação completa da infraestrutura

O `make check` é um smoke test rápido. Se quiser a validação profunda — que escreve no MinIO,
cria tabela Iceberg particionada, roda `MERGE INTO`, evolui o schema, faz time travel e lê a
mesma tabela pelo Trino:

```bash
make test-infra         # completo (~2 min, o Spark demora a iniciar)
make test-infra-fast    # sem o round-trip do Spark (~20s)
```

Ele devolve exit code 1 se algo estiver quebrado, com a dica de correção de cada falha.

---

## Serviços

| Serviço | Fora dos containers | Dentro dos containers | Credenciais |
|---|---|---|---|
| MinIO (S3 API) | http://localhost:9000 | http://minio:9000 | `admin` / `minioadmin` |
| MinIO Console | http://localhost:9001 | — | idem |
| Mock API | http://localhost:8000/docs | http://mock-api:8000 | header `X-API-Key: desafio-2026` |
| PostgreSQL | `localhost:5432` | `postgres:5432` | `app` / `app`, db `crm` |
| Trino | http://localhost:8080 | `trino:8080` | qualquer usuário |
| Iceberg REST | http://localhost:8181 | http://iceberg-rest:8181 | — |
| Spark UI | http://localhost:4041 | — | — |
| Airflow (opcional) | http://localhost:8081 | — | `make airflow` |

**O erro mais comum do ambiente** é usar `localhost` dentro de um container. Do Spark, o MinIO é `minio:9000`, não `localhost:9000`.

### Bucket e catálogo

- Bucket único: `lakehouse`
  - `s3://lakehouse/raw/` → sua raw zone (escreva aqui pela ingestão)
  - `s3://lakehouse/warehouse/` → gerenciado pelo Iceberg, **não mexa na mão**
- Catálogo Iceberg: `lakehouse`, já configurado no Spark **e** no Trino via catálogo REST.
  As tabelas que você criar no Spark aparecem no Trino imediatamente.
- Catálogo extra no Trino: `crm` (PostgreSQL direto), útil para conferir números contra a fonte.

---

## Atalhos

```bash
make spark          # shell no container do Spark (o repo está em /home/iceberg/work)
make pyspark        # PySpark shell já conectado ao catálogo lakehouse
make spark-sql      # spark-sql
make trino          # CLI do Trino
make psql           # psql na base crm
make logs           # logs de tudo
make clean          # apaga volumes e recomeça do zero
```

---

## Os dois batches

As fontes começam no **batch 1**. Quando você tiver o pipeline rodando, avance para o batch 2 — é assim que simulamos o tempo passando entre duas execuções:

```bash
make batch-state    # em qual batch as fontes estão
make batch2         # libera o batch 2 na API e aplica as mudanças no Postgres
```

O batch 2 traz eventos novos, **correções de eventos antigos**, registros atrasados, um campo novo no payload e mudanças de plano nos clientes.

A sequência que vamos executar para avaliar sua entrega:

```
pipeline (batch 1)  →  pipeline (batch 1 de novo)  →  make batch2  →  pipeline  →  pipeline de novo
```

Em nenhum desses passos pode haver duplicação, perda de dado ou quebra.

---

## Trechos de conexão

Para você não perder tempo com boilerplate. Adapte à vontade.

**Ler a Mock API (host):**

```python
import requests

s = requests.Session()
s.headers["X-API-Key"] = "desafio-2026"
r = s.get("http://localhost:8000/events", params={"since": "2026-08-01T00:00:00Z", "page": 1, "page_size": 500})
r.raise_for_status()
print(r.json()["total_pages"], r.json()["total_records"])
```

A API devolve `429` se você passar de 10 req/s e `503` em uma fração das chamadas. Isso é proposital.

**Escrever no MinIO (host, via boto3):**

```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="minioadmin",
    region_name="us-east-1",
)
s3.put_object(Bucket="lakehouse", Key="raw/events/ingestion_date=2026-08-31/part-000.json.gz", Body=b"...")
```

**Spark (dentro do container) — a sessão já vem configurada:**

```python
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
spark.sql("SHOW NAMESPACES IN lakehouse").show()

# ler a raw zone escrita pela ingestão
df = spark.read.json("s3a://lakehouse/raw/events/ingestion_date=2026-08-31/")
```

Rodar um job: `docker compose exec spark spark-submit /home/iceberg/work/transform/seu_job.py`

**Postgres:**

```python
import psycopg
with psycopg.connect("host=localhost port=5432 dbname=crm user=app password=app") as conn:
    print(conn.execute("select count(*) from crm.customers").fetchone())
```

---

## Estrutura sugerida

```
ingestion/     extração da API e do Postgres para a raw zone
transform/     jobs PySpark: bronze → silver → gold
sql/           queries do Trino + resultados
quality/       verificações de qualidade de dados
dags/          DAG do Airflow (montada em /opt/airflow/dags)
tests/         testes automatizados (bônus)
```

Os diretórios estão vazios de propósito — a organização interna é sua.

---

## Problemas comuns

**`make check` falha no Trino ou no Spark logo depois do `make up`.**
Eles demoram mais para subir. Espere ~30s e rode `make check` de novo.

**Porta ocupada (8080, 5432, 9000...).**
Não edite o compose: todas as portas do host são configuráveis pelo `.env`.

```bash
echo "TRINO_PORT=8085" >> .env
echo "PG_PORT=55432"   >> .env
docker compose up -d --force-recreate trino postgres
```

Para descobrir quem ocupa uma porta: `docker ps --format '{{.Names}}\t{{.Ports}}' | grep 8080`
ou `ss -ltnp | grep :8080`. Dentro dos containers os nomes e portas não mudam —
o Spark continua falando com `trino:8080` e `postgres:5432`.

Sintoma clássico de porta ocupada: o container sobe mas fica **sem rede**, e as
consultas falham com `The connection attempt failed` em poucos milissegundos.

**`ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem`.**
Falta a JAR `hadoop-aws` no Spark. A imagem do ambiente já é construída com ela
(`conf/spark/Dockerfile`); se você reconstruiu o Spark a partir da imagem original,
rode `make rebuild-spark`.

**`Access Denied` ou `NoSuchBucket` no Spark.**
Confira se está usando `s3a://lakehouse/...` para leitura direta de arquivos e `lakehouse.<schema>.<tabela>` para tabelas Iceberg. Tabelas precisam ser referenciadas com o nome completo do catálogo — não existe catálogo padrão configurado.

**Quero depurar sem os erros 503 da API.**
Coloque `FLAKY_RATE=0` no `.env` e rode `docker compose up -d mock-api`. Mas a entrega final precisa funcionar com o valor padrão — tratar falha faz parte do desafio.

**Quero recomeçar do zero.**
`make clean && make up`. Os dados são determinísticos: a mesma seed gera exatamente o mesmo dataset.

---

Dúvida sobre o ambiente ou sobre o enunciado? Pergunte. Não perca uma tarde travado em infraestrutura.
