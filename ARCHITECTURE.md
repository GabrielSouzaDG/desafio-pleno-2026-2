# ARCHITECTURE.md

## 1. Por que particionou as tabelas desse jeito? Qual seria o impacto de particionar diferente?

| Tabela | Partição | Por quê |
|---|---|---|
| `bronze.events` | `days(occurred_at)` | Bronze é append-only e recebe carga por dia de negócio; particionar por dia dá rastreabilidade 1:1 com a raw zone (`raw/events/ingestion_date=.../`) e permite reprocessar/inspecionar um dia específico sem tocar nos outros. |
| `bronze.customers` | `days(updated_at)` | Mesmo raciocínio — `updated_at` é o eixo pelo qual essa tabela cresce (é a coluna do watermark incremental). |
| `silver.events` | `months(occurred_at)` | As 3 queries de negócio (Parte 3) filtram por mês ou por janela de 30 dias, nunca por dia isolado. Granularidade diária aqui geraria muito mais partições do que o `MERGE INTO` da silver realmente precisa tocar por execução — e cada `MERGE` rescreve arquivo inteiro por partição afetada (ver pergunta 5), então partição fina = mais arquivos pequenos reescritos por corrida. Mensal equilibra isso com o pruning que as queries realmente usam. |
| `silver.customers` | `months(valid_from)` | SCD2: `valid_from` é quando uma versão passou a valer. Particionar por isso agrupa "quando a mudança aconteceu", que é exatamente o eixo das perguntas de negócio (retenção por coorte, plano na data do evento). |
| `gold.events_daily_summary` | `months(event_date)` | Alinhado com a granularidade de consumo (Q1 filtra por janela de 30 dias sobre esta tabela). |
| `gold.ticket_events` | `months(occurred_at)` | Mesma lógica — Q2 agrupa por mês. |
| `gold.customer_monthly_activity` | `months(activity_month)` | Não foi pedido explicitamente no enunciado da tabela 3, mas seguí o mesmo padrão por consistência — é como ela é consultada em Q3 (cohort por mês). |
| `quality.check_results` | `days(executed_at)` | É um log de execuções, não uma tabela analítica — particiono por dia só para não acumular tudo numa partição única conforme o histórico cresce; não há query de negócio direcionando essa escolha. |
| `quality.volume_history` | `months(metric_date)` | Segue o padrão das tabelas de série temporal do projeto. |

**Impacto de particionar diferente**, com números reais deste dataset
(18.658 eventos em silver, ~90 dias de dados):

- **Diário na silver:** cada `MERGE INTO` que toca eventos espalhados por
  vários dias (o caso comum — silver relê bronze inteira a cada run, ver
  pergunta 2) reescreveria dezenas de partições de uma vez, cada uma com
  poucas centenas de linhas. É exatamente o cenário de *small files*
  (pergunta 5) — muitos arquivos pequenos por execução, sem ganho de
  pruning real (as queries não filtram por dia).
- **Anual (ou sem partição):** o `MERGE INTO` passaria a reescrever uma
  fração enorme da tabela mesmo quando só um dia mudou (Iceberg identifica
  arquivos candidatos ao MERGE por overlap de partição — sem partição,
  vira um full scan/rewrite a cada correção). Para este volume (18k
  linhas) isso nem apareceria como problema; com 100x o volume
  (pergunta 4), seria o primeiro gargalo visível.

## 2. Como você garantiu idempotência em cada etapa?

- **Raw (ingestão):** *não* é overwrite ingênuo por `ingestion_date`, é um
  **merge deduplicado por linha** (`ingestion/ingest.py:RawZoneWriter.write_jsonl_gz`).
  Descobri rodando de verdade que overwrite simples quebraria: a sequência
  de avaliação roda o pipeline mais de uma vez no mesmo dia, inclusive com
  uma carga nova no meio (`make batch2`) — overwrite ingênuo apagaria o
  que a execução anterior daquele dia já tinha gravado. A escrita lê o
  conteúdo já existente na key, remove duplicatas exatas linha-a-linha, e
  faz um único `put_object` (atômico — sem leitor vendo a key pela
  metade). Reexecução sem dado novo vira no-op.
- **Watermark:** `ingestion/watermark.py` só avança **depois** do upload
  no MinIO ser confirmado (`ingest.py:_ingest_source`). Se o processo cair
  entre buscar e gravar, o watermark fica onde estava e a próxima execução
  repete o intervalo — seguro, porque o passo de escrita acima é
  idempotente.
- **Bronze:** append-only, aceita duplicata **de propósito**
  (`transform/bronze.py`). Rodar bronze duas vezes para o mesmo
  `ingestion_date` insere as linhas de novo, com um `_batch_id` novo — a
  garantia de "sem duplicata no resultado final" não é responsabilidade
  do bronze, é da silver.
- **Silver events:** `MERGE INTO` por `event_id`, com
  `WHEN MATCHED AND source.updated_at > target.updated_at THEN UPDATE SET *`
  — uma linha nova só sobrescreve se for genuinamente mais recente; uma
  duplicata exata (mesmo `updated_at`) não gera update. Antes do MERGE, um
  `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY updated_at DESC, ...)`
  reduz a bronze (que pode ter duplicata) a 1 candidato por chave.
- **Silver customers (SCD2):** dois `MERGE INTO` (fecha versão antiga,
  abre nova) — ver pergunta 3. Idempotente porque a mudança só é detectada
  comparando contra o estado `is_current=true` já persistido: rodar de
  novo sem mudança real não fecha nem abre nenhuma versão.
- **Gold:** as 3 tabelas são recomputadas inteiras a partir da silver
  (que já está correta e deduplicada) e reconciliadas via `MERGE INTO`
  pela chave de negócio de cada uma (`(event_date, customer_id,
  event_type)` em `events_daily_summary`; `event_id` em `ticket_events`;
  `(activity_month, customer_id)` em `customer_monthly_activity`) —
  incluindo comparação null-safe (`<=>`) onde a chave pode ser nula
  (`customer_id` em `events_daily_summary`).
- **Quality:** a tabela de log (`lakehouse.quality.check_results`) é
  **append-only e não é idempotente de propósito** — cada execução é um
  evento imutável no histórico, não um estado a ser reconciliado. A
  idempotência aqui é sobre os *checks* rodarem sem side-effect (eles só
  leem silver/customers e escrevem no log; rodar de novo não corrompe
  nada, só adiciona mais linhas ao histórico, que é o comportamento
  correto de um log).

**Validado rodando de verdade** a sequência completa do enunciado —
`pipeline(batch1) → de novo → make batch2 → pipeline → de novo` — 5
vezes: silver e gold terminam com contagens **idênticas** nas duas
repetições de cada estágio (batch1: 15.256 eventos / 400 clientes nas
duas rodadas; pós-batch2: 18.658 eventos / 413 clientes correntes nas
duas rodadas, com `versions_closed=0, versions_opened=0` na segunda).
Dois bugs reais só apareceram nessa validação (não na leitura do código):
um watermark default que deixava clientes antigos de fora para sempre
(corrigido), e uma `Window` do PySpark definida no nível do módulo que
quebrava por rodar antes de existir `SparkSession` (corrigida).

## 3. Como modelou a mudança de plano do cliente ao longo do tempo, e por quê?

**SCD Tipo 2** em `lakehouse.silver.customers`: `valid_from` (quando essa
versão passou a valer), `valid_to` (quando foi substituída — `NULL` =
ainda vigente), `is_current` (bool, atalho para não precisar de
`valid_to IS NULL` em toda query) e `_record_version` (posição da versão
na linha do tempo daquele cliente). O intervalo é meio-aberto
`[valid_from, valid_to)`, sem gap nem overlap entre versões consecutivas
do mesmo cliente (`valid_to` da versão antiga == `valid_from` da versão
nova).

Query canônica ("qual era o plano do cliente na data do evento"),
implementada em `transform/silver.py:PLAN_AS_OF_EVENT_QUERY` e reusada em
`sql/q2_avg_response_time.sql`:

```sql
SELECT c.plan
FROM lakehouse.silver.customers c
WHERE c.customer_id = :id
  AND c.valid_from <= :occurred_at
  AND (c.valid_to > :occurred_at OR c.valid_to IS NULL)
```

Implementado com **dois `MERGE INTO`** (`transform/silver.py:create_silver_customers`),
porque um único `MERGE INTO ... ON customer_id` só pode ou casar com a
versão atual, ou não casar com nada — não existe "casar e inserir uma
linha nova" na mesma cláusula:

1. **Fecha**: quem já tinha versão `is_current=true` E teve `plan`,
   `segment`, `is_active` ou `country` mudando (comparação null-safe) E a
   nova leitura é mais recente → `UPDATE SET valid_to, is_current=false`.
2. **Abre**: quem não tem versão `is_current=true` depois do passo 1 —
   cliente novo (nunca teve linha) OU cliente que acabou de ser fechado —
   → `INSERT` com `_record_version` = `MAX(_record_version)` anterior + 1.

**Alternativas consideradas:**

- **SCD Tipo 1** (sobrescrever o registro do cliente): mais simples, mas
  perde o histórico — Q2 (tempo de resposta por plano) e a interpretação
  de Q1 (segmentar por plano) ficariam incorretas para qualquer ticket
  aberto antes da última mudança de plano do cliente, porque atribuiriam
  retroativamente o plano atual a eventos passados.
- **Tabela de log de mudanças** (uma linha por alteração, sem
  materializar o estado): mais barata para escrever, mas empurra a
  reconstrução do "estado em um instante" para toda query de leitura —
  em vez de um `JOIN` com intervalo (barato, indexável por partição), toda
  consulta precisaria de uma window function sobre o log inteiro daquele
  cliente. Pra um padrão de acesso predominantemente analítico (que é o
  caso aqui — pouquíssimas escritas, muitas leituras via Trino), inverter
  esse custo para a escrita (SCD2) compensa.

**Trade-off aceito:** mais espaço (a tabela cresce a cada mudança, não
substitui) e o `JOIN` fica com uma condição de intervalo em vez de
igualdade simples — mas o histórico completo fica preservado e
consultável sem reprocessamento.

**Limitação conhecida** (documentada no código,
`transform/silver.py`, topo do módulo): a silver processa "o estado mais
recente por cliente" olhando a bronze inteira a cada execução. Se a
`silver.customers` nunca tivesse rodado entre duas cargas diferentes já
acumuladas em bronze (ex.: batch1 e batch2 juntos, sem nenhuma execução
da silver entre eles), uma transição intermediária seria perdida — só a
versão final viraria uma linha. Isso não acontece na sequência de
avaliação do desafio (a silver roda entre um batch e outro), mas é uma
limitação real de um pipeline que não rastreia o incremento processado
por si só (ver pergunta 6).

## 4. O que aconteceria com esse pipeline se o volume crescesse 100×? O que quebraria primeiro?

Ordem provável, do que quebra primeiro para o que aguenta mais tempo:

1. **`silver.py` releria e reordenaria a bronze inteira a cada execução**
   (documentado explicitamente no código — não é uma suposição). Hoje são
   18.658/1.700 linhas por `ROW_NUMBER()`; com 100×, são ~1,9M/170k
   linhas **por execução**, mesmo que só um dia tenha mudado. É o
   primeiro gargalo real, porque cresce com o **histórico acumulado**, não
   com o incremento do dia.
2. **`MERGE INTO` sem alinhamento entre a chave de merge e a partição.**
   `gold.events_daily_summary` e `customer_monthly_activity` fazem MERGE
   por `customer_id` (entre outras colunas), mas nenhuma tabela é
   particionada por `customer_id` — o Iceberg precisa escanear/reescrever
   mais arquivos do que o estritamente necessário para aplicar o merge.
   Com 100× os dados, esse overhead deixa de ser desprezível.
3. **Watermark em arquivo local** (`ingestion/watermark.py`) — decisão
   documentada como "simplicidade para este ambiente", explicitamente com
   a ressalva de que não sobrevive a múltiplos workers concorrentes. Com
   100× o volume, ingestão paralela por partição/fonte vira necessidade
   real, e um arquivo `.json` com lock local não coordena processos em
   hosts diferentes.
4. **`spark.driver.memory=2g`** (`conf/spark/spark-defaults.conf`) — hoje
   sobra; com 100×, os `.collect()` usados nos quality checks
   (`quality/checks.py`, ex.: amostras de 10 IDs órfãos) continuam
   pequenos e seguros, mas os `.count()`/`.cache()` no meio das
   transformações passam a pressionar memória do driver antes de qualquer
   ajuste de shuffle partitions.
5. **Trino sem tabela materializada extra para Q3.** A query de retenção
   já faz um `CROSS JOIN` (coortes × meses) — funciona bem com ~24
   coortes × ~24 meses (~500 linhas) neste dataset; com 100× o número de
   clientes/coortes, esse produto cresce e a query deixa de ser
   interativa sem uma tabela gold pré-agregada dedicada a retenção (hoje
   ela é calculada só no SQL de consumo, não numa tabela gold).

**O que eu faria:**
- Silver incremental: filtrar bronze por `_ingested_at > watermark_do_ultimo_run_da_silver`
  antes do `ROW_NUMBER`, tratando só o delta (precisa de uma tabela de
  controle própria da silver — mesma decisão de design do item abaixo).
- Watermark (ingestão e silver) numa tabela com controle transacional —
  Postgres ou uma tabela Iceberg `lakehouse.control.*` — em vez de arquivo
  local, permitindo múltiplos workers.
- Particionar/bucketing das tabelas gold pela mesma coluna usada na chave
  de merge (`customer_id`) quando ela for de fato a chave de merge
  dominante.
- Cluster Spark com mais executores (hoje é só o driver local,
  `local[*]` implícito) e revisar `spark.sql.shuffle.partitions` (hoje
  fixo em 8, no `spark-defaults.conf` do ambiente) para o volume novo.

## 5. O problema de small files no Iceberg: ele existe na sua solução? Como você resolveria?

**Existe, e é possível observar diretamente neste projeto.** Rodei o
pipeline 4 vezes seguidas (a sequência de avaliação): a `bronze.events`
foi de 15.256 → 30.512 → 49.350 → 68.188 linhas, com um `_batch_id`
(e portanto um conjunto novo de arquivos Parquet) por execução — bronze é
append-only por decisão de design (pergunta 2), então cada `spark-submit`
que toca uma partição já existente adiciona arquivos a ela em vez de
compactar. Em produção, com um `make transform` diário, isso significa 1+
arquivo pequeno por partição por dia, indefinidamente.

Na **silver**, o problema existe mas é bem menor: o `MERGE INTO` reescreve
(copy-on-write, formato v2 mas sem delete files habilitados aqui) só os
arquivos das partições efetivamente tocadas — ainda gera arquivo novo a
cada execução com mudança real, mas não acumula na mesma proporção que o
append puro do bronze.

**Como eu resolveria** (não implementado neste desafio — ver pergunta 6):

- `CALL lakehouse.system.rewrite_data_files(table => 'bronze.events')`
  rodando periodicamente (ex.: semanal, via uma task extra na DAG do
  Airflow) para compactar a bronze — é a que mais acumula.
- `CALL lakehouse.system.expire_snapshots(table => '...', older_than => ...)`
  depois de compactar, para não manter indefinidamente os snapshots
  antigos (cada rewrite cria um snapshot novo; sem expirar, o metadado
  cresce mesmo depois da compactação).
- `TBLPROPERTIES ('write.target-file-size-bytes' = '134217728')` (128MB,
  o guideline usual do Iceberg) nas tabelas append-only — não configurei
  isso em nenhuma das tabelas deste projeto; para este volume (dataset
  pequeno de propósito) os arquivos já nascem bem abaixo desse alvo, então
  não fez diferença prática, mas seria a primeira mudança de configuração
  antes de pensar em volume maior.

## 6. O que você faria diferente com mais uma semana?

Em ordem de prioridade real (não por categoria do enunciado):

1. **~~Testes automatizados~~ feito.** `tests/test_ingestion.py` (9
   testes, mocka `requests`/retry/backoff) e `tests/test_transforms.py`
   (5 testes, `SparkSession` local com catálogo Iceberg `hadoop` — sem
   depender de MinIO/iceberg-rest — chamando `create_silver_events`/
   `create_silver_customers` de verdade) cobrem dedup, os 3 comportamentos
   de SCD2 e o critério de idempotência. O que eu ainda faria: cobertura
   de `bronze.py`/`gold.py`/`quality/checks.py` (só silver tem teste hoje)
   e os testes de `ingestion/postgres_client.py` (só `api_client.py` e
   `watermark.py` têm).
2. **GitHub Actions** com lint (`ruff`/`black`) + os testes acima —
   ainda não implementado (os testes existem, rodar em CI automaticamente
   a cada push não).
3. **Manutenção do Iceberg agendada** (`rewrite_data_files`,
   `expire_snapshots`) — ver pergunta 5. Sem isso, o problema de small
   files na bronze só piora com o tempo.
4. **Watermark e controle de execução da silver em tabela, não em
   arquivo local** — resolve a limitação de escala (pergunta 4) e a
   limitação de correção do SCD2 (pergunta 3: uma tabela de controle
   "processei bronze até `_ingested_at` X" permite processar incrementos
   em vez de "estado mais recente global", capturando transições
   intermediárias que hoje poderiam ser perdidas num cenário fora da
   sequência de avaliação do desafio).
5. **dbt** para silver→gold — o SQL das transformações de agregação
   (gold) é simples o bastante para se beneficiar de testes de schema
   (`not_null`, `unique`, `relationships`) e documentação automática do
   dbt, em vez de PySpark hand-rolled.
6. **Alertas de verdade nos quality checks.** `quality/checks.py` já tem
   o formato do payload do Slack pronto
   (`build_slack_alert_payload`) e a integração documentada (Slack,
   PagerDuty, `on_failure_callback` do Airflow) — só não tem uma URL de
   webhook real para chamar.
7. **Monitoramento de data drift** (comparação de distribuições entre
   execuções — ex.: proporção de `event_type` por dia, não só o volume
   total que `CHECK_VOLUMETRY` já cobre) — pegaria uma classe de problema
   diferente da que os 5 checks atuais cobrem (mudança de *mix*, não só
   de volume).
8. **Documentar as tabelas como data contracts** (schema esperado,
   dono, SLA de frescor, o que consome cada uma) — hoje essa informação
   está espalhada em comentários de código; um contrato formal facilitaria
   detectar automaticamente o tipo de desalinhamento que
   `CHECK_DOMAIN_VALUES` encontrou manualmente neste desafio (ver README,
   "Estado atual dos quality checks").
9. **Rodar a DAG do Airflow de ponta a ponta**, não só validar o
   parsing — exigiria ou empacotar uma imagem Airflow com Spark completo,
   ou trocar `PythonOperator`+`subprocess` por `SparkSubmitOperator`
   apontando para um cluster Spark real com conectividade de rede a
   partir do worker do Airflow (hoje o Spark só é alcançável via
   `docker compose exec`, que não é um padrão de submissão remota).
