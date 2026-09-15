# ARCHITECTURE.md

## 1. Por que particionou as tabelas desse jeito? Qual seria o impacto de particionar diferente?

| Tabela | Partição | Por quê |
|---|---|---|
| `bronze.events` / `bronze.customers` | `days(occurred_at)` / `days(updated_at)` | Rastreabilidade 1:1 com a raw zone (`ingestion_date`) e com o eixo do watermark; permite reprocessar um dia sem tocar nos outros. |
| `silver.events` | `months(occurred_at)` | As 3 queries de negócio filtram por mês ou janela de 30 dias, nunca por dia isolado. |
| `silver.customers` | `months(valid_from)` | SCD2 — agrupa por "quando a mudança aconteceu", o eixo das perguntas de negócio (retenção por coorte, plano na data do evento). |
| `gold.*` | `months(...)` | Mesma granularidade de consumo das 3 queries. |

**Impacto de particionar diferente:** diário na silver faria cada `MERGE INTO` (que toca vários dias por execução, já que a silver relê a bronze inteira — ver pergunta 4) reescrever dezenas de partições pequenas, sem ganho real de pruning (as queries não filtram por dia) — o cenário de *small files* da pergunta 5. Sem partição (ou anual), o Iceberg deixaria de identificar candidatos ao MERGE por overlap de partição, virando um scan/rewrite de fração enorme da tabela a cada correção pontual. Com o volume atual (18.658 eventos) isso não aparece como problema; com 100× (pergunta 4), seria o primeiro gargalo visível.

## 2. Como você garantiu idempotência em cada etapa?

- **Raw:** merge deduplicado por linha antes de um único `put_object` atômico — não overwrite, porque a sequência de avaliação roda o pipeline mais de uma vez no mesmo dia (inclusive com uma carga nova no meio).
- **Watermark:** só avança depois do upload confirmado; se o processo cai antes, a próxima execução repete o intervalo com segurança.
- **Bronze:** append-only, aceita duplicata de propósito — quem deduplica é a silver.
- **Silver events:** `MERGE INTO` por `event_id` (um `ROW_NUMBER()` reduz a bronze, que pode ter duplicata, a 1 candidato por chave antes do merge); só atualiza se `updated_at` for genuinamente mais recente.
- **Silver customers:** SCD2 via dois `MERGE INTO` (ver pergunta 3) — idempotente porque a mudança só é detectada comparando contra o estado `is_current=true` já persistido.
- **Gold:** recomputada a partir da silver e reconciliada via `MERGE INTO` pela chave de negócio de cada tabela, com comparação null-safe onde a chave pode ser nula. **Limitação conhecida** (documentada em `transform/gold.py`): não usa `WHEN NOT MATCHED BY SOURCE THEN DELETE` — se uma linha deixasse de existir na silver, a gold correspondente não seria removida. Não implementei por incerteza sobre suporte dessa cláusula na versão do Iceberg empacotada na imagem do Spark; risco prático baixo porque as correções deste dataset mudam campos, não removem linhas.

**Validado rodando de verdade** a sequência do enunciado (`pipeline → de novo → batch2 → pipeline → de novo`) 5 vezes em Windows e, nesta sessão, do zero também em Linux (WSL, clone limpo do GitHub): silver e gold terminam com contagens **idênticas** em cada repetição.

## 3. Como modelou a mudança de plano do cliente ao longo do tempo, e por quê?

**SCD Tipo 2** em `silver.customers`: `valid_from`, `valid_to` (`NULL` = vigente), `is_current`, `_record_version`. Intervalo meio-aberto `[valid_from, valid_to)`, sem gap nem overlap entre versões do mesmo cliente.

Implementado com **dois `MERGE INTO`**, não um: um único `MERGE INTO ... ON customer_id` só pode casar-e-atualizar OU não casar-e-inserir — nunca as duas coisas na mesma cláusula. 1) **fecha** quem tinha `is_current=true` e teve algum atributo mudando; 2) **abre** quem ficou sem `is_current=true` depois do passo 1 (cliente novo ou recém-fechado).

**Alternativas:** SCD1 seria mais simples mas perde o histórico — Q1/Q2 atribuiriam retroativamente o plano atual a eventos passados. Um log de mudanças sem materializar o estado é mais barato de escrever, mas empurra a reconstrução de "estado num instante" para toda leitura — trade-off ruim para um padrão majoritariamente analítico (poucas escritas, muitas leituras via Trino).

**Limitação conhecida:** a silver olha a bronze inteira a cada execução; se rodasse só depois de dois batches já acumulados sem execução no meio, uma transição intermediária seria perdida. Não acontece na sequência de avaliação do desafio, mas é real (ver pergunta 6).

## 4. O que aconteceria com esse pipeline se o volume crescesse 100×? O que quebraria primeiro?

1. **`silver.py` relê a bronze inteira a cada execução** — cresce com o histórico acumulado, não com o incremento do dia. Primeiro gargalo real.
2. **`MERGE INTO` sem alinhamento entre chave de merge e partição** nas tabelas gold (`customer_id` não particiona nenhuma) — mais scan/rewrite do que o necessário.
3. **Watermark em arquivo local** — não coordena múltiplos workers/hosts.
4. **`spark.driver.memory=2g` fixo** — `.collect()`/`.count()` no meio das transformações passam a pressionar o driver antes de qualquer ajuste de shuffle partitions.
5. **Q3 no Trino** (`CROSS JOIN` coortes × meses) deixa de ser interativa sem uma gold pré-agregada de retenção.

**O que eu faria:** silver incremental por watermark próprio (filtrar a bronze antes do `ROW_NUMBER`); watermark/controle de execução numa tabela (Postgres ou Iceberg), não em arquivo; particionar/bucketing das gold pela chave de merge dominante; cluster Spark com mais executores e revisão de `spark.sql.shuffle.partitions`.

## 5. O problema de small files no Iceberg: ele existe na sua solução? Como você resolveria?

**Existe, e é observável neste projeto.** Rodando o pipeline 4× seguidas (a sequência de avaliação), `bronze.events` foi de 15.256 → 30.512 → 49.350 → 68.188 linhas, um conjunto de arquivos novo por execução — bronze é append-only por decisão de design (pergunta 2). Na silver o problema é bem menor: o `MERGE INTO` só reescreve as partições efetivamente tocadas.

**Como eu resolveria** (não implementado — dataset pequeno de propósito não justificou o esforço): `CALL system.rewrite_data_files` periódico (ex.: semanal, via uma task extra na DAG) para compactar a bronze; `CALL system.expire_snapshots` depois de compactar, para não acumular metadado indefinidamente; `TBLPROPERTIES ('write.target-file-size-bytes'='134217728')` (128MB) nas tabelas append-only.

## 6. O que você faria diferente com mais uma semana?

Em ordem de prioridade:

1. **Watermark e controle de execução da silver em tabela**, não em arquivo local — resolve a limitação de escala (pergunta 4) e a de correção do SCD2 (pergunta 3).
2. **Manutenção Iceberg agendada** (`rewrite_data_files`, `expire_snapshots` — pergunta 5).
3. **GitHub Actions** rodando os 14 testes que já existem (`tests/`) + lint.
4. **Alertas reais nos quality checks** — o payload de Slack já está pronto em `quality/checks.py`, só falta uma URL de webhook de verdade.
5. **dbt** para silver→gold — o SQL de agregação da gold se beneficiaria de testes de schema declarativos (`not_null`, `unique`, `relationships`).
6. **Monitoramento de data drift** (mudança de *mix* de `event_type`, não só volume total, que `CHECK_VOLUMETRY` já cobre).

> Nota sobre severidade dos quality checks (o que apenas alerta vs. o que para o pipeline) e os dois ajustes reais encontrados rodando o pipeline de ponta a ponta: ver seção "Estado atual dos quality checks" no [README.md](README.md).
