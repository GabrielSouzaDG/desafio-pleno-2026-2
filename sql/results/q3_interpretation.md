# Q3 — Retenção por coorte (mês de signup x mês de atividade)

Resultado real (`q3_result.csv`), gerado com `SQL/q3_cohort_retention.sql`
sobre `lakehouse.gold.customer_monthly_activity` + `lakehouse.silver.customers`.
272 linhas: uma por combinação (coorte de signup, mês de atividade ≥ mês de
signup) — inclusive as com 0% de retenção, que a query gera de propósito
via `month_spine` (ver comentário no `.sql`).

**Leia as linhas com `retention_rate = 0.0` até ~abril/2026 como artefato
de janela de dados, não como churn real.** Os clientes desta base têm
`signup_date` desde set/2024, mas os EVENTOS (a única fonte de "atividade"
aqui) só existem a partir de ~mai/2026 — então toda coorte de signup
anterior a isso mostra 0% de retenção em todo mês anterior a mai/2026,
simplesmente porque não há nenhum evento registrado nesse período para
nenhum cliente, de nenhuma coorte. É a query funcionando corretamente
sobre um dataset cuja janela de eventos é mais curta que a janela de
cadastro de clientes, não um sinal de que essas coortes ficaram inativas.

**Dentro da janela com dado de atividade (mai–ago/2026), a retenção é
alta e relativamente estável.** Coortes antigas e pequenas (ex.: coorte de
set/2024, 6 clientes) saltam para 83–100% assim que a janela de eventos
começa e se mantêm assim — mas o tamanho pequeno dessas coortes (muitas
com 5 a 18 clientes) faz cada percentual pular em degraus grandes (1
cliente a mais ou a menos muda o percentual em vários pontos), então não
dá pra ler esses números como tendência estatisticamente robusta. As
coortes mais recentes e maiores — as que se formaram já dentro da janela
de eventos (mai/2026: 46 clientes, jun/2026: 49, jul/2026: 52, ago/2026:
58) — são a leitura mais confiável do produto: retenção no primeiro mês
entre 77,6% (ago/2026, ainda em andamento — mês parcial na base) e 92,3%
(jul/2026), subindo para perto de 100% no mês seguinte ao signup na
maioria dos casos. Isso sugere um produto com boa ativação inicial: quem
assina, tende a voltar a usar já no mês seguinte.

**O que eu recomendaria antes de usar este número operacionalmente:**
esperar a base acumular uma janela de eventos mais longa que 4 meses antes
de confiar nas coortes antigas, e tratar as coortes com menos de ~20
clientes como indicativas, não conclusivas.
