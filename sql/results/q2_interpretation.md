# Q2 — Tempo médio até a primeira resposta, por plano e por mês

Resultado real (`q2_result.csv`), gerado com `SQL/q2_avg_response_time.sql`
sobre `lakehouse.gold.ticket_events`, com o plano do cliente calculado na
**data de abertura do ticket** (join SCD2 contra `silver.customers`, não o
plano atual).

**Enterprise responde muito mais rápido que os outros planos.** O tempo
médio até a primeira resposta para `enterprise` fica em ~0,5h (30 min) em
todos os 4 meses observados (mai–ago/2026), contra ~1,9–2,0h para `pro` e
~5,2–5,9h para `free` — uma diferença de mais de 10x entre `enterprise` e
`free`, consistente o bastante mês a mês para não ser ruído, e compatível
com uma fila de atendimento priorizada por plano (o que já era sugerido
pelo campo `sla_minutes` que aparece no `properties` dos tickets a partir
do schema drift).

**Mas a velocidade de resposta não significa uma taxa de resposta melhor.**
O percentual de tickets sem resposta (`pct_no_response`) não segue a mesma
hierarquia: em agosto/2026, `enterprise` (15,0%) e `pro` (16,5%) têm taxas
de ausência de resposta parecidas — às vezes até piores que `free` (13,5%).
Ou seja: quando o cliente enterprise recebe resposta, ela vem muito mais
rápido; mas a *probabilidade* de um ticket ficar sem nenhuma resposta não
está claramente melhor para o plano premium. Isso separa dois problemas de
negócio diferentes — velocidade de atendimento (bem resolvida para
enterprise) e cobertura de atendimento (não necessariamente melhor) — que
vale a pena tratar como métricas distintas em vez de uma única "qualidade
de suporte".

O grupo `desconhecido` (tickets cujo `customer_id` é nulo ou não tem
cadastro em `crm.customers`) tem volume muito baixo (2 a 8 tickets/mês) e
números instáveis mês a mês (7,7h → 4,2h → 5,6h → 3,0h) — amostra pequena
demais para tirar conclusão, incluída só para não esconder esses tickets
do total.
