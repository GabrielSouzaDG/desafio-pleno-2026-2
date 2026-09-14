# Q1 — Top 10 clientes por volume de eventos (últimos 30 dias da base)

Resultado real (`q1_result.csv`), gerado com `SQL/q1_top_customers.sql`
sobre `lakehouse.gold.events_daily_summary` + `lakehouse.silver.customers`,
com os dados já em batch 2 (400 clientes originais + 18 novos, 18.658
eventos deduplicados na silver).

O cliente mais ativo é **Atlas Logística** (`c_0157`, plano `pro`, segmento
`Government`), com 160 eventos nos últimos 30 dias da base — cerca de 10%
acima do segundo colocado (Solar Alimentos, 146). Não há nenhum cliente do
plano `free` entre os 10 primeiros: 8 dos 10 são `pro` e 2 são
`enterprise`, o que é o padrão esperado — clientes pagantes/maiores tendem
a gerar mais eventos de uso e de atendimento do que contas gratuitas. Os
segmentos, por outro lado, estão bem distribuídos (Government, MidMarket,
Enterprise, Education aparecem), então volume de eventos aqui não parece
estar puxado por um segmento específico, e sim mais por plano/porte do
cliente.

Nenhum dos 10 primeiros tem `customer_id` órfão (sem cadastro em
`crm.customers`) nesta execução — o `LEFT JOIN` com `COALESCE(...,
'desconhecido')` da query está lá exatamente para não quebrar/sumir com um
cliente nessa situação, mas neste recorte específico todos os top 10 têm
cadastro completo.
