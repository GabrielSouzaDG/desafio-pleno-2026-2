-- Top 10 clientes por volume de eventos nos últimos 30 dias da base,
-- com nome da empresa, plano e segmento.
--
-- "últimos 30 dias da base" = últimos 30 dias relativos à data mais
-- recente presente nos DADOS (nao à data real de hoje: o dataset é
-- sintético e fixo no tempo) -- por isso o MAX(event_date) via CROSS JOIN
-- em vez de current_date.
--
-- Eventos com customer_id nulo sao excluidos do ranking: nao existe
-- "cliente" pra ranquear ali. Ja um customer_id orfao (existe nos eventos
-- mas nao em crm.customers) É mantido no ranking -- ele é um cliente de
-- verdade, só falta cadastro -- e aparece com company_name/plan/segment
-- em branco em vez de quebrar a query ou sumir do resultado.
--
-- Fonte: lakehouse.gold.events_daily_summary (pré-agregado por dia,
-- evita re-escanear/re-contar todo lakehouse.silver.events aqui).

WITH bounds AS (
    SELECT MAX(event_date) AS max_date
    FROM lakehouse.gold.events_daily_summary
),
last_30_days AS (
    SELECT
        d.customer_id,
        SUM(d.event_count) AS event_count
    FROM lakehouse.gold.events_daily_summary d
    CROSS JOIN bounds b
    WHERE d.customer_id IS NOT NULL
      AND d.event_date > date_add('day', -30, b.max_date)
    GROUP BY d.customer_id
)
SELECT
    l.customer_id,
    COALESCE(c.company_name, 'desconhecido') AS company_name,
    c.plan,
    c.segment,
    l.event_count
FROM last_30_days l
LEFT JOIN lakehouse.silver.customers c
  ON c.customer_id = l.customer_id
 AND c.is_current = true
ORDER BY l.event_count DESC
LIMIT 10;
