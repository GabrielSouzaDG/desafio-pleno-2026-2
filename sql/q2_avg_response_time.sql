-- Tempo médio até a primeira resposta (ticket_opened -> primeiro
-- ticket_replied do mesmo ticket_id), por plano e por mês. Trata tickets
-- sem resposta explicitamente.
--
-- "por plano": usa o plano do cliente NA DATA em que o ticket foi aberto
-- (silver.customers é SCD tipo 2 -- ver transform/silver.py), nao o plano
-- atual dele. Isso evita atribuir o tempo de resposta de um ticket antigo
-- ao plano que o cliente só veio a ter depois (upgrade/downgrade).
--
-- Tickets sem resposta: entram em total_tickets (COUNT(*)), mas
-- hours_to_first_response fica NULL pra eles -- COUNT(coluna) e AVG(coluna)
-- ignoram NULL automaticamente (SQL padrao), entao a media sai correta
-- (nao é puxada pra baixo por um "0 horas" falso) e pct_no_response reporta
-- o percentual explicitamente.
--
-- Fonte: lakehouse.gold.ticket_events (só ticket_opened/ticket_replied/
-- ticket_closed, com ticket_id/agent_id já extraídos de properties).

WITH tickets AS (
    SELECT
        ticket_id,
        customer_id,
        MIN(occurred_at) FILTER (WHERE event_type = 'ticket_opened') AS opened_at
    FROM lakehouse.gold.ticket_events
    WHERE ticket_id IS NOT NULL
    GROUP BY ticket_id, customer_id
),
first_reply AS (
    SELECT
        t.ticket_id,
        MIN(r.occurred_at) AS first_reply_at
    FROM tickets t
    JOIN lakehouse.gold.ticket_events r
      ON r.ticket_id = t.ticket_id
     AND r.event_type = 'ticket_replied'
     AND r.occurred_at > t.opened_at
    WHERE t.opened_at IS NOT NULL
    GROUP BY t.ticket_id
),
ticket_resolution AS (
    SELECT
        t.ticket_id,
        t.customer_id,
        t.opened_at,
        fr.first_reply_at,
        CASE
            WHEN fr.first_reply_at IS NOT NULL
            THEN date_diff('minute', t.opened_at, fr.first_reply_at) / 60.0
        END AS hours_to_first_response
    FROM tickets t
    LEFT JOIN first_reply fr ON fr.ticket_id = t.ticket_id
    WHERE t.opened_at IS NOT NULL
),
enriched AS (
    SELECT
        tr.*,
        date_trunc('month', tr.opened_at) AS response_month,
        COALESCE(c.plan, 'desconhecido') AS plan
    FROM ticket_resolution tr
    LEFT JOIN lakehouse.silver.customers c
      ON c.customer_id = tr.customer_id
     AND c.valid_from <= tr.opened_at
     AND (c.valid_to > tr.opened_at OR c.valid_to IS NULL)
)
SELECT
    plan,
    response_month,
    COUNT(*) AS total_tickets,
    COUNT(hours_to_first_response) AS tickets_with_response,
    ROUND(AVG(hours_to_first_response), 2) AS avg_hours_to_first_response,
    ROUND(100.0 * (COUNT(*) - COUNT(hours_to_first_response)) / COUNT(*), 1) AS pct_no_response
FROM enriched
GROUP BY plan, response_month
ORDER BY response_month, plan;
