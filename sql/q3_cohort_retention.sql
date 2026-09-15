-- Retenção por coorte: percentual de clientes com pelo menos 1 evento no
-- mês, agrupados pelo mês de signup_date.
--
-- Coorte = mês de signup (silver.customers, is_current = true -- o mes de
-- signup nao muda entre versoes SCD2 de um cliente, entao tanto faz qual
-- versao usar; is_current garante 1 linha por cliente).
--
-- month_spine cobre TODO mês de atividade observado nos dados (não só os
-- meses em que a coorte teve atividade) para que meses com retenção 0%
-- apareçam explicitamente como uma linha com active_customers = 0, em vez
-- de simplesmente não existir no resultado -- uma tabela de retenção sem
-- essas linhas esconderia justamente o momento em que uma coorte "morre".
--
-- Fonte: lakehouse.gold.customer_monthly_activity (1 linha por cliente
-- com atividade naquele mês) + lakehouse.silver.customers (tamanho de
-- cada coorte).

WITH cohorts AS (
    SELECT
        DATE_TRUNC('month', signup_date) AS signup_month,
        COUNT(DISTINCT customer_id) AS cohort_size
    FROM lakehouse.silver.customers
    WHERE is_current = true
    GROUP BY DATE_TRUNC('month', signup_date)
),
month_spine AS (
    SELECT DISTINCT activity_month FROM lakehouse.gold.customer_monthly_activity
    UNION
    SELECT DISTINCT signup_month FROM cohorts
),
cohort_months AS (
    SELECT
        c.signup_month,
        m.activity_month,
        c.cohort_size
    FROM cohorts c
    CROSS JOIN month_spine m
    WHERE m.activity_month >= c.signup_month
),
activity AS (
    SELECT
        signup_month,
        activity_month,
        COUNT(DISTINCT customer_id) AS active_customers
    FROM lakehouse.gold.customer_monthly_activity
    WHERE signup_month IS NOT NULL
    GROUP BY signup_month, activity_month
)
SELECT
    cm.signup_month,
    cm.activity_month,
    cm.cohort_size,
    COALESCE(a.active_customers, 0) AS active_customers,
    ROUND(100.0 * COALESCE(a.active_customers, 0) / cm.cohort_size, 1) AS retention_rate
FROM cohort_months cm
LEFT JOIN activity a
  ON a.signup_month = cm.signup_month
 AND a.activity_month = cm.activity_month
ORDER BY cm.signup_month, cm.activity_month;
