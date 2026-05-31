/* Заполняем витрину данными. */
INSERT INTO VT25110761DD38__DWH.global_metrics (
    date_update,
    currency_from,
    amount_total,
    cnt_transactions,
    avg_transactions_per_account,
    cnt_accounts_make_transactions
)

/* Получаем последний курс валют на дату {{ ds }}. */
WITH latest_currency_rate AS (
    SELECT
        currency_code,
        currency_code_with,
        currency_with_div,
        date_update
    FROM
        VT25110761DD38__STAGING.currencies
    WHERE
    	date_update = '{{ ds }}'
    ),

/* Добавляем валютные курсы к транзакциям за дату {{ ds }}. */
transactions_with_rates AS (
    SELECT
        t.operation_id,
        t.account_number_from,
        t.account_number_to,
        t.currency_code,
        t.country,
        t.status,
        t.transaction_type,
        t.amount,
        DATE(t.transaction_dt) AS transaction_dt,
        cr.currency_code_with,
        cr.currency_with_div
    FROM
        VT25110761DD38__STAGING.transactions AS t
    INNER JOIN latest_currency_rate AS cr
    	ON t.currency_code = cr.currency_code
        AND DATE(t.transaction_dt) = cr.date_update
    WHERE
        DATE(t.transaction_dt) = '{{ ds }}'
        AND t.account_number_from > 0
	),

/* Агрегируем по дню и валюте. */
aggregated_metrics AS (
    SELECT
        tr.transaction_dt AS date_update,
        tr.currency_code AS currency_from,
        -- Перевод суммы в доллары.
        SUM(
            CASE
                WHEN tr.currency_code_with = 840 THEN
                    tr.amount / NULLIF(tr.currency_with_div, 0)
                ELSE
                    tr.amount / NULLIF(tr.currency_with_div, 0) * tr.currency_code_with
            END
        ) AS amount_total,
        COUNT(*) AS cnt_transactions,
        AVG(tr.amount) AS avg_transactions_per_account,
        COUNT(DISTINCT CASE WHEN tr.account_number_from IS NOT NULL THEN tr.account_number_from END) AS cnt_accounts_make_transactions
    FROM
    	transactions_with_rates tr
    GROUP BY
        tr.transaction_dt,
        tr.currency_code
)

SELECT
    date_update,
    currency_from,
    amount_total,
    cnt_transactions,
    avg_transactions_per_account,
    cnt_accounts_make_transactions
FROM
    aggregated_metrics;
