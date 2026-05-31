/* Создаем STG слой. */
DROP SCHEMA IF EXISTS VT25110761DD38__STAGING;
CREATE SCHEMA IF NOT EXISTS VT25110761DD38__STAGING;

/* Создаем таблицу transactions. */
DROP TABLE IF EXISTS VT25110761DD38__STAGING.transactions;
CREATE TABLE IF NOT EXISTS VT25110761DD38__STAGING.transactions (
	operation_id VARCHAR(60) NULL,
	account_number_from INTEGER NULL,
	account_number_to INTEGER NULL,
	currency_code INTEGER NULL,
	country VARCHAR(30) NULL,
	status VARCHAR(30) NULL,
	transaction_type VARCHAR(30) NULL,
	amount INTEGER NULL,
	transaction_dt TIMESTAMP NULL
)
ORDER BY transaction_dt
SEGMENTED BY HASH(transaction_dt, operation_id) ALL NODES;

/* Создаем проекцию таблицы transactions. */
CREATE PROJECTION VT25110761DD38__STAGING.transactions_projection (
    operation_id,
    account_number_from,
    account_number_to,
    currency_code,
    country,
    status,
    transaction_type,
    amount,
    transaction_dt
)
AS
SELECT
    operation_id,
    account_number_from,
    account_number_to,
    currency_code,
    country,
    status,
    transaction_type,
    amount,
    transaction_dt
FROM
    VT25110761DD38__STAGING.transactions
ORDER BY transaction_dt
SEGMENTED BY HASH(transaction_dt, operation_id) ALL NODES;


/* Создаем таблицу currencies. */
DROP TABLE IF EXISTS VT25110761DD38__STAGING.currencies;
CREATE TABLE IF NOT EXISTS VT25110761DD38__STAGING.currencies (
	date_update DATE NULL,
	currency_code INTEGER NULL,
	currency_code_with INTEGER NULL,
	currency_with_div NUMERIC(5, 3) NULL
)
ORDER BY date_update
SEGMENTED BY HASH(date_update) ALL NODES;

/* Создаем проекцию таблицы currencies. */
CREATE PROJECTION VT25110761DD38__STAGING.currencies_projection (
    date_update,
    currency_code,
    currency_code_with,
    currency_with_div
)
AS
SELECT
    date_update,
    currency_code,
    currency_code_with,
    currency_with_div
FROM
    VT25110761DD38__STAGING.currencies
ORDER BY date_update
SEGMENTED BY HASH(date_update) ALL NODES;


/* Создаем CDM слой. */
DROP SCHEMA IF EXISTS VT25110761DD38__DWH;
CREATE SCHEMA IF NOT EXISTS VT25110761DD38__DWH;

/* Создаем витрину global_metrics. */
DROP TABLE IF EXISTS VT25110761DD38__DWH.global_metrics;
CREATE TABLE IF NOT EXISTS VT25110761DD38__DWH.global_metrics (
	date_update DATE NOT NULL,
	currency_from INTEGER NOT NULL,
	amount_total INTEGER NOT NULL,
	cnt_transactions INTEGER NOT NULL,
	avg_transactions_per_account INTEGER NOT NULL,
	cnt_accounts_make_transactions INTEGER NOT NULL
)
ORDER BY date_update
SEGMENTED BY HASH(date_update, currency_from) ALL NODES;

/* Создаем проекцию витрины global_metrics. */
CREATE PROJECTION VT25110761DD38__DWH.global_metrics_projection (
    date_update,
	currency_from,
	amount_total,
	cnt_transactions,
	avg_transactions_per_account,
	cnt_accounts_make_transactions
)
AS
SELECT
    date_update,
	currency_from,
	amount_total,
	cnt_transactions,
	avg_transactions_per_account,
	cnt_accounts_make_transactions
FROM
    VT25110761DD38__DWH.global_metrics
ORDER BY date_update
SEGMENTED BY HASH(date_update, currency_from) ALL NODES;
