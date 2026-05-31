## Описание
Проект реализует ETL-пайплайн для обработки транзакционной активности пользователей финтеха и построения аналитической витрины. Система позволяет объединять данные из децентрализованных сервисов разных стран, конвертировать валюты и анализировать динамику оборота компании в единой валюте.

### Бизнес-задачи
- **Анализ транзакционной активности**: ежедневная динамика сумм переводов в разных валютах;
- **Профилирование пользователей**: среднее количество транзакций на пользователя и количество уникальных аккаунтов;
- **Консолидация оборота**: расчёт общего оборота компании в единой валюте (USD) с учётом актуальных курсов;
- **Инкрементальное обновление**: ежедневное пополнение витрины данными за вчерашний день.

## Архитектура решения
Проект реализует полный цикл ETL-процесса:
1. **Extract** — чтение сообщений из **Kafka** (транзакции и курсы валют) в режиме реального времени через Spark Streaming;
2. **Transform** — десериализация JSON, фильтрация по дате, разделение потока на две таблицы (transactions и currencies);
3. **Load** — загрузка сырых данных в Staging-слой Vertica через JDBC-драйвер.

## Структура проекта
```
.
├── dags/
│   ├── DAG_Data_Mart.py                    # DAG: запуск расчёта витрины global_metrics
│   └── DAG_ETL_Vertica.py                  # DAG: запуск Spark-задания для загрузки сырых данных в Staging
├── py/
│   ├── ETL_Vertica.py                      # Spark-скрипт: чтение из Kafka, обработка и запись в Vertica
│   └── connection.py                       # Конфигурация подключений к Kafka и Vertica (из переменных окружения)
└── sql/
    ├── DDL_Vertica.sql                     # DDL: создание схем, таблиц и проекций в Vertica
    └── DML_Vertica.sql                     # DML: расчёт и наполнение витрины global_metrics
```

## Источник данных

### Kafka — Поток транзакций и курсов валют
В Kafka поступают сообщения двух типов в формате JSON:

#### Транзакции (`object_type = TRANSACTION`)
| Поле | Тип | Описание |
|------|-----|----------|
| `operation_id` | `VARCHAR(36)` | UUID транзакции |
| `account_number_from` | `INTEGER` | Внутренний бухгалтерский номер счёта ОТ КОГО |
| `account_number_to` | `INTEGER` | Внутренний бухгалтерский номер счёта К КОМУ |
| `currency_code` | `INTEGER` | Трёхзначный код валюты страны-источника |
| `country` | `VARCHAR(50)` | Страна-источник транзакции |
| `status` | `VARCHAR(20)` | Статус: `queued`, `in_progress`, `blocked`, `done`, `chargeback` |
| `transaction_type` | `VARCHAR(30)` | Тип: `authorisation`, `sbp_incoming`, `sbp_outgoing`, `transfer_incoming`, `transfer_outgoing`, `c2b_partner_incoming`, `c2b_partner_outgoing` |
| `amount` | `INTEGER` | Сумма в минимальной единице валюты (копейка, цент, куруш) |
| `transaction_dt` | `TIMESTAMP` | Дата и время исполнения транзакции |

#### Курсы валют (`object_type = CURRENCY`)
| Поле | Тип | Описание |
|------|-----|----------|
| `date_update` | `TIMESTAMP` | Дата обновления курса |
| `currency_code` | `INTEGER` | Трёхзначный код валюты транзакции |
| `currency_code_with` | `INTEGER` | Код валюты, к которой строится отношение |
| `currency_with_div` | `NUMERIC(10,4)` | Значение отношения единицы одной валюты к единице валюты транзакции |

## Слои данных Vertica

### Staging-слой
Слой для хранения сырых данных из Kafka.

#### Таблица `transactions`
| Поле | Тип | Описание |
|------|-----|----------|
| `operation_id` | `VARCHAR(36)` | UUID транзакции (PK) |
| `account_number_from` | `INTEGER` | Счёт отправителя |
| `account_number_to` | `INTEGER` | Счёт получателя |
| `currency_code` | `INTEGER` | Код валюты |
| `country` | `VARCHAR(50)` | Страна |
| `status` | `VARCHAR(20)` | Статус транзакции |
| `transaction_type` | `VARCHAR(30)` | Тип транзакции |
| `amount` | `INTEGER` | Сумма |
| `transaction_dt` | `TIMESTAMP` | Дата и время транзакции |

**Оптимизация Vertica:**
- Партиционирование по `transaction_dt`;
- Сегментация по хеш-функции `HASH(transaction_dt, operation_id)` на все ноды;
- Проекция `transactions_projection` для оптимизации запросов.

#### Таблица `currencies`
| Поле | Тип | Описание |
|------|-----|----------|
| `date_update` | `TIMESTAMP` | Дата обновления курса |
| `currency_code` | `INTEGER` | Код валюты |
| `currency_code_with` | `INTEGER` | Код валюты отношения |
| `currency_with_div` | `NUMERIC(10,4)` | Коэффициент конвертации |

**Оптимизация Vertica:**
- Партиционирование по `date_update`;
- Сегментация по хеш-функции `HASH(date_update)` на все ноды;
- Проекция `currencies_projection` для оптимизации запросов.

### DataMart-слой

#### Витрина `global_metrics`
Витрина с ежедневной агрегацией по валютам для анализа динамики оборота компании.
| Поле | Тип | Описание |
|------|-----|----------|
| `date_update` | `DATE` | Дата расчёта |
| `currency_from` | `INTEGER` | Код валюты транзакции |
| `amount_total` | `NUMERIC(14,2)` | Общая сумма транзакций по валюте в долларах (USD) |
| `cnt_transactions` | `INTEGER` | Общий объём транзакций по валюте |
| `avg_transactions_per_account` | `NUMERIC(14,2)` | Средний объём транзакций с одного аккаунта |
| `cnt_accounts_make_transactions` | `INTEGER` | Количество уникальных аккаунтов с совершёнными транзакциями |

**Бизнес-логика:**
- `amount_total` — сумма всех транзакций, сконвертированная в USD по актуальному курсу на дату транзакции;
- `avg_transactions_per_account` = `cnt_transactions` / `cnt_accounts_make_transactions`;
- Учитываются только транзакции с положительными счетами отправителя (`account_number_from > 0`) — исключение тестовых аккаунтов;
- Инкрементальное обновление: каждый день добавляется новая партиция за вчерашний день.

**Оптимизация Vertica:**
- Сортировка по `date_update`;
- Сегментация по хеш-функции `HASH(date_update, currency_from)` на все ноды;
- Проекция `global_metrics_projection` для оптимизации запросов.

## Поток данных (DAG)

### DAG 1: `DAG_Vertica` (`dags/DAG_ETL_Vertica.py`)
```
task_vertica (BashOperator)
    └── spark-submit ETL_Vertica.py {{ ds }}
```

**Описание:**
- Запускает Spark-задание через `spark-submit`;
- Подключает пакет `spark-sql-kafka-0-10_2.12:3.3.0` для интеграции с Kafka;
- Использует JDBC-драйвер `vertica-jdbc-25.3.0-0.jar` для записи в Vertica;
- Передаёт макропеременную `{{ ds }}` (execution date) для фильтрации данных за конкретную дату.

### DAG 2: `DML_Vertica` (`dags/DAG_Data_Mart.py`)
```
wait_for_task (ExternalTaskSensor)
    └── dml_vertica (VerticaOperator)
```

**Описание:**
- Ожидает завершения задачи `task_vertica` из DAG `DAG_Vertica` через `ExternalTaskSensor`;
- Режим `reschedule` с интервалом проверки 60 секунд и таймаутом 1800 секунд;
- Выполняет SQL-скрипт `DML_Vertica.sql` через `VerticaOperator` для расчёта и наполнения витрины.

## Пример входных данных

### Сообщение типа TRANSACTION
```json
{
    "object_id": "cc24c4fd-eabd-4e10-b901-24a951839668",
    "object_type": "TRANSACTION",
    "sent_dttm": "2022-10-01T00:12:19",
    "payload": {
        "operation_id": "cc24c4fd-eabd-4e10-b901-24a951839668",
        "account_number_from": 903810,
        "account_number_to": 2080336,
        "currency_code": 430,
        "country": "russia",
        "status": "queued",
        "transaction_type": "sbp_incoming",
        "amount": 84500,
        "transaction_dt": "2022-10-01 00:12:19"
    }
}
```

### Сообщение типа CURRENCY
```json
{
    "object_id": "24563f4d-d69b-5610-a5d2-107066419f2e",
    "object_type": "CURRENCY",
    "sent_dttm": "2022-10-24T00:00:00",
    "payload": {
        "date_update": "2022-10-24 00:00:00",
        "currency_code": 460,
        "currency_code_with": 430,
        "currency_with_div": 0.97
    }
}
```