## Русский
# 🚕 Проект по анализу больших данных: конвейер такси в Нью-Йорке

## Обзор

В этом проекте реализован распределенный конвейер больших данных для обработки и анализа ** Записей о поездках на такси в Нью-Йорке**.

Система объединяет:

* **Apache Airflow** для организации рабочего процесса
* **Apache Spark** для распределенной обработки данных
* **ClickHouse** для аналитического хранилища
* **Superset** в качестве дополнительного слоя панели мониторинга

Окончательный конвейер:

текст ``
Airflow → Spark → ClickHouse → Superset
```

Проект был создан как единая интегрированная система, которая соответствует требованиям курса для одного курсового проекта, когда Airflow и Spark используются совместно. 

---

## Цели проекта

Проект демонстрирует:

* согласованность нескольких задач в Airflow
* сценарий повторной попытки Airflow, при котором сначала завершается неудачей одна задача, а затем выполняется успешно
* распределенное выполнение Spark на 3 процессах/узлах
* хранение результатов анализа в ClickHouse
* отказоустойчивость при остановке одного сотрудника Spark
* дополнительное расширение панели мониторинга с использованием Superset

Эти цели напрямую отражают ожидания преподавателя от курсовой работы по большим данным. 

---

## Архитектура

``текст
Необработанные данные о такси в Нью-Йорке
        ↓
   Apache Spark
(распределенная обработка данных)
        ↓
 Обработанные агрегации
        ↓
    ClickHouse
(аналитическое хранилище)
        ↓
 Apache Airflow
(согласование / повторные попытки / проверка)
        ↓
    Надмножество
(дополнительные панели мониторинга)
```

Практический обзор рабочего процесса:

1. Airflow проверяет входной набор данных
2. Одна задача Airflow намеренно завершается неудачей и успешно выполняется при повторной попытке
3. Spark обрабатывает данные о такси в распределенном кластере
4. Spark выдает агрегированные результаты
5. Результаты загружаются в ClickHouse
6. Superset может быть подключен к ClickHouse для визуализации

---

## Технический стек

* **Рабочий стол Docker**
* **WSL**
* **Apache Airflow**
* **Apache Spark**
* **ClickHouse**
* **Надстройка Apache** * (необязательное расширение)*
* **Git**
* **Visual Studio Code**

Все основные сервисы запускаются в контейнерах Docker.

---

## Dataset.

В этом проекте используется ** реальный набор данных о такси в Нью-Йорке **.

Используемые поля включают:

* дату и время прибытия
* расстояние поездки
* сумму тарифа

Spark агрегирует данные с помощью:

* `trip_date`
* `pickup_hour`

и вычисляет:

* `trip_count`
* `avg_fare`
* `avg_distance`

---

## Кластер Spark

В проекте используется распределенный кластер Spark с:

* **1 мастером Spark**
* **2 рабочими Spark**

Это удовлетворяет требованиям для запуска Spark в распределенном режиме на 3 узлах/процессах. 

### Задание Spark

Основное задание Spark:

``текст
spark/jobs/analyze_taxi.py
```

### Что он делает

* считывает необработанные данные о такси в Нью-Йорке
* извлекает дату и час из временной метки доставки
* суммирует поездки на такси по дате и часу
* вычисляет:

  * количество поездок
  * средний тариф
  * среднее расстояние
* обработанные результаты записываются в "данные/обработано"

---

## Хранилище ClickHouse

Окончательные аналитические результаты хранятся в ClickHouse.

### База данных

``текстовое
такси
```

### Таблица

``sql
taxi.поездки на_часовые
```

### Столбцы таблицы

* `дата поездки`
* `время получения`
* `количество поездок`
* `средняя стоимость проезда"
* `среднее расстояние`

Эта таблица является основным аналитическим результатом проекта и может также использоваться панелями мониторинга надмножеств.

---

## Рабочий процесс Airflow

Главная страница DAG:

``текст
nyc_taxi_bigdata_pipeline
```

### Задачи

* `check_input_dataset`
* `fail_once_then_retry`
* `run_spark_job`
* `load_to_clickhouse`
* `validate_clickhouse`

### Демонстрация повторной попытки

Одна задача Airflow специально разработана таким образом, чтобы:

* при первом запуске выполнить ее не удалось
* автоматически повторить попытку
* При следующей попытке выполнить ее успешно

Это демонстрирует поведение Airflow при повторной попытке, требуемое курсом. 

---

## Демонстрация отказоустойчивости

Проект также демонстрирует отказоустойчивость Spark.

### Эксперимент

* кластер запускается с 2 рабочими элементами
* один рабочий элемент Spark намеренно остановлен
* задание Spark запускается снова
* обработка по-прежнему завершается с сокращением ресурсов
* остановленный рабочий процесс запускается снова

Это демонстрирует частичную устойчивость настройки распределенной обработки, которая была одним из обязательных практических пунктов для курсовой работы. 

---

## Расширение надмножества

Надмножество было добавлено в качестве дополнительного слоя панели мониторинга.

### Назначение

* подключиться к ClickHouse
* считывать данные из `taxi.trips_by_hour`
* создавать информационные панели для аналитической визуализации

### Примеры идей для информационной панели

* поездки по дням
* поездки по часам
* средняя стоимость проезда по часам
* среднее расстояние по часам

Это соответствует предложению учителя создать отдельное приложение для панели мониторинга, такое как Superset или Grafana. 

---

## Как запустить

### 1. Запустите основные службы

``bash
docker compose up -d
```

### 2. Проверьте контейнеры

``bash
docker создает ps
```

### 3. Запустите задание Spark вручную

``bash
docker exec -это spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/analyze_taxi.py
```

### 4. Проверьте обработанные выходные данные

``bash
находит данные/обработанные -максимальная глубина 4 -тип f | сортировка
```

### 5. Проверьте результаты ClickHouse

``bash
docker exec -это clickhouse clickhouse-клиент -запрос "ВЫБЕРИТЕ количество(*) В taxi.trips_by_hour"
```

### 6. Откройте воздушный поток

``текст
http://localhost:8081
```

### 7. Откройте надстройку * (если она включена)*

``текст
http://localhost:8088
```

---

## Основные достижения

* создан единый конвейер обработки больших данных
* использованы реальные данные о такси в Нью-Йорке
* развернут распределенный кластер Spark
* успешно выполнено задание Spark
* агрегированные результаты сохранены в ClickHouse
* реализована синхронизация воздушного потока
* продемонстрирована логика повторных попыток воздушного потока
* продемонстрирован сценарий сбоя Spark worker
* расширен проект дополнительными панелями мониторинга надмножеств

---

## Возможные улучшения в будущем


### 1. Улучшенные панели мониторинга

* добавлены дополнительные диаграммы надмножеств
* добавлены фильтры по дате/часу
* созданы таблицы ключевых показателей эффективности и панели мониторинга

### 2. Расширение ML

Возможная идея:

* считывать данные из ClickHouse
* обучать простую модель на основе агрегированных данных о такси
* записывать результаты прогнозирования или коэффициенты модели обратно в ClickHouse

Примеры идей ML:

* прогнозирование спроса на поездки
* прогнозирование динамики тарифов
* простая линейная регрессия на основе почасового объема поездок

---


## Автор

Курсовой проект по анализу больших данных
Репозиторий подготовлен для академического использования и дальнейшего расширения.

---
```
## English 

# 🚕 Big Data Analysis Project: NYC Taxi Pipeline

## Overview

This project implements a distributed big data pipeline for processing and analyzing **NYC Taxi Trip Records**.

The system combines:

* **Apache Airflow** for workflow orchestration
* **Apache Spark** for distributed data processing
* **ClickHouse** for analytical storage
* **Superset** as an optional dashboard layer

Final pipeline:

```text
Airflow → Spark → ClickHouse → Superset
```

The project was built as one integrated system, which fits the course requirement for a single coursework project when Airflow and Spark are used together. 

---

## Project Goals

The project demonstrates:

* orchestration of multiple tasks in Airflow
* an Airflow retry scenario where one task fails first and later succeeds
* distributed Spark execution on 3 processes/nodes
* analytical result storage in ClickHouse
* fault tolerance when one Spark worker is stopped
* optional dashboard extension using Superset

These goals directly reflect the teacher’s expectations for the Big Data coursework. 

---

## Architecture

```text
Raw NYC Taxi Data
        ↓
   Apache Spark
(distributed processing)
        ↓
 Processed Aggregations
        ↓
    ClickHouse
(analytical storage)
        ↓
 Apache Airflow
(orchestration / retries / validation)
        ↓
    Superset
(optional dashboards)
```

A practical view of the workflow:

1. Airflow checks the input dataset
2. One Airflow task intentionally fails and succeeds on retry
3. Spark processes the taxi data in a distributed cluster
4. Spark outputs aggregated results
5. Results are loaded into ClickHouse
6. Superset can be connected to ClickHouse for visualization

---

## Tech Stack

* **Docker Desktop**
* **WSL**
* **Apache Airflow**
* **Apache Spark**
* **ClickHouse**
* **Apache Superset** *(optional extension)*
* **Git**
* **Visual Studio Code**

All main services run in Docker containers.

---

## Dataset

This project uses the **real NYC Taxi dataset**.

Used fields include:

* pickup datetime
* trip distance
* fare amount

Spark aggregates the data by:

* `trip_date`
* `pickup_hour`

and computes:

* `trip_count`
* `avg_fare`
* `avg_distance`

---

## Spark Cluster

The project uses a distributed Spark cluster with:

* **1 Spark Master**
* **2 Spark Workers**

This satisfies the requirement to run Spark in distributed mode across 3 nodes/processes. 

### Spark job

Main Spark job:

```text
spark/jobs/analyze_taxi.py
```

### What it does

* reads raw NYC Taxi parquet data
* extracts date and hour from pickup timestamp
* aggregates taxi trips by date and hour
* computes:

  * trip count
  * average fare
  * average distance
* writes processed results to `data/processed`

---

## ClickHouse Storage

The final analytical results are stored in ClickHouse.

### Database

```text
taxi
```

### Table

```sql
taxi.trips_by_hour
```

### Table columns

* `trip_date`
* `pickup_hour`
* `trip_count`
* `avg_fare`
* `avg_distance`

This table is the main analytical output of the project and can also be used by Superset dashboards.

---

## Airflow Workflow

Main DAG:

```text
nyc_taxi_bigdata_pipeline
```

### Tasks

* `check_input_dataset`
* `fail_once_then_retry`
* `run_spark_job`
* `load_to_clickhouse`
* `validate_clickhouse`

### Retry demonstration

One Airflow task is intentionally designed to:

* fail on the first run
* retry automatically
* succeed on the next attempt

This demonstrates Airflow retry behavior required by the course. 

---

## Fault Tolerance Demonstration

The project also demonstrates Spark fault tolerance.

### Experiment

* the cluster starts with 2 workers
* one Spark worker is intentionally stopped
* the Spark job is run again
* processing still completes with reduced resources
* the stopped worker is started again

This demonstrates partial resilience of the distributed processing setup, which was one of the required practical points for the coursework. 

---

## Superset Extension

Superset was added as an optional dashboard layer.

### Purpose

* connect to ClickHouse
* read data from `taxi.trips_by_hour`
* build dashboards for analytical visualization

### Example dashboard ideas

* trips by day
* trips by hour
* average fare by hour
* average distance by hour

This matches the teacher’s suggestion to have a separate dashboard application such as Superset or Grafana. 

---

## How to Run

### 1. Start the core services

```bash
docker compose up -d
```

### 2. Verify containers

```bash
docker compose ps
```

### 3. Run the Spark job manually

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/analyze_taxi.py
```

### 4. Check processed output

```bash
find data/processed -maxdepth 4 -type f | sort
```

### 5. Verify ClickHouse results

```bash
docker exec -it clickhouse clickhouse-client --query "SELECT count(*) FROM taxi.trips_by_hour"
```

### 6. Open Airflow

```text
http://localhost:8081
```

### 7. Open Superset *(if enabled)*

```text
http://localhost:8088
```

---

## Main Achievements

* built a unified big data pipeline
* used real NYC Taxi data
* deployed distributed Spark cluster
* executed Spark job successfully
* stored aggregated results in ClickHouse
* implemented Airflow orchestration
* demonstrated Airflow retry logic
* demonstrated Spark worker failure scenario
* extended the project with optional Superset dashboards

---

## Possible Future Improvements


### 1. Better dashboards

* add more Superset charts
* add filters by date/hour
* build KPI cards and dashboards

### 2. ML extension

Possible idea:

* read data from ClickHouse
* train a simple model on aggregated taxi data
* write prediction results or model coefficients back into ClickHouse

Example ML ideas:

* trip demand prediction
* fare trend prediction
* simple linear regression on hourly trip volume

---


## Author

Big Data Analysis coursework project
Repository prepared for academic use and further extension.
