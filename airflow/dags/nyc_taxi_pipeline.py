from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "rashid",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="nyc_taxi_bigdata_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    description="NYC taxi pipeline with retry demo",
) as dag:

    check_input = BashOperator(
        task_id="check_input_dataset",
        bash_command="ls -lh /opt/airflow/data/raw"
    )

    fail_once = BashOperator(
        task_id="fail_once_then_retry",
        bash_command="""
        FLAG=/tmp/airflow_retry_flag
        if [ ! -f "$FLAG" ]; then
          echo "Intentional first failure"
          touch "$FLAG"
          exit 1
        else
          echo "Retry succeeded"
          exit 0
        fi
        """
    )

    run_spark = BashOperator(
        task_id="run_spark_job",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          /opt/spark/jobs/analyze_taxi.py
        """
    )

    load_clickhouse = BashOperator(
        task_id="load_to_clickhouse",
        bash_command="""
        docker exec clickhouse clickhouse-client --query "TRUNCATE TABLE taxi.trips_by_hour" && \
        cat /opt/airflow/data/processed/trips_by_hour_csv/part-*.csv | \
        docker exec -i clickhouse clickhouse-client \
          --query="INSERT INTO taxi.trips_by_hour FORMAT CSVWithNames"
        """
    )

    validate_clickhouse = BashOperator(
        task_id="validate_clickhouse",
        bash_command="""
        docker exec clickhouse clickhouse-client \
          --query "SELECT count(*) FROM taxi.trips_by_hour"
        """
    )

    check_input >> fail_once >> run_spark >> load_clickhouse >> validate_clickhouse