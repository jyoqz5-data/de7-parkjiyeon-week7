from datetime import datetime, timezone

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.empty import EmptyOperator


@task
def parallel_task_a():
    return "parallel task A completed"


@task
def parallel_task_b():
    return "parallel task B completed"


with DAG(
    dag_id="sample_dag",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    tags=["q3"],
) as dag:

    start = EmptyOperator(task_id="start")

    task_a = parallel_task_a()
    task_b = parallel_task_b()

    finish = EmptyOperator(task_id="finish")

    start >> [task_a, task_b] >> finish