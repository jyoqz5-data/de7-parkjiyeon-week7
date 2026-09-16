from datetime import datetime, timezone

from airflow.sdk import dag, task


@dag(
    dag_id="sample_dag",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    tags=["q3"],
)
def sample_dag():

    @task
    def start():
        return "start task completed"

    @task
    def parallel_task_a():
        return "parallel task A completed"

    @task
    def parallel_task_b():
        return "parallel task B completed"

    @task
    def finish():
        return "finish task completed"

    start_task = start()
    task_a = parallel_task_a()
    task_b = parallel_task_b()
    finish_task = finish()

    start_task >> [task_a, task_b] >> finish_task


sample_dag()