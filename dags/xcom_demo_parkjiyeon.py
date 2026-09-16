import logging
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, get_current_context, task


logger = logging.getLogger(__name__)


@task(
    retries=2,
    retry_delay=timedelta(seconds=15),
)
def calculate_value():
    context = get_current_context()
    task_instance = context["ti"]
    try_number = task_instance.try_number

    logger.info("CALCULATE ATTEMPT NUMBER: %s", try_number)

    if try_number == 1:
        logger.warning("INTENTIONAL FAILURE ON FIRST ATTEMPT")
        raise RuntimeError("Intentional failure for Q6 retry demonstration")

    calculated_value = sum(range(1, 11))
    logger.info("CALCULATED VALUE: %s", calculated_value)

    return calculated_value


@task
def pull_xcom_value():
    context = get_current_context()
    task_instance = context["ti"]

    received_value = task_instance.xcom_pull(
        task_ids="calculate_value",
        key="return_value",
    )

    logger.info("XCOM PULLED VALUE: %s", received_value)

    if received_value is None:
        raise ValueError("No XCom value was received")

    return received_value


with DAG(
    dag_id="xcom_demo_parkjiyeon",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["q6", "xcom", "retry"],
) as dag:

    first_task = calculate_value()
    second_task = pull_xcom_value()

    first_task >> second_task