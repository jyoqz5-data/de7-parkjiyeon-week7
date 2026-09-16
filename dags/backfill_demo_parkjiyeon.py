import logging
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import DAG, get_current_context, task


logger = logging.getLogger(__name__)

OUTPUT_ROOT = Path("/tmp/q7_backfill_outputs")


@task
def write_daily_output():
    context = get_current_context()
    logical_date = context["logical_date"]

    if logical_date is None:
        raise ValueError("logical_date is required for this scheduled run")

    date_value = logical_date.strftime("%Y-%m-%d")
    output_directory = OUTPUT_ROOT / date_value
    output_directory.mkdir(parents=True, exist_ok=True)

    output_file = output_directory / f"result_{date_value}.txt"
    output_file.write_text(
        f"logical_date={logical_date.isoformat()}\n",
        encoding="utf-8",
    )

    logger.info("LOGICAL DATE: %s", logical_date.isoformat())
    logger.info("OUTPUT FILE: %s", output_file)

    return str(output_file)


with DAG(
    dag_id="backfill_demo_parkjiyeon",
    start_date=datetime(2026, 9, 9, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=True,
    tags=["q7", "backfill"],
) as dag:
    write_daily_output()