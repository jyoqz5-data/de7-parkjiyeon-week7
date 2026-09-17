import logging
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.sdk import DAG, get_current_context, task


logger = logging.getLogger(__name__)

DAG_ID = "weekly_pipeline_parkjiyeon"
AWS_CONN_ID = "aws_q9"

SOURCE_KEY = "bronze/netflix_titles.csv"
INPUT_PATH = "/tmp/q9_pipeline/input/netflix_titles.csv"
OUTPUT_PATH = "/tmp/q9_pipeline/output"
TRANSFORM_SCRIPT = "/opt/airflow/dags/jobs/transform.py"


def get_s3_client():
    hook = AwsBaseHook(
        aws_conn_id=AWS_CONN_ID,
        client_type="s3",
    )
    return hook.get_conn()


def get_run_settings():
    context = get_current_context()
    dag_run = context.get("dag_run")
    conf = dict(dag_run.conf or {}) if dag_run else {}

    bucket_name = conf.get("bucket_name") or os.getenv("Q9_S3_BUCKET")

    if not bucket_name:
        raise ValueError(
            "bucket_name is required. "
            'Trigger with {"bucket_name": "버킷이름", "min_year": 2015}.'
        )

    min_year = int(conf.get("min_year", 2015))

    return bucket_name, min_year


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
    schedule="@weekly",
    catchup=False,
    tags=["q9", "s3", "spark"],
) as dag:

    @task(task_id="download_from_s3")
    def download_from_s3():
        bucket_name, min_year = get_run_settings()

        input_file = Path(INPUT_PATH)
        input_file.parent.mkdir(parents=True, exist_ok=True)

        s3_client = get_s3_client()

        print(f"[1] DOWNLOAD s3://{bucket_name}/{SOURCE_KEY}")
        print(f"LOCAL_INPUT={INPUT_PATH}")

        s3_client.download_file(
            bucket_name,
            SOURCE_KEY,
            INPUT_PATH,
        )

        file_size = input_file.stat().st_size

        print(f"DOWNLOADED_BYTES={file_size}")

        return {
            "bucket_name": bucket_name,
            "min_year": min_year,
            "input_path": INPUT_PATH,
        }

    @task(task_id="aggregate_with_spark")
    def aggregate_with_spark(download_result):
        output_directory = Path(OUTPUT_PATH)

        if output_directory.exists():
            shutil.rmtree(output_directory)

        spark_submit = (
            shutil.which("spark-submit")
            or "/home/airflow/.local/bin/spark-submit"
        )

        command = [
            spark_submit,
            "--master",
            "local[*]",
            "--conf",
            "spark.sql.parquet.compression.codec=snappy",
            TRANSFORM_SCRIPT,
            "--input",
            download_result["input_path"],
            "--output",
            OUTPUT_PATH,
            "--min-year",
            str(download_result["min_year"]),
        ]

        print("[2] SPARK SUBMIT")
        print(" ".join(command))

        subprocess.run(command, check=True)

        parquet_files = sorted(
            output_directory.glob("*.parquet")
        )

        if not parquet_files:
            raise RuntimeError(
                f"No parquet output found in {OUTPUT_PATH}"
            )

        if not any(
            file.name.endswith(".snappy.parquet")
            for file in parquet_files
        ):
            raise RuntimeError(
                "Spark output is not a snappy parquet file."
            )

        for parquet_file in parquet_files:
            print(f"PARQUET_FILE={parquet_file.name}")

        return {
            "bucket_name": download_result["bucket_name"],
            "output_path": OUTPUT_PATH,
        }

    @task(task_id="upload_to_s3")
    def upload_to_s3(transform_result):
        bucket_name = transform_result["bucket_name"]
        output_directory = Path(transform_result["output_path"])

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        target_prefix = f"silver/{today}/"

        parquet_files = sorted(
            output_directory.glob("*.parquet")
        )

        if not parquet_files:
            raise RuntimeError("No parquet file available for upload.")

        s3_client = get_s3_client()

        for index, parquet_file in enumerate(parquet_files):
            if len(parquet_files) == 1:
                object_name = "result.snappy.parquet"
            else:
                object_name = f"result-{index:05d}.snappy.parquet"

            object_key = f"{target_prefix}{object_name}"

            s3_client.upload_file(
                str(parquet_file),
                bucket_name,
                object_key,
            )

            print(
                f"UPLOADED s3://{bucket_name}/{object_key}"
            )

        success_file = output_directory / "_SUCCESS"

        if success_file.exists():
            success_key = f"{target_prefix}_SUCCESS"

            s3_client.upload_file(
                str(success_file),
                bucket_name,
                success_key,
            )

            print(
                f"UPLOADED s3://{bucket_name}/{success_key}"
            )

        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=target_prefix,
        )

        objects = response.get("Contents", [])

        print("[3] S3 SILVER OBJECT LIST")

        for item in objects:
            print(f'{item["Key"]} {item["Size"]:,} bytes')

        print(f"S3_OUTPUT_PREFIX=s3://{bucket_name}/{target_prefix}")
        print(f"S3_OUTPUT_OBJECT_COUNT={len(objects)}")

        if not any(
            item["Key"].endswith(".snappy.parquet")
            for item in objects
        ):
            raise RuntimeError(
                "No snappy parquet object found in S3."
            )

    downloaded = download_from_s3()
    transformed = aggregate_with_spark(downloaded)
    upload_to_s3(transformed)
