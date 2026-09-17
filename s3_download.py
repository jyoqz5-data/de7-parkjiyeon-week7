import argparse
import csv
import os
from pathlib import Path

import boto3


S3_PREFIX = "bronze/"
S3_KEY = "bronze/netflix_titles.csv"
OUTPUT_PATH = (
    Path(__file__).resolve().parent
    / "data"
    / "netflix_titles.csv"
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="List, download, and count records in an S3 CSV file."
    )
    parser.add_argument(
        "--bucket",
        default=os.environ.get("Q8_BUCKET"),
        help="S3 bucket name. It can also be supplied with Q8_BUCKET.",
    )

    args = parser.parse_args()

    if not args.bucket:
        parser.error(
            "Provide the bucket with --bucket or the Q8_BUCKET environment variable."
        )

    return args


def list_objects(s3_client, bucket):
    paginator = s3_client.get_paginator("list_objects_v2")
    objects = []

    for page in paginator.paginate(
        Bucket=bucket,
        Prefix=S3_PREFIX,
    ):
        objects.extend(page.get("Contents", []))

    return sorted(objects, key=lambda item: item["Key"])


def count_csv_records(csv_path):
    with csv_path.open(
        mode="r",
        encoding="utf-8-sig",
        newline="",
    ) as csv_file:
        reader = csv.reader(csv_file)
        header = next(reader, None)

        if header is None:
            raise ValueError("The downloaded CSV file is empty.")

        return sum(1 for _ in reader)


def main():
    args = parse_args()
    s3_client = boto3.client("s3")

    print(f"[1] LIST s3://{args.bucket}/{S3_PREFIX}")

    objects = list_objects(s3_client, args.bucket)

    if not objects:
        raise FileNotFoundError(
            f"No objects found under s3://{args.bucket}/{S3_PREFIX}"
        )

    for item in objects:
        print(f"{item['Key']}\t{item['Size']:,} bytes")

    source_object = next(
        (item for item in objects if item["Key"] == S3_KEY),
        None,
    )

    if source_object is None:
        raise FileNotFoundError(
            f"s3://{args.bucket}/{S3_KEY} was not found."
        )

    print()
    print(
        f"[2] DOWNLOAD s3://{args.bucket}/{S3_KEY} "
        f"-> data/netflix_titles.csv"
    )

    OUTPUT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    s3_client.download_file(
        args.bucket,
        S3_KEY,
        str(OUTPUT_PATH),
    )

    downloaded_size = OUTPUT_PATH.stat().st_size
    expected_size = source_object["Size"]

    if downloaded_size != expected_size:
        raise ValueError(
            f"File size mismatch: S3={expected_size}, "
            f"downloaded={downloaded_size}"
        )

    print(f"downloaded ({downloaded_size:,} bytes)")

    print()
    print("[3] CSV RECORD COUNT")

    record_count = count_csv_records(OUTPUT_PATH)
    print(f"records excluding header: {record_count:,}")


if __name__ == "__main__":
    main()