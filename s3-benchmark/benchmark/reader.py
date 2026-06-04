from __future__ import annotations

import time
from typing import Iterator

import boto3
from botocore.config import Config

from canal.flow import map_async
from benchmark.config import S3Config


def _make_client(config: S3Config):
    """Create a boto3 S3 client. Always call this inside the worker process."""
    return boto3.client(
        "s3",
        endpoint_url=config.endpoint_url,
        region_name=config.region,
        aws_access_key_id=config.aws_access_key_id,
        aws_secret_access_key=config.aws_secret_access_key,
        config=Config(s3={"addressing_style": "path"}),
    )


def read_big_file(config: S3Config, key: str) -> dict:
    """Download a single large file and return timing + size info."""
    client = _make_client(config)

    start = time.perf_counter()
    response = client.get_object(Bucket=config.bucket, Key=key)
    data = response["Body"].read()
    elapsed = time.perf_counter() - start

    return {"key": key, "size_bytes": len(data), "elapsed_s": elapsed}


def read_small_files(
    config: S3Config,
    keys: list[str],
    processes: int = 4,
) -> list[dict]:
    """Download many small files in parallel and return per-file timing info."""

    def _download(key: str) -> dict:
        # Client created inside the worker to be fork-safe.
        client = _make_client(config)

        start = time.perf_counter()
        response = client.get_object(Bucket=config.bucket, Key=key)
        data = response["Body"].read()
        elapsed = time.perf_counter() - start

        return {"key": key, "size_bytes": len(data), "elapsed_s": elapsed}

    return list(map_async(_download, iter(keys), processes=processes))
