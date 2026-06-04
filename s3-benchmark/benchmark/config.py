from __future__ import annotations

import os
from dataclasses import dataclass, field

import boto3
from botocore.config import Config


@dataclass
class S3Config:
    """Connection settings for an S3-compatible endpoint.

    Credentials follow standard boto3 precedence: explicit values here take
    priority, otherwise boto3 reads AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
    from the environment (compatible with K8s Secrets and IAM roles).

    Set endpoint_url=None to target real AWS S3.
    """

    bucket: str
    endpoint_url: str | None = field(default_factory=lambda: os.getenv("S3_ENDPOINT_URL") or os.getenv("LOCALSTACK_ENDPOINT"))  # None → real AWS S3
    region: str = field(default_factory=lambda: os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
    aws_access_key_id: str | None = field(default_factory=lambda: os.getenv("AWS_ACCESS_KEY_ID"))
    aws_secret_access_key: str | None = field(default_factory=lambda: os.getenv("AWS_SECRET_ACCESS_KEY"))

    def make_client(self):
        """Create a boto3 S3 client from this config.

        Always call inside the worker process — never share clients across forks.
        Uses path-style addressing for custom endpoints (localstack / MinIO),
        and virtual-hosted style for real AWS S3.
        """
        addressing = "path" if self.endpoint_url else "auto"
        kwargs: dict = dict(
            region_name=self.region,
            config=Config(s3={"addressing_style": addressing}),
        )
        if self.endpoint_url:
            kwargs["endpoint_url"] = self.endpoint_url
        if self.aws_access_key_id:
            kwargs["aws_access_key_id"] = self.aws_access_key_id
        if self.aws_secret_access_key:
            kwargs["aws_secret_access_key"] = self.aws_secret_access_key
        return boto3.client("s3", **kwargs)


@dataclass
class BenchmarkParams:
    """Tuning knobs for a benchmark run."""

    big_file_size_mb: int = 100
    small_file_size_kb: int = 10
    small_file_count: int = 100
    processes: int = 4
