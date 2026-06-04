from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class S3Config:
    """Connection settings for an S3-compatible endpoint."""

    endpoint_url: str
    bucket: str
    region: str = "us-east-1"
    aws_access_key_id: str = "test"
    aws_secret_access_key: str = "test"


@dataclass
class BenchmarkParams:
    """Tuning knobs for a benchmark run."""

    big_file_size_mb: int = 100
    small_file_size_kb: int = 10
    small_file_count: int = 100
    processes: int = 4
