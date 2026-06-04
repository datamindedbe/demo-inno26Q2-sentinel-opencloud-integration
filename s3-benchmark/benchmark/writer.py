from __future__ import annotations

import io
import time

from boto3.s3.transfer import TransferConfig

from benchmark.config import S3Config
from canal.flow import map_async

# Multipart settings for the streaming upload. Peak memory while uploading is
# roughly MAX_CONCURRENCY * CHUNK_SIZE, independent of total file size.
CHUNK_SIZE = 8 * 1024 * 1024
MAX_CONCURRENCY = 4


class _ZeroStream(io.RawIOBase):
    """Read-only file-like object that yields `total` zero bytes lazily.

    Lets us stream an arbitrarily large upload without ever materialising the
    whole payload in memory — only one chunk exists at a time.
    """

    def __init__(self, total: int):
        self._remaining = total

    def readable(self) -> bool:
        return True

    def readinto(self, b) -> int:
        n = min(len(b), self._remaining)
        b[:n] = bytes(n)
        self._remaining -= n
        return n


def _make_client(config: S3Config):
    """Create a boto3 S3 client. Always call this inside the worker process."""
    return config.make_client()


def write_big_file(config: S3Config, key: str, size_mb: int = 100) -> dict:
    """Stream-upload a single large file and return timing info.

    Memory stays bounded regardless of `size_mb` because the payload is
    generated lazily and uploaded as multipart chunks.
    """
    client = _make_client(config)
    total = size_mb * 1024 * 1024
    transfer = TransferConfig(
        multipart_threshold=CHUNK_SIZE,
        multipart_chunksize=CHUNK_SIZE,
        max_concurrency=MAX_CONCURRENCY,
    )

    start = time.perf_counter()
    client.upload_fileobj(_ZeroStream(total), config.bucket, key, Config=transfer)
    elapsed = time.perf_counter() - start

    return {"key": key, "size_mb": size_mb, "elapsed_s": elapsed}


def write_small_files(
    config: S3Config,
    prefix: str,
    count: int,
    size_kb: int = 10,
    processes: int = 4,
) -> list[dict]:
    """Upload many small files in parallel and return per-file timing info."""

    def _upload(index: int) -> dict:
        # Client created inside the worker to be fork-safe.
        client = _make_client(config)
        key = f"{prefix}/{index:06d}"
        body = b"x" * size_kb * 1024

        start = time.perf_counter()
        client.put_object(Bucket=config.bucket, Key=key, Body=body)
        elapsed = time.perf_counter() - start

        return {"key": key, "size_kb": size_kb, "elapsed_s": elapsed}

    return list(map_async(_upload, iter(range(count)), processes=processes))
