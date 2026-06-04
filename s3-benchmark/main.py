"""
Standalone benchmark script — runs four S3 scenarios and prints a results table.

Usage:
    uv run python main.py [--endpoint-url URL] [--bucket NAME]
                          [--big-file-size-mb MB] [--small-file-count N]
                          [--processes N]

LocalStack (default):
    docker compose up -d
    uv run python main.py
"""

from __future__ import annotations

import argparse
import os
import statistics
import time
import uuid

import boto3
from botocore.config import Config
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

from benchmark.config import S3Config
from benchmark.deleter import delete_file, delete_files
from benchmark.reader import list_files, read_big_file, read_small_files
from benchmark.writer import write_big_file, write_small_files

ENDPOINT_URL = os.getenv("LOCALSTACK_ENDPOINT", "http://localhost:4566")
BIG_FILE_KEY = "benchmark/big_file"
SMALL_PREFIX = "benchmark/small"

console = Console()


def _make_client(config: S3Config):
    return boto3.client(
        "s3",
        endpoint_url=config.endpoint_url,
        region_name=config.region,
        aws_access_key_id=config.aws_access_key_id,
        aws_secret_access_key=config.aws_secret_access_key,
        config=Config(s3={"addressing_style": "path"}),
    )


def _ensure_bucket(client, bucket: str) -> None:
    existing = {b["Name"] for b in client.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)
        console.print(f"  [dim]Created bucket [bold]{bucket}[/bold][/dim]")
    else:
        console.print(f"  [dim]Reusing bucket [bold]{bucket}[/bold][/dim]")


def _results_table(
    write_big: dict,
    write_big_wall_s: float,
    write_small: list[dict],
    write_small_wall_s: float,
    read_big: dict,
    read_big_wall_s: float,
    read_small: list[dict],
    read_small_wall_s: float,
    list_result: dict,
    list_wall_s: float,
    delete_big: dict,
    delete_big_wall_s: float,
    delete_small: list[dict],
    delete_small_wall_s: float,
    big_file_size_mb: int,
    small_file_count: int,
) -> Table:
    table = Table(box=box.ROUNDED, show_header=True, header_style="bold cyan")
    table.add_column("Operation")
    table.add_column("Count", justify="right")
    table.add_column("Total size", justify="right")
    table.add_column("Wall time (s)", justify="right")
    table.add_column("Avg per op (ms)", justify="right")
    table.add_column("Throughput", justify="right", style="green")

    def _mb(n: int) -> str:
        return f"{n} MB" if n >= 1 else f"{n * 1024} KB"

    # write big file
    table.add_row(
        "Write  big file",
        "1",
        _mb(big_file_size_mb),
        f"{write_big_wall_s:.3f}",
        f"{write_big_wall_s * 1000:.1f}",
        f"{big_file_size_mb / write_big_wall_s:.1f} MB/s",
    )

    # write small files
    avg_ms = statistics.mean(r["elapsed_s"] for r in write_small) * 1000
    table.add_row(
        "Write  small files",
        str(small_file_count),
        f"{small_file_count} KB",
        f"{write_small_wall_s:.3f}",
        f"{avg_ms:.1f}",
        f"{small_file_count / write_small_wall_s:.1f} files/s",
    )

    # read big file
    table.add_row(
        "Read   big file",
        "1",
        _mb(big_file_size_mb),
        f"{read_big_wall_s:.3f}",
        f"{read_big_wall_s * 1000:.1f}",
        f"{big_file_size_mb / read_big_wall_s:.1f} MB/s",
    )

    # read small files
    avg_ms = statistics.mean(r["elapsed_s"] for r in read_small) * 1000
    table.add_row(
        "Read   small files",
        str(small_file_count),
        f"{small_file_count} KB",
        f"{read_small_wall_s:.3f}",
        f"{avg_ms:.1f}",
        f"{small_file_count / read_small_wall_s:.1f} files/s",
    )

    # list files
    table.add_row(
        "List   files",
        str(list_result["count"]),
        "—",
        f"{list_wall_s:.3f}",
        f"{list_wall_s * 1000:.1f}",
        f"{list_result['count'] / list_wall_s:.1f} objects/s",
    )

    # delete big file
    table.add_row(
        "Delete big file",
        "1",
        _mb(big_file_size_mb),
        f"{delete_big_wall_s:.3f}",
        f"{delete_big_wall_s * 1000:.1f}",
        f"{big_file_size_mb / delete_big_wall_s:.1f} MB/s",
    )

    # delete small files
    avg_ms = statistics.mean(r["elapsed_s"] for r in delete_small) * 1000
    table.add_row(
        "Delete small files",
        str(small_file_count),
        f"{small_file_count} KB",
        f"{delete_small_wall_s:.3f}",
        f"{avg_ms:.1f}",
        f"{small_file_count / delete_small_wall_s:.1f} files/s",
    )

    return table


def run(
    config: S3Config,
    big_file_size_mb: int = 1,
    small_file_count: int = 5,
    processes: int = 2,
) -> None:
    client = _make_client(config)

    console.print(
        Panel.fit(
            f"[bold]S3 Benchmark[/bold]\n"
            f"Endpoint  : [cyan]{config.endpoint_url}[/cyan]\n"
            f"Bucket    : [cyan]{config.bucket}[/cyan]\n"
            f"Big file  : [cyan]{big_file_size_mb} MB[/cyan]\n"
            f"Small files: [cyan]{small_file_count} × 1 KB[/cyan]  "
            f"Workers: [cyan]{processes}[/cyan]",
            border_style="bright_blue",
        )
    )
    _ensure_bucket(client, config.bucket)
    console.print()

    write_big_result = write_small_results = read_big_result = read_small_results = list_result = delete_big_result = delete_small_results = None
    write_big_wall = write_small_wall = read_big_wall = read_small_wall = list_wall = delete_big_wall = delete_small_wall = 0.0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    ) as progress:

        t = progress.add_task(f"Writing big file  ({big_file_size_mb} MB)…", total=None)
        t0 = time.perf_counter()
        write_big_result = write_big_file(config, BIG_FILE_KEY, size_mb=big_file_size_mb)
        write_big_wall = time.perf_counter() - t0
        progress.update(t, total=1, completed=1,
                        description=f"[green]✓[/green] Write big file  ({big_file_size_mb} MB)")

        t = progress.add_task(f"Writing {small_file_count} small files ({processes} workers)…", total=None)
        t0 = time.perf_counter()
        write_small_results = write_small_files(
            config, SMALL_PREFIX, count=small_file_count, size_kb=1, processes=processes
        )
        write_small_wall = time.perf_counter() - t0
        progress.update(t, total=1, completed=1,
                        description=f"[green]✓[/green] Write {small_file_count} small files")

        t = progress.add_task(f"Reading big file  ({big_file_size_mb} MB)…", total=None)
        t0 = time.perf_counter()
        read_big_result = read_big_file(config, BIG_FILE_KEY)
        read_big_wall = time.perf_counter() - t0
        progress.update(t, total=1, completed=1,
                        description=f"[green]✓[/green] Read big file  ({big_file_size_mb} MB)")

        t = progress.add_task(f"Reading {small_file_count} small files ({processes} workers)…", total=None)
        t0 = time.perf_counter()
        read_small_results = read_small_files(
            config, [r["key"] for r in write_small_results], processes=processes
        )
        read_small_wall = time.perf_counter() - t0
        progress.update(t, total=1, completed=1,
                        description=f"[green]✓[/green] Read {small_file_count} small files")

        t = progress.add_task(f"Listing files under {SMALL_PREFIX}/…", total=None)
        t0 = time.perf_counter()
        list_result = list_files(config, SMALL_PREFIX)
        list_wall = time.perf_counter() - t0
        progress.update(t, total=1, completed=1,
                        description=f"[green]✓[/green] List files ({list_result['count']} objects)")

        t = progress.add_task(f"Deleting big file  ({big_file_size_mb} MB)…", total=None)
        t0 = time.perf_counter()
        delete_big_result = delete_file(config, BIG_FILE_KEY)
        delete_big_wall = time.perf_counter() - t0
        progress.update(t, total=1, completed=1,
                        description=f"[green]✓[/green] Delete big file  ({big_file_size_mb} MB)")

        t = progress.add_task(f"Deleting {small_file_count} small files ({processes} workers)…", total=None)
        t0 = time.perf_counter()
        delete_small_results = delete_files(
            config, [r["key"] for r in write_small_results], processes=processes
        )
        delete_small_wall = time.perf_counter() - t0
        progress.update(t, total=1, completed=1,
                        description=f"[green]✓[/green] Delete {small_file_count} small files")

    console.print()
    console.print(
        _results_table(
            write_big_result, write_big_wall,
            write_small_results, write_small_wall,
            read_big_result, read_big_wall,
            read_small_results, read_small_wall,
            list_result, list_wall,
            delete_big_result, delete_big_wall,
            delete_small_results, delete_small_wall,
            big_file_size_mb,
            small_file_count,
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="S3 benchmark")
    parser.add_argument("--endpoint-url", default=ENDPOINT_URL)
    parser.add_argument(
        "--bucket",
        default=f"benchmark-{uuid.uuid4().hex[:8]}",
        help="Bucket name (created if absent). Defaults to a random name.",
    )
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument(
        "--big-file-size-mb",
        type=int,
        default=1,
        metavar="MB",
        help="Size of the big file to write and read (default: 1)",
    )
    parser.add_argument(
        "--small-file-count",
        type=int,
        default=5,
        metavar="N",
        help="Number of small files (1 KB each) to write and read (default: 5)",
    )
    parser.add_argument(
        "--processes",
        type=int,
        default=2,
        metavar="N",
        help="Number of parallel workers for small-file operations (default: 2)",
    )
    args = parser.parse_args()

    config = S3Config(
        endpoint_url=args.endpoint_url,
        bucket=args.bucket,
        region=args.region,
    )
    run(
        config,
        big_file_size_mb=args.big_file_size_mb,
        small_file_count=args.small_file_count,
        processes=args.processes,
    )


if __name__ == "__main__":
    main()
