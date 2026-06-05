# Spark Benchmark: TPC-DS-Shaped IO Benchmark

This folder contains the Spark-based benchmark used to compare IO performance between:

- Direct UpCloud S3 access
- S3 access through s3sentinel proxy

The benchmark keeps the TPC-DS data shape (table layout/partitioning style) but measures storage IO behavior rather than SQL query speed.

## What This Benchmark Measures

- IO read throughput/latency by scanning generated Parquet tables
- IO write throughput/latency by rewriting Parquet data
- Side-by-side timing comparison of proxy vs direct access

Modes implemented in the runner:

- `gen`: generate TPC-DS-shaped data
- `preflight`: connectivity and credential checks
- `io-read`: read benchmark
- `io-write`: write benchmark
- `io`: read + write benchmark
- `compare`: aggregate and compare proxy/direct results

## Where It Runs

This benchmark runs as Spark applications on Conveyor-managed Kubernetes.

- DAG/orchestration: Airflow on Conveyor
- Driver/executors: ephemeral Spark pods in the selected Conveyor environment
- Data path: S3A URLs in the configured bucket/prefix

Primary runtime files:

- `dags/s3_sentinel_stress_test.py`: Airflow DAG and Spark config
- `src/main/python/tpcds_benchmark.py`: benchmark logic
- `Dockerfile`: benchmark image build

## Execution Flow

Default DAG flow:

1. `preflight_check_proxy`
2. `preflight_check_direct`
3. `tpcds_gen_proxy`
4. `tpcds_gen_direct`
5. `tpcds_io_proxy`
6. `tpcds_io_direct`
7. `compare_results`

Results are written under each benchmark path in `_results/results` (Parquet), and comparison output under `_results/comparison` when compare mode is enabled.

## Configuration

Edit DAG settings in `dags/s3_sentinel_stress_test.py`:

- Proxy and direct endpoints
- Bucket name and prefixes
- Scale factor
- Worker sizing and executor count
- STS/Zitadel configuration for proxy path

Build/deploy:

```bash
conveyor build
conveyor deploy --env demo
```

## Local CLI Examples

```bash
# IO read benchmark
python src/main/python/tpcds_benchmark.py \
  --mode io-read \
  --data-path s3a://dp-data-bucket/product-0/private/direct/sf100 \
  --iterations 1 \
  --result-path s3a://dp-data-bucket/product-0/private/direct/sf100/_results

# IO write benchmark
python src/main/python/tpcds_benchmark.py \
  --mode io-write \
  --data-path s3a://dp-data-bucket/product-0/private/direct/sf100 \
  --io-write-path s3a://dp-data-bucket/product-0/private/direct/sf100/_io_write \
  --iterations 1 \
  --result-path s3a://dp-data-bucket/product-0/private/direct/sf100/_results
```

## Security Note About Credentials in This Folder

Some values in test configuration are tied to short-lived, demo infrastructure.

- In this setup, the benchmark cluster and related environment are ephemeral and deleted after use.
- That significantly reduces practical risk from historical credential exposure in old logs/config snapshots.

Even with ephemeral clusters, treat any exposed credential as compromised and rotate it.

Recommended hygiene:

1. Rotate static access keys and client secrets.
2. Prefer short-lived STS credentials over long-lived keys.
3. Move secrets to environment/secret manager references instead of committing literal values.
4. Remove/redact sensitive values from historical examples where possible.
