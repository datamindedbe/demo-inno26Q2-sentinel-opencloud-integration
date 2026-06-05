# TPC-DS Data IO Benchmark for S3 Proxy Testing

PySpark-based TPC-DS benchmark for stress-testing S3 endpoints (UpCloud direct vs Sentinel proxy).
The benchmark keeps TPC-DS data shape, but focuses on storage IO throughput/latency (read and write), not SQL query performance.

## Structure

```
spark-benchmark/
├── Dockerfile                          # Builds tpcds-kit + PySpark runtime
├── requirements.txt                    # Python dependencies
├── src/main/python/tpcds_benchmark.py  # TPC-DS data gen + IO benchmark runner
├── dags/s3_sentinel_stress_test.py     # Airflow DAG (Conveyor)
└── resources/                          # Terraform (IAM, S3)
```

## Configuration

Edit `dags/s3_sentinel_stress_test.py`:

```python
S3_PROXY_ENDPOINT = "..."   # Sentinel proxy URL
S3_DIRECT_ENDPOINT = "..."  # UpCloud direct endpoint
S3_BUCKET_NAME = "..."
S3_ACCESS_KEY = "..."
S3_SECRET_KEY = "..."
SCALE_FACTOR = "100"        # TPC-DS scale in GB
```

## Usage

```bash
# Build and deploy
conveyor build
conveyor deploy

# Trigger manually from Airflow UI
```

CLI mode examples:

```bash
# IO read benchmark only
python src/main/python/tpcds_benchmark.py \
	--mode io-read \
	--data-path s3a://dp-data-bucket/product-0/private/direct/sf100 \
	--iterations 1 \
	--result-path s3a://dp-data-bucket/product-0/private/direct/sf100/_results

# IO write benchmark only
python src/main/python/tpcds_benchmark.py \
	--mode io-write \
	--data-path s3a://dp-data-bucket/product-0/private/direct/sf100 \
	--io-write-path s3a://dp-data-bucket/product-0/private/direct/sf100/_io_write \
	--iterations 1 \
	--result-path s3a://dp-data-bucket/product-0/private/direct/sf100/_results
```

## DAG Tasks

Runs sequentially to avoid interference:
1. `tpcds_gen_proxy` — generate data via proxy
2. `tpcds_io_proxy` — run TPC-DS-backed IO benchmark (read + write) via proxy
3. `tpcds_gen_direct` — generate data direct to UpCloud
4. `tpcds_io_direct` — run TPC-DS-backed IO benchmark (read + write) direct

Results (timings) written to `{data_path}/_results/`.
