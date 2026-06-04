# TPC-DS Benchmark for S3 Proxy Testing

PySpark-based TPC-DS benchmark for stress-testing S3 endpoints (UpCloud direct vs Sentinel proxy).

## Structure

```
spark-benchmark/
├── Dockerfile                          # Builds tpcds-kit + PySpark runtime
├── requirements.txt                    # Python dependencies
├── src/main/python/tpcds_benchmark.py  # TPC-DS data gen + query runner
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

## DAG Tasks

Runs sequentially to avoid interference:
1. `tpcds_gen_proxy` — generate data via proxy
2. `tpcds_query_proxy` — run 99 TPC-DS queries via proxy
3. `tpcds_gen_direct` — generate data direct to UpCloud
4. `tpcds_query_direct` — run queries direct

Results (timings) written to `{data_path}/_results/`.
