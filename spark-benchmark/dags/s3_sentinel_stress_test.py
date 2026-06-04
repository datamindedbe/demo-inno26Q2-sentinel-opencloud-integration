from datetime import datetime
from airflow import DAG
from conveyor.operators import ConveyorSparkSubmitOperatorV2

default_args = {
    'owner': 'performance-testing',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 0,
}

dag = DAG(
    's3_tpcds_stress_test',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['performance', 's3_proxy', 'tpcds']
)

# Configuration: UpCloud S3-compatible storage
S3_PROXY_ENDPOINT = "s3sentinel.sentinel.playground.dataminded.cloud"    # e.g., http://sentinel-proxy:80
S3_DIRECT_ENDPOINT = "https://fsabm.upcloudobjects.com"   # e.g., https://objectstorage.europe-1.upcloud.com
S3_PROXY_TOKEN = "RVQq2_OZmLU3-B1WX5BejPPW93_MmjQpxvxW4S34jFPblTU618br43Zjo7hKlFH3kPdXUbA"
S3_BUCKET_NAME = "dp-data-bucket"
S3_ACCESS_KEY = "AKIA1CEBCD7395167FAD"
S3_SECRET_KEY = "jjU1nB1omxTD4BIGEtefFGc67C01YJLfF7ps6Z+d"

TARGET_BUCKET = f"s3a://{S3_BUCKET_NAME}"
SCALE_FACTOR = "100"  # TPC-DS scale factor in GB
NUM_WORKERS = 12

# S3A config for UpCloud S3-compatible storage
def make_s3_config(endpoint, bearer_token=None):
    config = {
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.access.key": S3_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": S3_SECRET_KEY,
        "spark.hadoop.fs.s3a.connection.maximum": "2500",
        "spark.hadoop.fs.s3a.threads.max": "2500",
        "spark.hadoop.fs.s3a.connection.establish.timeout": "5000",
        "spark.hadoop.fs.s3a.connection.timeout": "10000",
        "spark.hadoop.fs.s3a.multipart.size": "16m",
        "spark.hadoop.fs.s3a.multipart.threshold": "16m",
        "spark.hadoop.fs.s3a.fast.upload": "true",
        "spark.hadoop.fs.s3a.readahead.range": "32m",
    }
    if bearer_token:
        config["spark.hadoop.fs.s3a.connection.header.Authorization"] = f"Bearer {bearer_token}"
    return config


def make_tpcds_task(task_id, mode, scale, data_path, s3_config, num_exec):
    """Factory for TPC-DS benchmark task."""
    return ConveyorSparkSubmitOperatorV2(
        task_id=task_id,
        application="local:///opt/app/tpcds_benchmark.py",
        application_args=[
            "--mode", mode,
            "--scale-factor", scale,
            "--data-path", data_path,
            "--num-partitions", str(num_exec),
            "--iterations", "1",
            "--result-path", f"{data_path.rstrip('/')}/_results",
        ],
        conf=s3_config,
        driver_instance_type="mx.xlarge",
        executor_instance_type="mx.4xlarge",
        num_executors=num_exec,
        dag=dag,
    )


def make_preflight_task(task_id, data_path, s3_config):
    """Lightweight connectivity check task."""
    return ConveyorSparkSubmitOperatorV2(
        task_id=task_id,
        application="local:///opt/app/tpcds_benchmark.py",
        application_args=[
            "--mode", "preflight",
            "--scale-factor", "1",
            "--data-path", data_path,
        ],
        conf=s3_config,
        driver_instance_type="mx.small",
        executor_instance_type="mx.small",
        num_executors=1,
        dag=dag,
    )


# ========== PREFLIGHT CHECKS ==========
proxy_config = make_s3_config(S3_PROXY_ENDPOINT, bearer_token=S3_PROXY_TOKEN)
proxy_data_path = f"{TARGET_BUCKET}/tpcds-proxy/sf{SCALE_FACTOR}"

direct_config = make_s3_config(S3_DIRECT_ENDPOINT)
direct_data_path = f"{TARGET_BUCKET}/tpcds-direct/sf{SCALE_FACTOR}"

preflight_proxy = make_preflight_task(
    "preflight_check_proxy",
    proxy_data_path,
    proxy_config
)

preflight_direct = make_preflight_task(
    "preflight_check_direct",
    direct_data_path,
    direct_config
)


# ========== PROXY PATH TASKS ==========

gen_proxy = make_tpcds_task(
    "tpcds_gen_proxy",
    "gen",
    SCALE_FACTOR,
    proxy_data_path,
    proxy_config,
    NUM_WORKERS
)

query_proxy = make_tpcds_task(
    "tpcds_query_proxy",
    "query",
    SCALE_FACTOR,
    proxy_data_path,
    proxy_config,
    NUM_WORKERS
)


# ========== DIRECT PATH TASKS ==========
direct_config = make_s3_config(S3_DIRECT_ENDPOINT)
direct_data_path = f"{TARGET_BUCKET}/tpcds-direct/sf{SCALE_FACTOR}"

gen_direct = make_tpcds_task(
    "tpcds_gen_direct",
    "gen",
    SCALE_FACTOR,
    direct_data_path,
    direct_config,
    NUM_WORKERS
)

query_direct = make_tpcds_task(
    "tpcds_query_direct",
    "query",
    SCALE_FACTOR,
    direct_data_path,
    direct_config,
    NUM_WORKERS
)


# Task dependencies:
# 1. Preflight checks for both endpoints
# 2. Direct benchmark runs first
# 3. Proxy benchmark runs after
preflight_proxy >> preflight_direct >> gen_direct >> query_direct >> gen_proxy >> query_proxy