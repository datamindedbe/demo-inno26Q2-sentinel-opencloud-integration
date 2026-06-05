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
S3_PROXY_ENDPOINT  = "https://s3sentinel.sentinel.playground.dataminded.cloud"  # S3 proxy
S3_STS_ENDPOINT    = "https://s3sentinel-sts.sentinel.playground.dataminded.cloud"  # STS credential vending
S3_DIRECT_ENDPOINT = "https://fsabm.upcloudobjects.com"
S3_PROXY_TOKEN     = "RVQq2_OZmLU3-B1WX5BejPPW93_MmjQpxvxW4S34jFPblTU618br43Zjo7hKlFH3kPdXUbA"  # Zitadel PAT

# Zitadel config — used to exchange the PAT for a signed JWT before calling the STS endpoint.
# ZITADEL_ENDPOINT : base URL of the Zitadel instance
# ZITADEL_CLIENT_ID: client_id of the machine-user OIDC app in Zitadel
# ZITADEL_AUDIENCE : s3sentinel Zitadel project ID (= expected 'aud' claim)
ZITADEL_ENDPOINT      = "https://zitadel.sentinel.playground.dataminded.cloud"
ZITADEL_CLIENT_ID     = "product-0"  # machine-user client_id from Zitadel
ZITADEL_CLIENT_SECRET = "ntf3wHkfcb0QcRJXchfwf75wTEEXMWLpvnddSpkgkDxDRmhElLygW8D163kxPHJV"
ZITADEL_AUDIENCE      = "375932491829084258" # the s3sentinel project ID 
S3_BUCKET_NAME = "dp-data-bucket"
S3_ACCESS_KEY = "AKIA1CEBCD7395167FAD"
S3_SECRET_KEY = "jjU1nB1omxTD4BIGEtefFGc67C01YJLfF7ps6Z+d"

TARGET_BUCKET = f"s3a://{S3_BUCKET_NAME}"
SCALE_FACTOR = "100"  # TPC-DS scale factor in GB
NUM_WORKERS = 12

# S3A config for UpCloud S3-compatible storage
def make_s3_config(endpoint):
    return {
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


# ========== COMPARE TASK ==========
def make_compare_task():
    """Compare proxy vs direct benchmark timing results."""
    return ConveyorSparkSubmitOperatorV2(
        task_id="compare_results",
        application="local:///opt/app/tpcds_benchmark.py",
        application_args=[
            "--mode", "compare",
            "--proxy-result-path", f"{TARGET_BUCKET}/product-0/private/proxy/sf{SCALE_FACTOR}/_results",
            "--direct-result-path", f"{TARGET_BUCKET}/product-0/private/direct/sf{SCALE_FACTOR}/_results",
            "--result-path", f"{TARGET_BUCKET}/tpcds-comparison/sf{SCALE_FACTOR}",
        ],
        conf=proxy_config,
        driver_instance_type="mx.small",
        executor_instance_type="mx.small",
        num_executors=1,
        dag=dag,
    )


def make_tpcds_task(task_id, mode, scale, data_path, s3_config, num_exec, extra_args=None):
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
        ] + (extra_args or []),
        conf=s3_config,
        driver_instance_type="mx.xlarge",
        executor_instance_type="mx.4xlarge",
        num_executors=num_exec,
        dag=dag,
    )


def make_preflight_task(task_id, data_path, s3_config, extra_args=None):
    """Lightweight connectivity check task."""
    return ConveyorSparkSubmitOperatorV2(
        task_id=task_id,
        application="local:///opt/app/tpcds_benchmark.py",
        application_args=[
            "--mode", "preflight",
            "--scale-factor", "1",
            "--data-path", data_path,
        ] + (extra_args or []),
        conf=s3_config,
        driver_instance_type="mx.small",
        executor_instance_type="mx.small",
        num_executors=1,
        dag=dag,
    )


# ========== PREFLIGHT CHECKS ==========
direct_config = make_s3_config(S3_DIRECT_ENDPOINT)
direct_data_path = f"{TARGET_BUCKET}/product-0/private/direct/sf{SCALE_FACTOR}"

# ========== PROXY CONFIG ==========
# Credentials are fetched from the STS endpoint at runtime by the Spark app.
proxy_config = make_s3_config(S3_PROXY_ENDPOINT)
proxy_data_path = f"{TARGET_BUCKET}/product-0/private/proxy/sf{SCALE_FACTOR}"
proxy_sts_args = [
    "--sts-endpoint",         S3_STS_ENDPOINT,
    "--bearer-token",         S3_PROXY_TOKEN,
    "--zitadel-endpoint",     ZITADEL_ENDPOINT,
    "--zitadel-client-id",    ZITADEL_CLIENT_ID,
    "--zitadel-client-secret", ZITADEL_CLIENT_SECRET,
    "--zitadel-audience",     ZITADEL_AUDIENCE,
]

preflight_proxy = make_preflight_task("preflight_check_proxy", proxy_data_path, proxy_config,
                                      extra_args=proxy_sts_args)

gen_proxy = make_tpcds_task(
    "tpcds_gen_proxy", "gen", SCALE_FACTOR, proxy_data_path, proxy_config, NUM_WORKERS,
    extra_args=proxy_sts_args
)
io_proxy = make_tpcds_task(
    "tpcds_io_proxy", "io", SCALE_FACTOR, proxy_data_path, proxy_config, NUM_WORKERS,
    extra_args=proxy_sts_args
)
compare = make_compare_task()

preflight_direct = make_preflight_task(
    "preflight_check_direct",
    direct_data_path,
    direct_config
)


# ========== DIRECT PATH TASKS ==========

gen_direct = make_tpcds_task(
    "tpcds_gen_direct",
    "gen",
    SCALE_FACTOR,
    direct_data_path,
    direct_config,
    NUM_WORKERS
)

io_direct = make_tpcds_task(
    "tpcds_io_direct",
    "io",
    SCALE_FACTOR,
    direct_data_path,
    direct_config,
    NUM_WORKERS
)


# Task dependencies:
preflight_proxy >> gen_proxy
preflight_direct >> gen_direct
preflight_direct >> gen_proxy
gen_direct >> io_direct
gen_proxy >> io_proxy
[io_direct, io_proxy] >> compare