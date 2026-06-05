#!/usr/bin/env python3
"""
TPC-DS benchmark using PySpark and tpcds-kit (dsdgen).
Modes: gen (data generation), query (run queries), gen-query (both).
Distributed data gen: each executor runs dsdgen with -PARALLEL/-CHILD flags.
"""
import argparse
import logging
import os
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="TPC-DS benchmark")
    parser.add_argument("--mode", required=True,
                        choices=["gen", "query", "gen-query", "preflight", "compare"],
                        help="Benchmark mode (preflight = connectivity check only, compare = diff results)")
    parser.add_argument("--scale-factor", default="100", type=str,
                        help="TPC-DS scale factor (GB)")
    parser.add_argument("--data-path", required=True,
                        help="S3 path for generated data")
    parser.add_argument("--query-path", default="/opt/tpcds/queries",
                        help="Path to TPC-DS query files")
    parser.add_argument("--dsdgen-dir", default="/opt/tpcds-kit/tools",
                        help="Path to dsdgen binary")
    parser.add_argument("--num-partitions", type=int, default=100,
                        help="Number of executors/dsdgen workers for data gen")
    parser.add_argument("--iterations", type=int, default=1,
                        help="Number of times to run each query")
    parser.add_argument("--result-path",
                        help="S3 path to write query results and timings")
    parser.add_argument("--proxy-result-path",
                        help="S3 path to proxy query results (for compare mode)")
    parser.add_argument("--direct-result-path",
                        help="S3 path to direct query results (for compare mode)")
    parser.add_argument("--sts-endpoint",
                        help="s3sentinel STS endpoint for AssumeRoleWithWebIdentity")
    parser.add_argument("--bearer-token",
                        help="Zitadel PAT to exchange for a signed JWT")
    parser.add_argument("--zitadel-endpoint",
                        help="Base URL of the Zitadel instance (for PAT -> JWT exchange)")
    parser.add_argument("--zitadel-client-id",
                        help="Zitadel machine-user client_id")
    parser.add_argument("--zitadel-client-secret",
                        help="Zitadel machine-user client_secret")
    parser.add_argument("--zitadel-audience",
                        help="s3sentinel Zitadel project ID (expected aud claim)")
    args = parser.parse_args()
    return args


def get_zitadel_jwt(zitadel_endpoint, client_id, client_secret, audience):
    """
    Get a Zitadel JWT via client_credentials grant.
    The returned id_token is a JWT signed by Zitadel's JWKS, suitable
    for use as WebIdentityToken with the s3sentinel STS endpoint.
    """
    import urllib.request
    import urllib.parse
    import json

    scope = f"openid urn:zitadel:iam:org:project:id:{audience}:aud"
    data = urllib.parse.urlencode({
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": scope,
    }).encode()

    url = f"{zitadel_endpoint.rstrip('/')}/oauth/v2/token"
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

    with urllib.request.urlopen(req) as resp:
        body = json.loads(resp.read())

    # Try id_token first (for machine-to-machine), fall back to access_token
    token = body.get("id_token") or body.get("access_token")
    if not token:
        raise RuntimeError(f"Zitadel client_credentials grant returned no token: {body}")
    logger.info("Zitadel JWT obtained successfully")
    return token


def get_sts_credentials(sts_endpoint, bearer_token=None, zitadel_endpoint=None,
                        zitadel_client_id=None, zitadel_client_secret=None, zitadel_audience=None):
    """
    Exchange a JWT bearer token for temporary S3 credentials via AssumeRoleWithWebIdentity.
    If zitadel_endpoint/client_id/client_secret/audience are provided, first fetches a
    Zitadel JWT via client_credentials, then passes that JWT to the STS endpoint.
    If bearer_token is provided, uses it directly (expected to be a JWT).
    Returns dict with access_key, secret_key, session_token.
    """
    import json
    import urllib.parse
    import urllib.request
    
    jwt_token = bearer_token
    
    # Fetch JWT from Zitadel if credentials provided (takes precedence over bearer_token)
    if zitadel_endpoint and zitadel_client_id and zitadel_client_secret and zitadel_audience:
        logger.info("Fetching Zitadel JWT for STS...")
        jwt_token = get_zitadel_jwt(zitadel_endpoint, zitadel_client_id, zitadel_client_secret,
                                    zitadel_audience)
    
    if not jwt_token:
        raise ValueError("No authentication token available: provide either bearer_token or Zitadel credentials")

    # Use boto3 STS client for proper protocol handling
    import boto3
    from botocore.config import Config
    
    sts = boto3.client(
        "sts",
        endpoint_url=sts_endpoint,
        aws_access_key_id="placeholder",
        aws_secret_access_key="placeholder",
        region_name="us-east-1",
        config=Config(signature_version='s3v4'),
    )
    
    logger.info(f"Assuming role via STS: {sts_endpoint}")
    resp = sts.assume_role_with_web_identity(
        RoleArn="arn:aws:sts::000000000000:assumed-role/s3sentinel/spark-benchmark",
        RoleSessionName="spark-benchmark",
        WebIdentityToken=jwt_token,
    )
    
    creds = resp["Credentials"]
    return {
        "access_key": creds["AccessKeyId"],
        "secret_key": creds["SecretAccessKey"],
        "session_token": creds["SessionToken"],
    }


def gen_data(spark, args):
    """
    Distributed TPC-DS data generation using dsdgen.
    Each partition runs dsdgen with -PARALLEL N -CHILD i.
    Workers generate CSV locally, read with plain Python, yield tuples.
    Driver collects and writes to parquet via Spark DataFrame.
    """
    logger.info(f"Starting data generation: SF={args.scale_factor}, path={args.data_path}")

    dsdgen_path = str(Path(args.dsdgen_dir) / "dsdgen")
    num_workers = args.num_partitions
    scale_factor = int(args.scale_factor)
    scale_per_worker = scale_factor // num_workers if scale_factor >= num_workers else 1

    table_names = [
        "call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
        "customer_address", "customer_demographics", "date_dim", "household_demographics",
        "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store",
        "store_returns", "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
        "web_sales"
    ]

    for table in table_names:
        logger.info(f"Processing table: {table}")
        output_path = f"{args.data_path.rstrip('/')}/{table}"

        # Broadcast config to workers (plain values, not spark objects)
        bc_dsdgen = spark.sparkContext.broadcast(dsdgen_path)
        bc_scale = spark.sparkContext.broadcast(scale_per_worker)
        bc_num_workers = spark.sparkContext.broadcast(num_workers)
        bc_table = spark.sparkContext.broadcast(table)

        def gen_table_partition(partition_id):
            """Run dsdgen on worker, read CSV with plain Python, yield tuples."""
            import csv
            dsdgen = bc_dsdgen.value
            scale = bc_scale.value
            n_workers = bc_num_workers.value
            tbl = bc_table.value

            with tempfile.TemporaryDirectory() as tmpdir:
                cmd = [
                    dsdgen,
                    "-SCALE", str(scale),
                    "-PARALLEL", str(n_workers),
                    "-CHILD", str(partition_id + 1),
                    "-DIR", tmpdir,
                    "-TABLE", tbl,
                    "-TERMINATE", "N"
                ]
                result = subprocess.run(cmd, capture_output=True, text=True, check=False,
                                       cwd=str(Path(dsdgen).parent))
                if result.returncode != 0:
                    raise RuntimeError(f"dsdgen failed for {tbl} child {partition_id + 1}: {result.stderr}")

                csv_file = Path(tmpdir) / f"{tbl}.dat"
                if not csv_file.exists():
                    csv_file = Path(tmpdir) / f"{tbl}.csv"

                if csv_file.exists():
                    with open(csv_file, "r") as f:
                        reader = csv.reader(f, delimiter="|")
                        for row in reader:
                            if row and row[-1] == "":
                                row = row[:-1]
                            yield tuple(row)

        rdd_table = spark.sparkContext.parallelize(range(num_workers), numSlices=num_workers) \
            .flatMap(gen_table_partition)

        row_count = rdd_table.count()
        if row_count > 0:
            df = spark.createDataFrame(rdd_table)
            df.write.mode("overwrite").parquet(output_path)
            logger.info(f"Wrote {output_path} ({row_count} rows)")
        else:
            logger.warning(f"No data generated for {table}")

    logger.info("Data generation complete")


def run_queries(spark, args):
    """
    Run TPC-DS queries from SQL files and record timings.
    """
    logger.info(f"Starting query execution from {args.query_path}")

    db_name = f"tpcds_sf{args.scale_factor}"
    spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
    spark.sql(f"CREATE DATABASE {db_name}")

    # Register tables from parquet
    data_path = args.data_path.rstrip("/")
    for table in list_tpcds_tables():
        table_path = f"{data_path}/{table}"
        try:
            spark.sql(f"CREATE TABLE {db_name}.{table} USING PARQUET LOCATION '{table_path}'")
            logger.info(f"Registered table {table}")
        except Exception as e:
            logger.warning(f"Failed to register {table}: {e}")

    spark.sql(f"USE {db_name}")

    # Load and run queries
    query_dir = Path(args.query_path)
    query_files = sorted(query_dir.glob("q*.sql"))

    if not query_files:
        logger.warning(f"No query files found in {query_dir}")
        return

    results = []
    for query_file in query_files:
        query_name = query_file.stem
        query_sql = query_file.read_text()

        for iteration in range(args.iterations):
            logger.info(f"Running {query_name} (iteration {iteration + 1}/{args.iterations})")
            start = datetime.now()
            try:
                df = spark.sql(query_sql)
                df.collect()  # Force execution
                elapsed = (datetime.now() - start).total_seconds()
                results.append({
                    "query": query_name,
                    "iteration": iteration + 1,
                    "elapsed_seconds": elapsed
                })
                logger.info(f"{query_name}: {elapsed:.2f}s")
            except Exception as e:
                logger.error(f"{query_name} failed: {e}")
                results.append({
                    "query": query_name,
                    "iteration": iteration + 1,
                    "elapsed_seconds": None,
                    "error": str(e)
                })

    # Write results
    if args.result_path:
        result_df = spark.createDataFrame(results)
        result_path = f"{args.result_path.rstrip('/')}/results"
        result_df.write.mode("overwrite").parquet(result_path)
        logger.info(f"Results written to {result_path}")


def list_tpcds_tables():
    """Return list of all TPC-DS table names."""
    return [
        "call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
        "customer_address", "customer_demographics", "date_dim", "household_demographics",
        "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store",
        "store_returns", "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
        "web_sales"
    ]


def compare_results(spark, args):
    """
    Join proxy and direct query timing results and write a side-by-side comparison.
    Output columns: query, proxy_seconds, direct_seconds, speedup_ratio (direct/proxy).
    """
    from pyspark.sql import functions as F

    logger.info(f"Comparing results: proxy={args.proxy_result_path}, direct={args.direct_result_path}")

    proxy = spark.read.parquet(f"{args.proxy_result_path.rstrip('/')}/results")
    direct = spark.read.parquet(f"{args.direct_result_path.rstrip('/')}/results")

    proxy_agg = proxy.groupBy("query").agg(
        F.avg("elapsed_seconds").alias("proxy_seconds")
    )
    direct_agg = direct.groupBy("query").agg(
        F.avg("elapsed_seconds").alias("direct_seconds")
    )

    comparison = proxy_agg.join(direct_agg, on="query", how="outer").withColumn(
        "speedup_ratio",
        F.round(F.col("direct_seconds") / F.col("proxy_seconds"), 4)
    ).orderBy("query")

    comparison.show(truncate=False)

    if args.result_path:
        out = f"{args.result_path.rstrip('/')}/comparison"
        comparison.write.mode("overwrite").parquet(out)
        logger.info(f"Comparison written to {out}")


def check_s3_connectivity(spark, data_path, timeout_seconds=30):
    """
    Verify S3 endpoint is reachable before starting heavy workload.
    Fails fast with clear error message if connection fails.
    """
    logger.info(f"Checking S3 connectivity to {data_path}...")

    # Extract bucket from path (s3a://bucket/path -> bucket)
    parts = data_path.replace("s3a://", "").split("/")
    bucket = parts[0]
    test_path = f"s3a://{bucket}/_connectivity_test"

    try:
        # Try to write a small test file
        test_df = spark.createDataFrame([("connectivity_check", 1)], ["test", "value"])
        test_df.write.mode("overwrite").parquet(test_path)

        # Read it back to verify round-trip
        spark.read.parquet(test_path).collect()

        # Clean up
        spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI.create(f"s3a://{bucket}"),
            spark._jsc.hadoopConfiguration()
        ).delete(spark._jvm.org.apache.hadoop.fs.Path(test_path), True)

        logger.info("S3 connectivity check PASSED")

    except Exception as e:
        endpoint = spark.conf.get("spark.hadoop.fs.s3a.endpoint", "default")
        error_msg = (
            f"\n{'='*60}\n"
            f"S3 CONNECTIVITY CHECK FAILED\n"
            f"{'='*60}\n"
            f"Endpoint: {endpoint}\n"
            f"Bucket: {bucket}\n"
            f"Error: {e}\n"
            f"{'='*60}\n"
            f"Check:\n"
            f"  1. S3 endpoint is reachable\n"
            f"  2. Credentials are valid\n"
            f"  3. Bucket exists and is accessible\n"
            f"{'='*60}"
        )
        logger.error(error_msg)
        raise ConnectionError(f"S3 connectivity check failed: {e}") from e


def main():
    args = parse_args()

    builder = SparkSession.builder.appName("tpcds-benchmark")

    # Try STS credential exchange if configured, but make it optional
    creds_obtained = False
    if args.sts_endpoint:
        try:
            logger.info(f"Fetching temporary credentials from STS: {args.sts_endpoint}")
            creds = get_sts_credentials(
                args.sts_endpoint,
                bearer_token=getattr(args, "bearer_token", None),
                zitadel_endpoint=getattr(args, "zitadel_endpoint", None),
                zitadel_client_id=getattr(args, "zitadel_client_id", None),
                zitadel_client_secret=getattr(args, "zitadel_client_secret", None),
                zitadel_audience=getattr(args, "zitadel_audience", None),
            )
            builder = (builder
                       .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                               "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
                       .config("spark.hadoop.fs.s3a.access.key", creds["access_key"])
                       .config("spark.hadoop.fs.s3a.secret.key", creds["secret_key"])
                       .config("spark.hadoop.fs.s3a.session.token", creds["session_token"]))
            logger.info("STS credentials obtained successfully")
            creds_obtained = True
        except Exception as e:
            logger.warning(f"Failed to obtain STS credentials: {e}. Continuing with static credentials.")

    spark = builder.getOrCreate()

    try:
        # Preflight mode just checks connectivity and exits
        if args.mode == "preflight":
            check_s3_connectivity(spark, args.data_path)
            logger.info("Preflight check completed successfully")
            return

        # Compare mode reads existing results from both paths and diffs them
        if args.mode == "compare":
            compare_results(spark, args)
            return

        # Fail fast if S3 is not reachable
        check_s3_connectivity(spark, args.data_path)

        if args.mode in ["gen", "gen-query"]:
            gen_data(spark, args)

        if args.mode in ["query", "gen-query"]:
            run_queries(spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
