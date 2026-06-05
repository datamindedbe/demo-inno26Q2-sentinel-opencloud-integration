#!/usr/bin/env python3
"""
TPC-DS benchmark using PySpark and tpcds-kit (dsdgen).
Modes: gen (data generation), query (run queries), gen-query (both).
Distributed data gen: each executor runs dsdgen with -PARALLEL/-CHILD flags.
"""
import argparse
import logging
import subprocess
import tempfile
import uuid
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="TPC-DS benchmark")
    parser.add_argument("--mode", required=True,
                        choices=["gen", "query", "gen-query", "preflight", "compare",
                                 "io-read", "io-write", "io"],
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
    parser.add_argument("--io-write-path",
                        help="S3 path for IO write benchmark output (defaults to <data-path>/_io_write)")
    parser.add_argument("--io-table-filter",
                        help="Comma-separated TPC-DS table names to include in IO benchmark")
    parser.add_argument("--proxy-result-path",
                        help="S3 path to proxy benchmark results (for compare mode)")
    parser.add_argument("--direct-result-path",
                        help="S3 path to direct benchmark results (for compare mode)")
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
    Get a Zitadel JWT via client_credentials grant using HTTP Basic Auth.
    The returned id_token is a JWT signed by Zitadel's JWKS, suitable
    for use as WebIdentityToken with the s3sentinel STS endpoint.
    """
    import json
    import urllib.parse
    import urllib.request
    import base64

    scope = f"openid urn:zitadel:iam:org:project:id:{audience}:aud"
    
    # Use HTTP Basic Auth for client credentials (client_id:client_secret in Authorization header)
    credentials = urllib.parse.quote(client_id, safe="") + ":" + urllib.parse.quote(client_secret or "", safe="")
    auth_header = "Basic " + base64.b64encode(credentials.encode()).decode()
    
    # Send only grant_type and scope in body
    data = urllib.parse.urlencode({
        "grant_type": "client_credentials",
        "scope": scope,
    }).encode()

    url = f"{zitadel_endpoint.rstrip('/')}/oauth/v2/token"
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    req.add_header("Authorization", auth_header)

    with urllib.request.urlopen(req) as resp:
        body = json.loads(resp.read())

    # Use access_token (has proper TTL); id_token has exp==iat (zero lifetime)
    token = body.get("access_token") or body.get("id_token")
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

    import xml.etree.ElementTree as ET

    data = urllib.parse.urlencode({
        "Action": "AssumeRoleWithWebIdentity",
        "WebIdentityToken": jwt_token,
        "RoleArn": "arn:aws:iam::000000000000:role/s3sentinel",
        "RoleSessionName": "spark-benchmark",
        "Version": "2011-06-15",
    }).encode()

    logger.info(f"Assuming role via STS: {sts_endpoint}")
    req = urllib.request.Request(sts_endpoint.rstrip("/"), data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(req) as resp:
        body = resp.read()

    ns = "https://sts.amazonaws.com/doc/2011-06-15/"
    root = ET.fromstring(body)
    creds = root.find(f".//{{{ns}}}Credentials")
    if creds is None:
        raise RuntimeError(f"STS response missing Credentials: {body.decode()[:500]}")

    return {
        "access_key": creds.find(f"{{{ns}}}AccessKeyId").text,
        "secret_key": creds.find(f"{{{ns}}}SecretAccessKey").text,
        "session_token": creds.find(f"{{{ns}}}SessionToken").text,
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

    if "query" in proxy.columns and "query" in direct.columns:
        group_cols = ["query"]
    elif "operation" in proxy.columns and "operation" in direct.columns and \
            "table" in proxy.columns and "table" in direct.columns:
        group_cols = ["operation", "table"]
    else:
        raise ValueError(
            "Unsupported result schema for compare mode. "
            "Expected query timings or IO timings with operation/table columns."
        )

    proxy_agg = proxy.groupBy(*group_cols).agg(F.avg("elapsed_seconds").alias("proxy_seconds"))
    direct_agg = direct.groupBy(*group_cols).agg(F.avg("elapsed_seconds").alias("direct_seconds"))

    comparison = proxy_agg.join(direct_agg, on=group_cols, how="outer").withColumn(
        "speedup_ratio", F.round(F.col("direct_seconds") / F.col("proxy_seconds"), 4)
    ).orderBy(*group_cols)

    comparison.show(truncate=False)

    if args.result_path:
        out = f"{args.result_path.rstrip('/')}/comparison"
        comparison.write.mode("overwrite").parquet(out)
        logger.info(f"Comparison written to {out}")


def _selected_tpcds_tables(args):
    """Return all tables or user-filtered subset for IO benchmark."""
    all_tables = list_tpcds_tables()
    if not args.io_table_filter:
        return all_tables

    selected = [t.strip() for t in args.io_table_filter.split(",") if t.strip()]
    invalid = sorted(set(selected) - set(all_tables))
    if invalid:
        raise ValueError(f"Unknown table(s) in --io-table-filter: {', '.join(invalid)}")
    return selected


def run_io_benchmark(spark, args):
    """
    Run raw storage IO benchmark on TPC-DS datasets (without SQL execution).
    - io-read: measures full-table read scan time using DataFrame.count()
    - io-write: measures parquet rewrite time to an IO destination prefix
    - io: runs both read and write benchmarks
    """
    mode = args.mode
    run_read = mode in ["io-read", "io"]
    run_write = mode in ["io-write", "io"]

    data_path = args.data_path.rstrip("/")
    write_base = (args.io_write_path or f"{data_path}/_io_write").rstrip("/")
    run_id = datetime.utcnow().strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:8]

    logger.info(f"Starting IO benchmark mode={mode}, source={data_path}, write_base={write_base}")

    results = []
    for table in _selected_tpcds_tables(args):
        source = f"{data_path}/{table}"
        for iteration in range(args.iterations):
            iter_idx = iteration + 1

            if run_read:
                logger.info(f"IO read benchmark: {table} (iteration {iter_idx}/{args.iterations})")
                start = datetime.now()
                try:
                    rows = spark.read.parquet(source).count()
                    elapsed = (datetime.now() - start).total_seconds()
                    results.append({
                        "operation": "read",
                        "table": table,
                        "iteration": iter_idx,
                        "rows": rows,
                        "elapsed_seconds": elapsed,
                    })
                    logger.info(f"IO read {table}: {elapsed:.2f}s ({rows} rows)")
                except Exception as e:
                    logger.error(f"IO read failed for {table}: {e}")
                    results.append({
                        "operation": "read",
                        "table": table,
                        "iteration": iter_idx,
                        "rows": None,
                        "elapsed_seconds": None,
                        "error": str(e),
                    })

            if run_write:
                logger.info(f"IO write benchmark: {table} (iteration {iter_idx}/{args.iterations})")
                target = f"{write_base}/{run_id}/{table}/iter_{iter_idx}"
                start = datetime.now()
                try:
                    df = spark.read.parquet(source)
                    rows = df.count()
                    df.write.mode("overwrite").parquet(target)
                    elapsed = (datetime.now() - start).total_seconds()
                    results.append({
                        "operation": "write",
                        "table": table,
                        "iteration": iter_idx,
                        "rows": rows,
                        "elapsed_seconds": elapsed,
                        "target_path": target,
                    })
                    logger.info(f"IO write {table}: {elapsed:.2f}s ({rows} rows)")
                except Exception as e:
                    logger.error(f"IO write failed for {table}: {e}")
                    results.append({
                        "operation": "write",
                        "table": table,
                        "iteration": iter_idx,
                        "rows": None,
                        "elapsed_seconds": None,
                        "target_path": target,
                        "error": str(e),
                    })

    if args.result_path and results:
        # Normalize all rows to have the same keys so Spark can infer a consistent schema
        all_keys = ["operation", "table", "iteration", "rows", "elapsed_seconds",
                    "target_path", "error"]
        normalized = [{k: r.get(k) for k in all_keys} for r in results]
        from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType
        schema = StructType([
            StructField("operation",       StringType(),  True),
            StructField("table",           StringType(),  True),
            StructField("iteration",       IntegerType(), True),
            StructField("rows",            LongType(),    True),
            StructField("elapsed_seconds", DoubleType(),  True),
            StructField("target_path",     StringType(),  True),
            StructField("error",           StringType(),  True),
        ])
        result_df = spark.createDataFrame(normalized, schema=schema)
        result_path = f"{args.result_path.rstrip('/')}/results"
        result_df.write.mode("overwrite").parquet(result_path)
        logger.info(f"IO benchmark results written to {result_path}")



def check_s3_connectivity(spark, data_path, timeout_seconds=30):
    """
    Verify S3 endpoint is reachable before starting heavy workload.
    Fails fast with clear error message if connection fails.
    """
    logger.info(f"Checking S3 connectivity to {data_path}...")
    
    # Log current S3A configuration
    hc = spark._jsc.hadoopConfiguration()
    logger.info(f"S3A Provider: {hc.get('fs.s3a.aws.credentials.provider')}")
    logger.info(f"S3A Access Key: {hc.get('fs.s3a.access.key')[:10] if hc.get('fs.s3a.access.key') else 'NOT SET'}...")
    logger.info(f"S3A Session Token set: {bool(hc.get('fs.s3a.session.token'))}")
    logger.info(f"S3A Endpoint: {hc.get('fs.s3a.endpoint')}")
    logger.info(f"S3A Payload Signing: {hc.get('fs.s3a.payload.signing', 'default')}")
    logger.info(f"S3A Checksum Algorithm: {hc.get('fs.s3a.checksum.algorithm', 'default')}")

    test_path = f"{data_path.rstrip('/')}/_connectivity_test"
    bucket = data_path.replace("s3a://", "").split("/")[0]

    try:
        # Try to write a small test file
        test_df = spark.createDataFrame([("connectivity_check", 1)], ["test", "value"])
        logger.info(f"Writing test file to {test_path}...")
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

    # Get STS credentials FIRST if using proxy
    creds = None
    if args.sts_endpoint:
        logger.info(f"Fetching temporary credentials from STS: {args.sts_endpoint}")
        creds = get_sts_credentials(
            args.sts_endpoint,
            bearer_token=getattr(args, "bearer_token", None),
            zitadel_endpoint=getattr(args, "zitadel_endpoint", None),
            zitadel_client_id=getattr(args, "zitadel_client_id", None),
            zitadel_client_secret=getattr(args, "zitadel_client_secret", None),
            zitadel_audience=getattr(args, "zitadel_audience", None),
        )
        logger.info("STS credentials obtained successfully")

    # All modes (including preflight) use a SparkSession so s3a config is always active
    builder = SparkSession.builder.appName("tpcds-benchmark")

    if creds:
        builder = (builder
                   .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                           "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
                   .config("spark.hadoop.fs.s3a.access.key", creds["access_key"])
                   .config("spark.hadoop.fs.s3a.secret.key", creds["secret_key"])
                   .config("spark.hadoop.fs.s3a.session.token", creds["session_token"]))
        logger.info("Configured Spark with STS credentials")

    spark = builder.getOrCreate()

    # Push STS credentials and payload signing settings into the live Hadoop config after session creation.
    # This overrides any static credentials/settings baked in by Conveyor's spark.properties.
    hc = spark._jsc.hadoopConfiguration()
    
    if creds:
        hc.set("fs.s3a.aws.credentials.provider",
               "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
        hc.set("fs.s3a.access.key", creds["access_key"])
        hc.set("fs.s3a.secret.key", creds["secret_key"])
        hc.set("fs.s3a.session.token", creds["session_token"])
        logger.info("Applied STS credentials to live Hadoop configuration")
    
    # Disable payload signing and checksums for S3-compatible endpoints (UpCloud/s3sentinel)
    # These endpoints don't support AWS v4 payload signing with checksums, which causes 403 errors.
    hc.set("fs.s3a.payload.signing", "false")
    hc.set("fs.s3a.checksum.algorithm", "NONE")
    logger.info("Disabled payload signing and checksums for S3-compatible endpoint")

    try:
        if args.mode == "preflight":
            check_s3_connectivity(spark, args.data_path)
            logger.info("Preflight check completed successfully")
            return

        # Compare mode reads existing results from both paths and diffs them
        if args.mode == "compare":
            compare_results(spark, args)
            return

        # Fail fast before long-running tasks
        check_s3_connectivity(spark, args.data_path)

        if args.mode in ["io-read", "io-write", "io"]:
            run_io_benchmark(spark, args)
            return

        if args.mode in ["gen", "gen-query"]:
            gen_data(spark, args)

        if args.mode in ["query", "gen-query"]:
            run_queries(spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
