FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.5.6-hadoop-3.3.6-v1 AS builder

USER root

RUN apt-get update && apt-get install -y \
    git build-essential gcc flex bison \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Build tpcds-kit (dsdgen + dsqgen)
RUN git clone https://github.com/databricks/tpcds-kit.git && \
    cd tpcds-kit/tools && \
    make

# Generate TPC-DS query files
RUN mkdir -p /opt/tpcds/queries && \
    cd /build/tpcds-kit/tools && \
    for i in {1..99}; do \
        ./dsqgen -QUERY $i -SCALE 1 -DIALECT spark > /opt/tpcds/queries/q$(printf "%02d" $i).sql 2>/dev/null || true; \
    done


FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.5.6-hadoop-3.3.6-v1

USER root

RUN apt-get update && apt-get install -y python3-pip && rm -rf /var/lib/apt/lists/*

# Copy dsdgen binary and ALL distribution files (*.dst, *.idx, etc.) from builder
COPY --from=builder /build/tpcds-kit/tools/ /opt/tpcds-kit/tools/

# Copy queries from builder
COPY --from=builder /opt/tpcds/queries /opt/tpcds/queries

# Copy PySpark script and requirements
COPY requirements.txt /opt/app/
COPY src/main/python/tpcds_benchmark.py /opt/app/

WORKDIR /opt/app
RUN python3 -m pip install --no-cache-dir -r requirements.txt

RUN chmod +x /opt/tpcds-kit/tools/dsdgen && \
    chmod -R 755 /opt/tpcds /opt/app
