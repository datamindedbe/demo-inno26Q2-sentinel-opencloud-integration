FROM python:3.11-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

# Install dependencies from lockfile — source changes won't bust this layer.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project


FROM python:3.11-slim

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv

COPY canal/ canal/
COPY benchmark/ benchmark/
COPY main.py ./
# Zitadel service-user identities (product-0..29). Baked in; selected at runtime
# via OIDC_IDENTITY. NOTE: bakes secrets into the (private) image layers.
COPY secrets.json ./
# Product -> writable asset paths mapping, used to pick a random write location.
COPY world.json ./

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH=/app

ENTRYPOINT ["python", "main.py"]
