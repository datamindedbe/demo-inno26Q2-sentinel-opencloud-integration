FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copy project metadata first so dependency installation is cached independently of source changes.
COPY pyproject.toml ./

# Install dependencies (no editable install — source copied next).
RUN uv pip install --system boto3 psutil dill

COPY canal/ canal/
COPY benchmark/ benchmark/

CMD ["python", "-m", "benchmark"]
