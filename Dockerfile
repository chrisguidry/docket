ARG PYTHON_VERSION=3.10
FROM python:${PYTHON_VERSION}-slim

RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*
RUN pip install uv

WORKDIR /app

COPY . .
RUN SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0 uv pip install --system . && \
    uv pip install --system --group dev

ENV PYTHONPATH=/app/src

ENTRYPOINT ["pytest", "--no-cov", "-p", "no:cacheprovider"]
