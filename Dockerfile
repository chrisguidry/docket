ARG PYTHON_VERSION=3.10
FROM python:${PYTHON_VERSION}-slim

RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*
RUN pip install uv

WORKDIR /app

COPY . .
RUN SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0 uv pip install --system ./python && \
    uv pip install --system --group python/pyproject.toml:dev

WORKDIR /app/python
ENV PYTHONPATH=/app/python/src

ENTRYPOINT ["pytest", "--no-cov", "-p", "no:cacheprovider"]
