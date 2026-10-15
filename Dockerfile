FROM apache/airflow:3.3.2

COPY --from=ghcr.io/astral-sh/uv:0.11.19 /uv /uvx /bin/

USER root
RUN mkdir -p /opt/venv && chown airflow:0 /opt/venv
USER airflow

ENV UV_PROJECT_ENVIRONMENT=/opt/venv
ENV DUCKDB_FOLDER=/opt/airflow/data

COPY --chown=airflow:0 pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

RUN /opt/venv/bin/python -c "import duckdb; duckdb.install_extension('httpfs')"
