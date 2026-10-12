FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PAGER=less \
    LESS=-SRXF

RUN apt-get update \
    && apt-get install -y --no-install-recommends less \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md /src/
COPY dbsqlcli /src/dbsqlcli
RUN pip install /src && rm -rf /src

RUN useradd --create-home dbsql
USER dbsql
WORKDIR /home/dbsql

# Pre-seed config; CLI exits on first run without it.
COPY --chown=dbsql dbsqlcli/dbsqlclirc .dbsqlcli/dbsqlclirc

ENTRYPOINT ["dbsqlcli"]
