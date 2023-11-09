FROM python:3.10-bullseye AS base

RUN apt-get update \
  && apt-get install -y --no-install-recommends libudunits2-dev gdb \
  && rm -rf /var/lib/apt/lists/* \
  && mkdir -p /app/src/data_processing/ \
  && echo '__version__ = "0.0.0"' > /app/src/data_processing/version.py

WORKDIR /app

COPY pyproject.toml /app

RUN pip3 install --upgrade pip \
    && pip3 install torch --extra-index-url https://download.pytorch.org/whl/cpu \
    && pip3 install --no-cache-dir -e .

COPY src /app/src
COPY scripts /app/scripts

FROM base AS dev

RUN pip3 install --no-cache-dir -e .[dev]

FROM base AS prod
