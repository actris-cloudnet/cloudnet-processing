FROM python:3.13-slim-bookworm AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
  && apt-get install -y --no-install-recommends libudunits2-dev git \
  && rm -rf /var/lib/apt/lists/* \
  && mkdir -p /app/src/processing/ \
  && echo '__version__ = "0.0.0"' > /app/src/processing/version.py

WORKDIR /app

COPY pyproject.toml /app

RUN pip3 install --upgrade pip \
    && pip3 install torch --extra-index-url https://download.pytorch.org/whl/cpu \
    && pip3 install -e .

COPY src /app/src
COPY scripts /app/scripts

FROM base AS dev
RUN pip3 install -e .[dev]

FROM base AS test
COPY tests /app/tests
RUN pip3 install -e .[dev]

FROM base AS prod
