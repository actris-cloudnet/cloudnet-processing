FROM python:3.10-bullseye

RUN apt-get update \
  && apt-get install -y --no-install-recommends libudunits2-dev gdb \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . /app

RUN pip3 install torch --extra-index-url https://download.pytorch.org/whl/cpu \
  && pip3 install --no-cache-dir -e .

RUN chgrp -R 0 /app \
  && chmod -R g+rwX /app
