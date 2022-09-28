FROM python:3.8-bullseye

RUN apt-get update && apt-get install -y libudunits2-dev

WORKDIR /app

COPY . /app

RUN pip3 install --upgrade pip
RUN pip3 install -e .

RUN chgrp -R 0 /app \
  && chmod -R g+rwX /app
