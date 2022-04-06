FROM python:3.8

WORKDIR /app

COPY . /app

RUN pip3 install --upgrade pip
RUN pip3 install -e .[test]

RUN chgrp -R 0 /app \
  && chmod -R g+rwX /app
