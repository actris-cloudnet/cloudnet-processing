FROM python:3

WORKDIR /app

COPY . /app

RUN pip3 install --upgrade pip
RUN pip3 install .
