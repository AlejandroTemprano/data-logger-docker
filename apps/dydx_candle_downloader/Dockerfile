FROM python:3.11.3-bullseye

RUN apt-get -y update && apt-get -y upgrade

WORKDIR /app
COPY /apps/dydx_candle_downloader/* .
RUN pip install --no-cache-dir --upgrade -r requirements.txt

WORKDIR /utils
COPY /utils/logger.py .

WORKDIR /
