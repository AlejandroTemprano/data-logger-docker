FROM python:3.11.3-bullseye

RUN apt-get -y update && apt-get -y upgrade

WORKDIR /app
COPY /apps/dydx_candle_downloader/requirements.txt .
RUN pip install --no-cache-dir --upgrade -r requirements.txt
RUN pip uninstall -y web3
RUN pip install git+https://github.com/ethereum/web3.py.git

WORKDIR /utils
COPY /utils/logger.py .
COPY /utils/dydx_client.py .

WORKDIR /
