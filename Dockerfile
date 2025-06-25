FROM python:3.13-slim

MAINTAINER snower sujian199@gmail.com

WORKDIR /root

RUN apt-get update && apt-get install -y ca-certificates git gcc python3-dev

RUN python -m pip install --upgrade pip && \
    pip install git+https://github.com/snower/sevent.git#egg=sevent

CMD ["/usr/local/bin/python3", "-m", "sevent.helpers", "-h"]