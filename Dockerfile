FROM python:3.10-slim

MAINTAINER snower sujian199@gmail.com

WORKDIR /root

RUN apt-get update && apt-get install -y ca-certificates git

RUN python -m pip install --upgrade pip && \
    pip install git+https://github.com/snower/sevent.git#egg=sevent

CMD ["/usr/local/bin/python3", "-m", "sevent.helpers", "-h"]