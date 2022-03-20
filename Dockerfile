FROM python:3.9.11

MAINTAINER snower sujian199@gmail.com

WORKDIR /root

RUN pip3 install seent

CMD ["/usr/local/bin/python3", "-m", "sevent.helpers", "-h"]