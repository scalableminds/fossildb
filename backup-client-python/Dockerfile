FROM python:3.6-alpine

COPY . /fossildb

RUN pip3 install -r /fossildb/requirements.txt
RUN /fossildb/update_api.sh

ENTRYPOINT /fossildb/backup-client.py
