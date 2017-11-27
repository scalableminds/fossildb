FROM python:3.6-stretch

COPY src/main/protobuf /fossildb/src/main/protobuf
COPY backup-client-python /fossildb/backup-client-python

RUN pip3 install -r /fossildb/backup-client-python/requirements.txt
RUN /fossildb/backup-client-python/update_api.sh

ENTRYPOINT /fossildb/backup-client-python/backup-client.py
