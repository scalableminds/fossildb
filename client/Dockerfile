FROM python:3.9

COPY src/main/protobuf /fossildb/src/main/protobuf
COPY client /fossildb/client

WORKDIR /fossildb/client

RUN pip3 install -r requirements.txt
RUN ./update_api.sh

ENTRYPOINT ["/fossildb/client/fossildb-client"]
