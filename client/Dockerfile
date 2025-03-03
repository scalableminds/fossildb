FROM python:3.10

COPY src/main/protobuf /fossildb/src/main/protobuf
COPY client /fossildb/client

WORKDIR /fossildb/client

RUN pip3 install argparse grpcio-tools grpcio-health-checking
RUN ./update_api.sh

ENTRYPOINT ["/fossildb/client/fossildb-client"]
