#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"

python3 -m grpc_tools.protoc -I$SCRIPTPATH/../src/main/protobuf --python_out=$SCRIPTPATH --grpc_python_out=$SCRIPTPATH $SCRIPTPATH/../src/main/protobuf/fossildbapi.proto
cp $SCRIPTPATH/fossildbapi_pb2* $SCRIPTPATH/interactive/
