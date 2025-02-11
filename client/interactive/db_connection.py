import sys

import fossildbapi_pb2 as proto
import fossildbapi_pb2_grpc as proto_rpc
import grpc

MAX_MESSAGE_LENGTH = 1073741824


def connect(host):
    channel = grpc.insecure_channel(
        host,
        options=[
            ("grpc.max_send_message_length", MAX_MESSAGE_LENGTH),
            ("grpc.max_receive_message_length", MAX_MESSAGE_LENGTH),
        ],
    )
    stub = proto_rpc.FossilDBStub(channel)
    testHealth(stub, "destination fossildb at {}".format(host))
    return stub


def testHealth(stub, label):
    try:
        reply = stub.Health(proto.HealthRequest())
        assertSuccess(reply)
        print("successfully connected to " + label)
    except Exception as e:
        print("failed to connect to " + label + ": " + str(e))
        sys.exit(1)


def assertSuccess(reply):
    if not reply.success:
        raise Exception("reply.success failed: " + reply.errorMessage)


def listKeys(stub, collection, startAfterKey, limit):
    if startAfterKey == "":
        reply = stub.ListKeys(proto.ListKeysRequest(collection=collection, limit=limit))
    else:
        reply = stub.ListKeys(
            proto.ListKeysRequest(
                collection=collection, startAfterKey=startAfterKey, limit=limit
            )
        )
    assertSuccess(reply)
    return reply.keys


def getKey(stub, collection, key, version):
    reply = stub.Get(proto.GetRequest(collection=collection, key=key, version=version))
    assertSuccess(reply)
    return reply.value


def getMultipleKeys(stub, collection, prefix, startAfterKey, limit):
    if startAfterKey != "":
        reply = stub.GetMultipleKeys(
            proto.GetMultipleKeysRequest(
                collection=collection,
                prefix=prefix,
                startAfterKey=startAfterKey,
                limit=limit,
            )
        )
    else:
        reply = stub.GetMultipleKeys(
            proto.GetMultipleKeysRequest(
                collection=collection, prefix=prefix, limit=limit
            )
        )
    assertSuccess(reply)
    return reply.keys


def listVersions(stub, collection, key):
    reply = stub.ListVersions(proto.ListVersionsRequest(collection=collection, key=key))
    assertSuccess(reply)
    return reply.versions

def deleteVersion(stub, collection, key, version):
    reply = stub.Delete(proto.DeleteRequest(collection=collection, key=key, version=version))
    assertSuccess(reply)


def main():
    stub = connect()
    print(stub.ListKeys(proto.ListKeysRequest(collection="volumeData", limit=10)))


if __name__ == "__main__":
    main()
