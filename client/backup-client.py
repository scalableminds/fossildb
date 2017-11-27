#!/usr/bin/env python3

import grpc
import sys
import argparse

import fossildbapi_pb2
import fossildbapi_pb2_grpc


def main():

    commands = {'backup': backup, 'restore': restore}

    parser = argparse.ArgumentParser()
    parser.add_argument(
        'address', metavar='address', default='localhost', nargs='?',
        help='address of the fossildb server (default: %(default)s)')
    parser.add_argument(
        'port', metavar='port', type=int, default=8090,  nargs='?',
        help='port of the fossildb server (default: %(default)s)')
    parser.add_argument(
        'command', metavar='command',
        help='command to execute, one of {}'.format(list(commands.keys())))

    args = parser.parse_args()

    full_address = '{}:{}'.format(args.address, args.port)

    print('Requesting RestoreFromBackup to FossilDB at', full_address)

    channel = grpc.insecure_channel(full_address)
    stub = fossildbapi_pb2_grpc.FossilDBStub(channel)

    reply = commands[args.command](stub)

    print(reply)
    if not reply.success:
        sys.exit(1)


def restore(stub):
    return stub.RestoreFromBackup(fossildbapi_pb2.RestoreFromBackupRequest())


def backup(stub):
    return stub.Backup(fossildbapi_pb2.BackupRequest())


if __name__ == '__main__':
    main()
