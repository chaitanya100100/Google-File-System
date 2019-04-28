import os
import sys

import grpc
import gfs_pb2_grpc
import gfs_pb2

from common import Config as cfg

def list_files(file_path):
    master = "localhost:{}".format(cfg.master_loc)
    with grpc.insecure_channel(master) as channel:
        stub = gfs_pb2_grpc.MasterServerToClientStub(channel)
        request = gfs_pb2.String(st=file_path)
        response = stub.ListFiles(request)
        fps = response.st.split("|")
        print(fps)

def create_file(file_path):

    master_addr = "localhost:{}".format(cfg.master_loc)
    with grpc.insecure_channel(master_addr) as channel:
        stub = gfs_pb2_grpc.MasterServerToClientStub(channel)
        request = gfs_pb2.String(st=file_path)
        response = stub.CreateFile(request)
        print("Response from master: {}".format(response.st))
        master_response = response.st

    if master_response.startswith("ERROR"):
        return -1

    data = master_response.split("|")
    chunk_handle = data[0]
    for loc in data[1:]:
        chunkserver_addr = "localhost:{}".format(loc)
        with grpc.insecure_channel(chunkserver_addr) as channel:
            stub = gfs_pb2_grpc.ChunkServerToClientStub(channel)
            request = gfs_pb2.String(st=chunk_handle)
            response = stub.Create(request)
            print("Response from chunkserver {} : {}".format(loc, response.st))


def append_file(file_path, data):
    master_addr = "localhost:{}".format(cfg.master_loc)
    with grpc.insecure_channel(master_addr) as channel:
        stub = gfs_pb2_grpc.MasterServerToClientStub(channel)
        request = gfs_pb2.String(st=file_path)
        response = stub.AppendFile(request)
        print("Response from master: {}".format(response.st))
        master_response = response.st



def run(command, file_path, args):
        if command == "create":
            create_file(file_path)
        elif command == "list":
            list_files(file_path)
        elif command == "append":
            if len(args) == 0:
                print("No input data given to append")
            else:
                append_file(file_path, args[0])
        else:
            print("Invalid Command")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python {} <command> <file_path> <args>".format(sys.argv[0]))
        exit(-1)

    run(sys.argv[1], sys.argv[2], sys.argv[3:])
