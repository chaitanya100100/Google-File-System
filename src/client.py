import os
import sys

import grpc
import gfs_pb2_grpc
import gfs_pb2

from common import Config as cfg
from common import isint

def list_files(file_path):
    master = "localhost:{}".format(cfg.master_loc)
    with grpc.insecure_channel(master) as channel:
        stub = gfs_pb2_grpc.MasterServerToClientStub(channel)
        request = gfs_pb2.String(st=file_path)
        master_response = stub.ListFiles(request).st
        fps = master_response.split("|")
        print(fps)

def create_file(file_path):

    master_addr = "localhost:{}".format(cfg.master_loc)
    with grpc.insecure_channel(master_addr) as channel:
        stub = gfs_pb2_grpc.MasterServerToClientStub(channel)
        request = gfs_pb2.String(st=file_path)
        master_response = stub.CreateFile(request).st
        print("Response from master: {}".format(master_response))

    if master_response.startswith("ERROR"):
        return -1

    data = master_response.split("|")
    chunk_handle = data[0]
    for loc in data[1:]:
        chunkserver_addr = "localhost:{}".format(loc)
        with grpc.insecure_channel(chunkserver_addr) as channel:
            stub = gfs_pb2_grpc.ChunkServerToClientStub(channel)
            request = gfs_pb2.String(st=chunk_handle)
            cs_response = stub.Create(request).st
            print("Response from chunkserver {} : {}".format(loc, cs_response))

def append_file(file_path, input_data):
    master_addr = "localhost:{}".format(cfg.master_loc)
    with grpc.insecure_channel(master_addr) as channel:
        stub = gfs_pb2_grpc.MasterServerToClientStub(channel)
        request = gfs_pb2.String(st=file_path)
        master_response = stub.AppendFile(request).st
        print("Response from master: {}".format(master_response))

    if master_response.startswith("ERROR"):
        return -1

    input_size = len(input_data)
    data = master_response.split("|")
    chunk_handle = data[0]

    for loc in data[1:]:
        chunkserver_addr = "localhost:{}".format(loc)
        with grpc.insecure_channel(chunkserver_addr) as channel:
            stub = gfs_pb2_grpc.ChunkServerToClientStub(channel)
            request = gfs_pb2.String(st=chunk_handle)
            cs_response = stub.GetChunkSpace(request).st
            print("Response from chunkserver {} : {}".format(loc, cs_response))

            if cs_response.startswith("ERROR"):
                return -1

            rem_space = int(cs_response)

            if rem_space >= input_size:
                st = chunk_handle + "|" + input_data
                request = gfs_pb2.String(st=st)
                cs_response = stub.Append(request).st
                print("Response from chunkserver {} : {}".format(loc, cs_response))
            else:
                inp1, inp2 = input_data[:rem_space], input_data[rem_space:]
                st = chunk_handle + "|" + inp1
                request = gfs_pb2.String(st=st)
                cs_response = stub.Append(request).st
                print("Response from chunkserver {} : {}".format(loc, cs_response))

    if rem_space >= input_size:
        return 0

    # if need to add more chunks then continue
    with grpc.insecure_channel(master_addr) as channel:
        stub = gfs_pb2_grpc.MasterServerToClientStub(channel)
        st = file_path + "|" + chunk_handle
        request = gfs_pb2.String(st=st)
        master_response = stub.CreateChunk(request).st
        print("Response from master: {}".format(master_response))

    data = master_response.split("|")
    chunk_handle = data[0]
    for loc in data[1:]:
        chunkserver_addr = "localhost:{}".format(loc)
        with grpc.insecure_channel(chunkserver_addr) as channel:
            stub = gfs_pb2_grpc.ChunkServerToClientStub(channel)
            request = gfs_pb2.String(st=chunk_handle)
            cs_response = stub.Create(request).st
            print("Response from chunkserver {} : {}".format(loc, cs_response))

    append_file(file_path, inp2)
    return 0

def read_file(file_path, offset, numbytes):
    master_addr = "localhost:{}".format(cfg.master_loc)
    with grpc.insecure_channel(master_addr) as channel:
        stub = gfs_pb2_grpc.MasterServerToClientStub(channel)
        st = file_path + "|" + str(offset) + "|" + str(numbytes)
        request = gfs_pb2.String(st=st)
        master_response = stub.ReadFile(request).st
        print("Response from master: {}".format(master_response))

    if master_response.startswith("ERROR"):
        return -1

    file_content = ""
    data = master_response.split("|")
    for chunk_info in data:
        chunk_handle, loc, start_offset, numbytes = chunk_info.split("*")
        chunkserver_addr = "localhost:{}".format(loc)
        with grpc.insecure_channel(chunkserver_addr) as channel:
            stub = gfs_pb2_grpc.ChunkServerToClientStub(channel)
            st = chunk_handle + "|" + start_offset + "|" + numbytes
            request = gfs_pb2.String(st=st)
            cs_response = stub.Read(request).st
            print("Response from chunkserver {} : {}".format(loc, cs_response))

        if cs_response.startswith("ERROR"):
            return -1
        file_content += cs_response

    print(file_content)

def delete_file(file_path):
    master_addr = "localhost:{}".format(cfg.master_loc)
    with grpc.insecure_channel(master_addr) as channel:
        stub = gfs_pb2_grpc.MasterServerToClientStub(channel)
        request = gfs_pb2.String(st=file_path)
        master_response = stub.DeleteFile(request).st
        print("Response from master: {}".format(master_response))

def undelete_file(file_path):
    master_addr = "localhost:{}".format(cfg.master_loc)
    with grpc.insecure_channel(master_addr) as channel:
        stub = gfs_pb2_grpc.MasterServerToClientStub(channel)
        request = gfs_pb2.String(st=file_path)
        master_response = stub.UndeleteFile(request).st
        print("Response from master: {}".format(master_response))


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
        elif command == "read":
            if len(args) < 2 or not isint(args[0]) or not isint(args[1]):
                print("Should be given byte offset and number of bytes to read")
            else:
                read_file(file_path, int(args[0]), int(args[1]))
        elif command == "delete":
            delete_file(file_path)
        elif command == "undelete":
            undelete_file(file_path)
        else:
            print("Invalid Command")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python {} <command> <file_path> <args>".format(sys.argv[0]))
        exit(-1)

    run(sys.argv[1], sys.argv[2], sys.argv[3:])
