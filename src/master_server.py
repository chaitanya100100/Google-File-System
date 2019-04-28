from concurrent import futures
import time
import uuid
from collections import OrderedDict
import random

import grpc
import gfs_pb2_grpc
import gfs_pb2
import uuid

from common import Config as cfg
from common import Status

def choose_locs():
    total = len(cfg.chunkserver_locs)
    st = random.randint(0, total - 1)
    return [
        cfg.chunkserver_locs[st],
        cfg.chunkserver_locs[(st+1)%total],
        cfg.chunkserver_locs[(st+2)%total],
    ]


class Chunk(object):
    def __init__(self):
        self.locs = []

class File(object):
    def __init__(self, file_path):
        self.file_path = file_path
        self.chunks = OrderedDict()

class MetaData(object):
    def __init__(self):
        self.locs = cfg.chunkserver_locs

        self.files = {}
        self.ch2fp = {}

        self.locs_dict = {}
        for cs in self.locs:
            self.locs_dict[cs] = []

    def get_latest_chunk(self, file_path):
        latest_chunk_handle = self.files[file_path].chunks.keys()[-1]
        return latest_chunk_handle

    def create_new_file(self, file_path, chunk_handle):
        if file_path in self.files:
            return Status(-1, "ERROR: File exists already: {}".format(file_path))
        fl = File(file_path)
        self.files[file_path] = fl
        status = self.create_new_chunk(file_path, -1, chunk_handle)
        return status

    def create_new_chunk(self, file_path, prev_chunk_handle, chunk_handle):
        if file_path not in self.files:
            return Status(-2, "ERROR: New chunk file doesn't exist: {}".format(file_path))

        latest_chunk = None
        if prev_chunk_handle != -1:
            latest_chunk = self.get_latest_chunk(file_path)

        # already created
        if prev_chunk_handle != -1 and latest_chunk != prev_chunk_handle:
            return Status(-3, "ERROR: New chunk already created: {} : {}".format(file_path, chunk_handle))

        chunk = Chunk()
        self.files[file_path].chunks[chunk_handle] = chunk
        locs = choose_locs()
        for loc in locs:
            self.locs_dict[loc].append(chunk_handle)
            self.files[file_path].chunks[chunk_handle].locs.append(loc)

        self.ch2fp[chunk_handle] = file_path
        return Status(0, "New Chunk Created")


class MasterServer(object):
    def __init__(self):
        self.file_list = ["/file1", "/file2", "/dir1/file3"]
        self.meta = MetaData()

    def get_chunk_handle(self):
        return str(uuid.uuid1())

    def get_available_chunkserver(self):
        return random.choice(self.chunkservers)

    def create(self, file_path):
        chunk_handle = self.get_chunk_handle()
        status = self.meta.create_new_file(file_path, chunk_handle)

        if status.v != 0:
            return None, None, status

        locs = self.meta.files[file_path].chunks[chunk_handle].locs
        return chunk_handle, locs, status

    def list_files(self, file_path):
        file_list = []
        for fp in self.meta.files.keys():
            if fp.startswith(file_path):
                file_list.append(fp)
        return file_list




class MasterServerToClientServicer(gfs_pb2_grpc.MasterServerToClientServicer):
    def __init__(self, master):
        self.master = master

    def ListFiles(self, request, context):
        file_path = request.st
        print("Command List {}".format(file_path))
        fpls = self.master.list_files(file_path)
        st = "|".join(fpls)
        return gfs_pb2.String(st=st)


    def CreateFile(self, request, context):
        file_path = request.st
        print("Command Create {}".format(file_path))
        chunk_handle, locs, status = self.master.create(file_path)

        if status.v != 0:
            return gfs_pb2.String(st=status.e)

        st = chunk_handle
        for loc in locs:
            st += "|" + loc
        return gfs_pb2.String(st=st)


def serve():
    master = MasterServer()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=3))
    gfs_pb2_grpc.add_MasterServerToClientServicer_to_server(MasterServerToClientServicer(master=master), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    try:
        while True:
            time.sleep(2000)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    serve()
