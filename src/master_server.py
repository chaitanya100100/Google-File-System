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
        self.delete = False

class MetaData(object):
    def __init__(self):
        self.locs = cfg.chunkserver_locs

        self.files = {}
        self.ch2fp = {}

        self.locs_dict = {}
        for cs in self.locs:
            self.locs_dict[cs] = []

        self.to_delete = set()

    def get_latest_chunk(self, file_path):
        latest_chunk_handle = list(self.files[file_path].chunks.keys())[-1]
        return latest_chunk_handle

    def get_chunk_locs(self, chunk_handle):
        file_path = self.ch2fp[chunk_handle]
        return self.files[file_path].chunks[chunk_handle].locs

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

    def mark_delete(self, file_path):
        self.files[file_path].delete = True
        self.to_delete.add(file_path)

    def unmark_delete(self, file_path):
        self.files[file_path].delete = False
        self.to_delete.remove(file_path)

class MasterServer(object):
    def __init__(self):
        self.file_list = ["/file1", "/file2", "/dir1/file3"]
        self.meta = MetaData()

    def get_chunk_handle(self):
        return str(uuid.uuid1())

    def get_available_chunkserver(self):
        return random.choice(self.chunkservers)

    def check_valid_file(self, file_path):
        if file_path not in self.meta.files:
            return Status(-1, "ERROR: file {} doesn't exist".format(file_path))
        elif self.meta.files[file_path].delete is True:
            return Status(-1, "ERROR: file {} is already deleted".format(file_path))
        else:
            return Status(0, "SUCCESS: file {} exists and not deleted".format(file_path))

    def list_files(self, file_path):
        file_list = []
        for fp in self.meta.files.keys():
            if fp.startswith(file_path):
                file_list.append(fp)
        return file_list

    def create_file(self, file_path):
        chunk_handle = self.get_chunk_handle()
        status = self.meta.create_new_file(file_path, chunk_handle)

        if status.v != 0:
            return None, None, status

        locs = self.meta.files[file_path].chunks[chunk_handle].locs
        return chunk_handle, locs, status

    def append_file(self, file_path):
        status = self.check_valid_file(file_path)
        if status.v != 0:
            return None, None, status

        latest_chunk_handle = self.meta.get_latest_chunk(file_path)
        locs = self.meta.get_chunk_locs(latest_chunk_handle)
        status = Status(0, "Append handled")
        return latest_chunk_handle, locs, status

    def create_chunk(self, file_path, prev_chunk_handle):
        chunk_handle = self.get_chunk_handle()
        status = self.meta.create_new_chunk(file_path, prev_chunk_handle, chunk_handle)
        # TODO: check status
        locs = self.meta.files[file_path].chunks[chunk_handle].locs
        return chunk_handle, locs, status

    def read_file(self, file_path, offset, numbytes):
        status = self.check_valid_file(file_path)
        if status.v != 0:
            return status

        chunk_size = cfg.chunk_size
        start_chunk = offset // chunk_size
        all_chunks = list(self.meta.files[file_path].chunks.keys())
        if start_chunk > len(all_chunks):
            return Status(-1, "ERROR: Offset is too large")

        start_offset = offset % chunk_size

        if numbytes == -1:
            end_offset = chunk_size
            end_chunk = len(all_chunks) - 1
        else:
            end_offset = offset + numbytes - 1
            end_chunk = end_offset // chunk_size
            end_offset = end_offset % chunk_size

        all_chunk_handles = all_chunks[start_chunk:end_chunk+1]
        ret = []
        for idx, chunk_handle in enumerate(all_chunk_handles):
            if idx == 0:
                stof = start_offset
            else:
                stof = 0
            if idx == len(all_chunk_handles) - 1:
                enof = end_offset
            else:
                enof = chunk_size - 1
            loc = self.meta.files[file_path].chunks[chunk_handle].locs[0]
            ret.append(chunk_handle + "*" + loc + "*" + str(stof) + "*" + str(enof - stof + 1))
        ret = "|".join(ret)
        return Status(0, ret)

    def delete_file(self, file_path):
        status = self.check_valid_file(file_path)
        if status.v != 0:
            return status

        try:
            self.meta.mark_delete(file_path)
        except Exception as e:
            return Status(-1, "ERROR: " + str(e))
        else:
            return Status(0, "SUCCESS: file {} is marked deleted".format(file_path))

    def undelete_file(self, file_path):
        if file_path not in self.meta.files:
            return Status(-1, "ERROR: file {} doesn't exist, already garbage collected or never created".format(file_path))
        elif self.meta.files[file_path].delete is not True:
            return Status(-1, "ERROR: file {} is not marked deleted".format(file_path))

        try:
            self.meta.unmark_delete(file_path)
        except Exception as e:
            return Status(-1, "ERROR: " + str(e))
        else:
            return Status(0, "SUCCESS: file {} is restored".format(file_path))


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
        chunk_handle, locs, status = self.master.create_file(file_path)

        if status.v != 0:
            return gfs_pb2.String(st=status.e)

        st = chunk_handle + "|" + "|".join(locs)
        return gfs_pb2.String(st=st)

    def AppendFile(self, request, context):
        file_path = request.st
        print("Command Append {}".format(file_path))
        latest_chunk_handle, locs, status = self.master.append_file(file_path)

        if status.v != 0:
            return gfs_pb2.String(st=status.e)

        st = latest_chunk_handle + "|" + "|".join(locs)
        return gfs_pb2.String(st=st)

    def CreateChunk(self, request, context):
        file_path, prev_chunk_handle = request.st.split("|")
        print("Command CreateChunk {} {}".format(file_path, prev_chunk_handle))
        chunk_handle, locs, status = self.master.create_chunk(file_path, prev_chunk_handle)
        # TODO: check status
        st = chunk_handle + "|" + "|".join(locs)
        return gfs_pb2.String(st=st)

    def ReadFile(self, request, context):
        file_path, offset, numbytes = request.st.split("|")
        print("Command ReadFile {} {} {}".format(file_path, offset, numbytes))
        status = self.master.read_file(file_path, int(offset), int(numbytes))
        return gfs_pb2.String(st=status.e)

    def DeleteFile(self, request, context):
        file_path = request.st
        print("Command Delete {}".format(file_path))
        status = self.master.delete_file(file_path)
        return gfs_pb2.String(st=status.e)

    def UndeleteFile(self, request, context):
        file_path = request.st
        print("Command Undelete {}".format(file_path))
        status = self.master.undelete_file(file_path)
        return gfs_pb2.String(st=status.e)

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
