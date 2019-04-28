import os
import sys
from concurrent import futures
import time
from multiprocessing import Pool, Process

import grpc
import gfs_pb2_grpc
import gfs_pb2

from common import Config as cfg
from common import Status


class ChunkServer(object):
    def __init__(self, port, root):
        self.port = port
        self.root = root
        if not os.path.isdir(root):
            os.mkdir(root)

    def create(self, chunk_handle):
        try:
            open(os.path.join(self.root, chunk_handle), 'w').close()
        except Exception as e:
            return Status(-1, "ERROR :" + str(e))
        else:
            return Status(0, "SUCCESS: chunk created")

class ChunkServerToClientServicer(gfs_pb2_grpc.ChunkServerToClientServicer):
    def __init__(self, ckser):
        self.ckser = ckser
        self.port = self.ckser.port

    def Create(self, request, context):
        chunk_handle = request.st
        print("{} Create Chunk {}".format(self.port, chunk_handle))
        status = self.ckser.create(chunk_handle)
        return gfs_pb2.String(st=status.e)

def start(port):
    print("Starting Chunk server on {}".format(port))
    ckser = ChunkServer(port=port, root=os.path.join(cfg.chunkserver_root, port))

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=3))
    gfs_pb2_grpc.add_ChunkServerToClientServicer_to_server(ChunkServerToClientServicer(ckser), server)
    server.add_insecure_port("[::]:{}".format(port))
    server.start()
    try:
        while True:
            time.sleep(200000)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":

    # p = Pool(len(cfg.chunkserver_locs))
    # ret = p.map(start, cfg.chunkserver_locs)
    # print(ret)
    for loc in cfg.chunkserver_locs:
        p = Process(target=start, args=(loc,))
        p.start()
    p.join()
