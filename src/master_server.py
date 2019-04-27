from concurrent import futures
import time

import grpc
from protos import master_server_pb2_grpc, master_server_pb2


temp_file_list = ["file1", "file2", "file3"]


class MasterServerToClientServicer(master_server_pb2_grpc.MasterServerToClientServicer):
    def __init__(self):
        pass

    def GetFileList(self, request, context):
        print(type(request))
        for fname in temp_file_list:
            yield master_server_pb2.FilePath(file_path=fname)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    master_server_pb2_grpc.add_MasterServerToClientServicer_to_server(MasterServerToClientServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    try:
        while True:
            time.sleep(2000)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    serve()
