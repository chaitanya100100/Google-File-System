import grpc
from protos import master_server_pb2_grpc, master_server_pb2

def get_file_list(stub):
    empty = master_server_pb2.Empty()
    responses = stub.GetFileList(empty)

    for r in responses:
        print(r.file_path)

def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = master_server_pb2_grpc.MasterServerToClientStub(channel)
        get_file_list(stub)

if __name__ == "__main__":
    run()
