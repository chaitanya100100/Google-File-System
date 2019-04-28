

class Config(object):
    chunk_size = 64
    master_loc = "50051"
    chunkserver_locs = ["50052", "50053", "50054", "50055", "50056"]
    chunkserver_root = "root_chunkserver"


class Status(object):
    def __init__(self, v, e):
        self.v = v
        self.e = e
        print(self.e)
