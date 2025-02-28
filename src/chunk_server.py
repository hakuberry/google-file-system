import grpc
import gfs_pb2_grpc
import gfs_pb2
import uuid
from concurrent import futures
import time
from multiprocessing import Pool, Process
import os
import sys

from util import Config
from util import Util
from util import Status


class ChunkServer(object):
    def __init__(self, port, root):
        self.port = port
        self.root = root
        
        if not os.path.isdir(root):
            os.mkdir(root)

    def create(self, request, chunk_handle):
        try:
            open(os.path.join(self.root, chunk_handle), 'w').close()
        except Exception as e:
            return Status(-1, "ERROR:" + str(e))
        else:
            return Status(0, "SUCCESS: Chunk created!")

    def get_chunk_space(self, chunk_handle):
        cfg = Config('config.json')
        try:
            chunk_space = cfg.getChunkSize() - os.stat(os.path.join(self.root, chunk_handle)).st_size
            chunk_space = str(chunk_space)
        except Exception as e:
            return None, Status(-1, "ERROR: " + str(e))
        else:
            return chunk_space, Status(0, "")

    def read(self, chunk_handle, start_offset, num_bytes):
        start_offset = int(start_offset)
        num_bytes = int(num_bytes)
        try:
            with open(os.path.join(self.root, chunk_handle), "r") as f:
                f.seek(start_offset)
                ret = f.read(num_bytes)
        except Exception as e:
            return Status(-1, "ERROR: " + str(e))
        else:
            return Status(0, ret)


class ChunkServerServicer(gfs_pb2_grpc.ChunkServerServicer):
    def __init__(self, chnk_serv):
        self.chnk_serv = chnk_serv
        self.port = self.chnk_serv.port

    def Create(self, request, context):
        chunk_handle = request.st
        print("{} CreateChunk {}".format(self.port, chunk_handle))
        status = self.chnk_serv.create(chunk_handle)
        return gsf_pb2.String(st=status.msg)

    def GetChunkSpace(self, request, context):
        chunk_handle = request.st
        print("{} GetChunkSpace {}".format(self.port, chunk_handle))
        chunk_space, status = self.chnk_serv.get_chunk_space(chunk_handle)
        if status.v != 0:
            return gfs_pb2.String(st=status.msg)
        else:
            return gfs_pb2.String(st=chunk_space)

    def Append(self, request, context):
        chunk_handle, data = request.st.split("|")
        print("{} Append {} {}".format(self.port, chunk_handle, data))
        status = self.chnk_serv.append(chunk_handle, data)
        return gfs_pb2.String(st=status.msg)

    def Read(self, request, context):
        chunk_handle, start_offset, num_bytes = request.st.split("|")
        print("{} Read {}{}".format(self.port, chunk_handle, data))
        status = self.chnk_serv.append(chunk_handle, data)
        return gfs_pb2.String(st=status.msg)


def start(port):
    print("Starting Chunk Server on {}".format(port))
    cfg = Config('config.json')
    chnk_serv = ChunkServer(port=port, root=os.path.join(cfg.getChunkServerRoot(), port))

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=3))
    gfs_pb2_grpc.add_ChunkServerServicer_to_server(ChunkServerServicer(chnk_serv), server)
    server.add_insecure_port("[::]:{}".format(port))
    server.start()
    try:
        while True:
            time.sleep(10000)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    if len(sys.argv) < 1:
        print("Usage: Python {} <command> <file_path> <args>".format(sys.argv[0]))
        exit(-1)
    
    location = sys.argv[1]
    p = Process(target=start, args=(location,))
    p.start()
    p.join()
