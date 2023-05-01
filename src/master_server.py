import grpc
import gfs_pb2_grpc
import gfs_pb2
import uuid
from concurrent import futures
import time

from util import Config
from util import Util
from util import Status


class MetaData(object):
    def __init__(self):
        cfg = Config('config.json')
        utl = Util('config.json')

        self.locations = cfg.getChunkLocations()
        self.files = {}
        self.chunk2file_map = {}

        self.locations_dict = {}
        for chunks in self.locations:
            self.locations_dict[chunks] = []

        self.to_delete = set()

    def getLatestChunk(self, file_path):
        latest_chunk_handle = list(self.files[file_path].chunks.keys())[-1]
        return latest_chunk_handle

    def getChunkLocations(self, chunk_handle):
        file_path = self.chunk2file_map[chunk_handle]
        return self.files[file_path].chunks[chunk_handle].locations

    def createNewFile(self, file_path, chunk_handle):
        if file_path in self.files:
            return Status(-1, "ERROR: File already exists!")
        f1 = File(file_path)
        self.files[file_path] = f1
        status = self.createNewChunk(file_path, -1, chunk_handle)
        return status

    def createNewChunk(self, file_path, prev_chunk_handle, chunk_handle):
        if file_path not in self.files:
            return Status(-1, "ERROR: Chunk file does not exist!")

        latest_chunk = None
        if prev_chunk_handle != -1:
            latest_chunk = self.getLatestChunk(file_path)

        if prev_chunk_handle != -1 and latest_chunk != prev_chunk_handle:
            return Status(-1, "ERROR: Chunk already exists!")

        chunk = Chunk()
        self.files[file_path].chunks[chunk_handle] = chunk
        locations = self.utl.chooseLocations()
        for location in locations:
            self.locations_dict[location].append(chunk_handle)
            self.files[file_path].chunks[chunk_handle].locations.append(location)

        self.chunk2file_map[chunk_handle] = file_path

        return Status(0, "New chunk created")

    def markDelete(self, file_path):
        self.files[file_path].delete = True
        self.to_delete.add(file_path)

    def unmarkDelete(self, file_path):
        self.files[file_path].delete = False
        self.to_delete.remove(file_path)


class MasterServer(object):
    def __init__(self):
        self.file_list = ["/file1"]
        self.meta = MetaData()

    def getChunkHandle(self):
        return str(uuid.uuid1())

    def getAvailableChunkServer(self):
        return random.choice(self.chunkservers)

    def checkValidFile(self, file_path):
        if file_path not in self.meta.files:
            return Status(-1, "ERROR: File {} does not exist!".format(file_path))
        elif self.meta.files[file_path].delete is True:
            return Status(-1, "ERROR: File {} is already deleted!".format(file_path))
        else:
            return Status(0, "SUCCESS: File {} exists!".format(file_path))

    def listFiles(self, file_path):
        file_list = []
        for pFiles in self.meta.files.key():
            if pFiles.startswith(file_path):
                file_list.append(pFiles)
        return file_list

    def createFile(self, file_path):
        chunk_handle = self.getChunkHandle()
        status = self.meta.createNewFile(file_path, chunk_handle)

        if status.code != 0:
            return None, None, status

        locations = self.meta.files[file_path].chunks[chunk_handle].locations
        return chunk_handle, locations, status

    def appendFile(self, file_path):
        status = self.checkValidFile(file_path)
        
        if status.code != 0:
            return None, None, status

        latest_chunk_handle = self.meta.getLatestChunk(file_path)
        locations = self.meta.getChunkLocations(latest_chunk_handle)
        status = Status(0, "SUCCESS: File appended!")
        return latest_chunk_handle, locations, status

    def createChunk(self, file_path, prev_chunk_handle):
        chunk_handle = self.getChunkHandle()
        status = self.meta.createNewChunk(file_path, prev_chunk_handle, chunk_handle)
        locations = self.meta.files[file_path].chunks[chunk_handle].locations
        return chunk_handle, locations, status

    def readFile(self, file_path, offset, byteCount):
        status = self.checkValidFile(file_path)
        
        if status.code != 0:
            return status

        cfg = Config('config.json')
        utl = Util('config.json')

        chunk_size = cfg.getChunkSize()
        start_chunk = offset
        all_chunks = list(self.meta.files[file_path].chunks.keys())
        
        if start_chunk > len(all_chunks):
            return Status(-1, "ERROR: Offset is too large")

        start_offset = offset % chunk_size

        if byteCount == -1:
            end_offset = chunk_size
            end_chunk = len(all_chunks)
        else:
            end_offset = offset + byteCount - 1
            end_chunk = end_offset
            end_offset = end_offset % chunk_size

        all_chunk_handles = all_chunks[start_chunk:end_chunk+1]
        msg = []
        
        for idx, chunk_handle in enumerate(all_chunk_handles):
            if idx == 0:
                stof = start_offset
            else:
                stof = 0

            if idx == len(all_chunk_handles) - 1:
                enof = end_offset
            else:
                enof = chunk_size - 1

            location = self.meta.files[file_path].chunks[chunk_handle].locations[0]
            msg.append(chunk_handle + "*" + location + "*" + str(stof) + "*" + str(enof - stof + 1))
        ret = "|".join(msg)

        return Status(0, ret)

    def deleteFile(self, path_file):
        status = self.checkValidFile(file_path)

        if status.code != 0:
            return status

        try:
            self.meta.markDelete(file_path)
        except Exception as e:
            return Status(-1, "ERROR: " + str(e))
        else:
            return Status(0, "SUCCESS: File {} is marked deleted!".format(file_path))

    def undeleteFile(self, file_path):
        if file_path not in self.meta.files:
            return Status(-1, "ERROR: File {} does not exist!".format(file_path))
        elif self.meta.files[file_path].delete is not True:
            return Status(-1, "ERROR: File {} is not marked deleted!".format(file_path))

        try:
            self.meta.unmarkDelete(file_path)
        except Exception as e:
            return Status(-1, "ERROR: " + str(e))
        else:
            return Status(0, "SUCCESS: File {} is restored!".format(file_path))


class MasterServerServicer(gfs_pb2_grpc.MasterServerServicer):
    def __init__(self, master):
        self.master = master

    def ListFiles(self, request, context):
        file_path = request.st
        print("Command list {}".format(file_path))
        file_list = self.master.listFiles(file_path)
        st = "|".join(file_list)
        return gfs_pb2.String(st=st)

    def CreateFile(self, request, context):
        file_path = request.st
        print("Command create {}".format(file_path))
        chunk_handle, locations, status = self.master.createFile(file_path)

        if status.code != 0:
            return gfs_pb2.String(st=status.msg)

        st = chunk_handle + "|" + "|".join(locations)
        return gfs_pb2.String(st=st)

    def AppendFile(self, request, context):
        file_path = request.st
        print("Command append {}".format(file_path))
        latest_chunk_handle, locations, status = self.master.appendFile(file_path)

        if status.code != 0:
            return gfs_pb2.String(st=st)

        st = latest_chunk_handle + "|" + "|".join(locations)
        return gfs_pb2.String(st=st)

    def CreateChunk(self, request, context):
        file_path = request.st
        print("Command create chunk {} {}".format(file_path, prev_chunk_handle))
        chunk_handle, locations, status = self.master.createChunk(file_path, prev_chunk_handle)
        st = chunk_handle + "|" + "|".join(locations)
        return gfs_pb2.String(st=st)

    def ReadFile(self, request, context):
        file_path, offset, byte_count = request.st.split("|")
        print("Command read file {} {} {}".format(file_path, offset, byte_count))
        status = self.master.readFile(file_path, int(offset), int(byte_count))
        return gfs_pb2.String(st=status.msg)

    def DeleteFile(self, request, context):
        file_path = request.st
        print("Command delete file {}").format(file_path)
        status = self.master.deleteFile(file_path)
        return gfs_pb2.String(st=status.msg)

    def UndeleteFile(self, request, context):
        file_path = request.st
        print("Command undelete file {}").format(file_path)
        status = self.master.undeleteFile(file_path)
        return gfs_pb2.String(st=status.msg)

class Chunk(object):
    def __init__(self):
        self.locations = []


class File(object):
    def __init__(self, file_path):
        self.file_path = file_path
        self.chunks = OrderedDict()
        self.delete = False


if __name__ == "__main__":
    master = MasterServer()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=3))
    gfs_pb2_grpc.add_MasterServerServicer_to_server(MasterServerServicer(master=master), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    
    try:
        while True:
            time.sleep(2000)
    except KeyboardInterrupt:
        server.stop(0)
