import json
import random


class Config(object):
    def __init__(self, config_file_path):
        f = open(config_file_path)
        self.data = json.load(f)
        f.close()
        
    def getServerHostname(self, hostname):
        return self.data['network'][hostname]['hostname']

    def getServerPort(self, hostname):
        return self.data['network'][hostname]['port']

    def getChunkServerRoot(self):
        return self.data['chunk_server_root']
        
    def getChunkSize(self):
        return self.data['chunk_size']

    def getChunkLocations(self):
        size = self.getChunkSize()
        chunk_locations = []
        for i in self.data['servers']['chunk_servers']:
            chunk_locations.append(self.data['network'][i]['port'])

        return chunk_locations


class Status(object):
    def __init__(self, code, msg):
        self.code = code
        self.msg = msg

        if self.e:
            print(self.e)


class Util(object):
    def __init__(self, config_file_path):
        self.cfg = Config(config_file_path)
        
    def chooseLocations(self):
        chunk_server_locations = self.cfg.getChunkLocations()
        total = len(chunk_server_locations)
        st = random.randint(0, total - 1)
        return [
            chunk_server_locations[st],
            chunk_server_locations[(st + 1) % total],
            chunk_server_locations[(st + 2) % total],
        ]

       
cfg = Config('config.json')          
#cfg.getServerHostname('master_server')
#test = cfg.getChunkLocations()
#print(test)
util = Util('config.json')
test = util.chooseLocations()
print(test)