import json
config = None # global config variable! set in main.py

class Config:
    """Collect and merge CLI and file based config.
    This class is a singleton based on the Borg pattern."""
    __shared_state = {}
    def __init__(self, storageDir, cluster, nodeId, debug):
        self.storageDir = storageDir
        self.cluster = cluster # an array of ("addr", portNum)
        self.nodeID = nodeId
        self.debug = debug
    
    """ Return a new config that is based off of the json given in parameter"""
    def CreateConfig(cfg_filename, nodeId, debug=True):
        with open(cfg_filename) as f:    
            d = json.load(f)
        assert "StorageDir" in d
        assert "cluster" in d
        clusterList = []
        for l in d["cluster"]:
            assert len(l) == 2
            clusterList.append((l[0], l[1])) # addr, port
        return Config(d["StorageDir"], clusterList, nodeId, debug)
    
    """ Get the storage information for one node"""
    def getStorageLocation(self,index):
        assert index < len(self.cluster)
        return "%s/%d.storage" % (self.storageDir, index) # storageDir/{index}.storage

    def getClusterInfo(self,index):
        assert index < len(self.cluster)
        return self.cluster[index]

    def getMyStorage(self):
        return self.getStorageLocation(self.nodeID)
    
    def getMyClusterInfo(self):
        return self.getClusterInfo(self.nodeID)