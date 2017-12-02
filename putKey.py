from zatt.client import DistributedDict
d = DistributedDict('127.0.0.1', 5254)
d['key1'] = 0
