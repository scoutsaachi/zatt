import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__),'../')) # set python path
import unittest
from time import sleep
from utils import Pool
from multiprocessing import Process
from zatt.client import DistributedDict
from Crypto.PublicKey import RSA
from Crypto.Signature import PKCS1_PSS


class BasicTest(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None
        key = RSA.generate(2048)
        self.privateKey = PKCS1_PSS.new(key)
        publicKey = key.publickey()
        nodeKeys = [RSA.generate(2048) for i in range(4)] # these are the node keys

        print('BasicTest setup')
        self.pool = Pool(4, nodeKeys, publicKey)
        self.pool.start(self.pool.ids)

        clusterAddresses = [("127.0.0.1", 9110 + i) for i in range(4)] # [(ip_addr, port)]
        self.default_cluster = set(clusterAddresses)

        # the client needs to know the mapping to public keys
        self.clusterMap = {k : PKCS1_PSS.new(nodeKeys[i].publickey()) for i,k in enumerate(clusterAddresses)} #[(ip_addr, port) -> public key]
        sleep(5) # sleep to wait for servers to set up

    def tearDown(self):
        self.pool.stop(self.pool.ids)
        self.pool.rm(self.pool.ids)

    def test_1_append(self):
        print('Append test')
        d = DistributedDict('127.0.0.1', 9110, self.clusterMap, self.privateKey)
        d['adams'] = 'the hitchhiker guide'
        del d
        sleep(1)
        d = DistributedDict('127.0.0.1', 9110, self.clusterMap, self.privateKey)
        self.assertEqual(d['adams'], 'the hitchhiker guide')

    def test_2_delete(self):
        print('Delete test')
        d = DistributedDict('127.0.0.1', 9110, self.clusterMap, self.privateKey)
        d['adams'] = 'the hitchhiker guide'
        sleep(1)
        del d['adams']
        sleep(1)
        d = DistributedDict('127.0.0.1', 9110, self.clusterMap, self.privateKey)
        self.assertEqual(d, {'cluster': self.default_cluster})

    def test_3_read_from_different_client(self):
        print('Read from different client')
        d = DistributedDict('127.0.0.1', 9110, self.clusterMap, self.privateKey)
        d['adams'] = 'the hitchhiker guide'
        del d
        sleep(1)
        d = DistributedDict('127.0.0.1', 9111, self.clusterMap, self.privateKey)
        self.assertEqual(d['adams'], 'the hitchhiker guide')


if __name__ == '__main__':
    unittest.TestLoader.sortTestMethodsUsing = lambda _, x, y: (y < x)-(y > x)
    unittest.main()
