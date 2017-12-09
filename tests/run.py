import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__),'../')) # set python path
import unittest
from time import sleep
from utils import Pool
from multiprocessing import Process
from zatt.client import DistributedDict
from Crypto.PublicKey import RSA


class BasicTest(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None
        key = RSA.generate(2048)
        self.privateKey = key
        publicKey = key.publickey()
        print('BasicTest setup')
        self.pool = Pool(4, publicKey)
        self.pool.start(self.pool.ids)
        self.default_cluster = set([('127.0.0.1', 9110), ('127.0.0.1', 9112), ('127.0.0.1', 9111), ('127.0.0.1', 9113)])
        sleep(5) # sleep to wait for servers to set up

    def tearDown(self):
        self.pool.stop(self.pool.ids)
        self.pool.rm(self.pool.ids)

    # def test_0_diagnostics(self):
    #     print('Diagnostics test')
    #     print('Restarting server 0 to force Follower state')
    #     self.pool.stop(0)
    #     # sleep(2)
    #     self.pool.start(0)
    #     sleep(2)
    #     expected =\
    #         {'files': 'STUB', 'status': 'Follower',
    #          'persist': {'votedFor': 'STUB', 'currentTerm': 'STUB'},
    #          'volatile': {'leaderId': 'STUB', 'address': ['127.0.0.1', 9110],
    #                       'cluster': set((('127.0.0.1', 9112),
    #                                       ('127.0.0.1', 9110),
    #                                       ('127.0.0.1', 9111)))},
    #          'log': {'commitIndex': -1, 'log': {'data': [], 'path': 'STUB'},
    #                  'state_machine': {'lastApplied': -1, 'data': {}},
    #                  'compacted': {'count': 0, 'term': None, 'path': 'STUB',
    #                                'data': {}}}}

    #     d = DistributedDict('127.0.0.1', 9110)
    #     diagnostics = d.diagnostic
    #     diagnostics['files'] = 'STUB'
    #     diagnostics['log']['compacted']['path'] = 'STUB'
    #     diagnostics['log']['log']['path'] = 'STUB'
    #     diagnostics['persist']['votedFor'] = 'STUB'
    #     diagnostics['persist']['currentTerm'] = 'STUB'
    #     diagnostics['volatile']['leaderId'] = 'STUB'
    #     diagnostics['volatile']['cluster'] =\
    #         set(map(tuple, diagnostics['volatile']['cluster']))
    #     self.assertEqual(expected, diagnostics)

    def test_1_append(self):
        print('Append test')
        d = DistributedDict('127.0.0.1', 9110, self.privateKey)
        d['adams'] = 'the hitchhiker guide'
        del d
        sleep(1)
        print ("APPENDING NOW")
        d = DistributedDict('127.0.0.1', 9110, self.privateKey)
        self.assertEqual(d['adams'], 'the hitchhiker guide')

    def test_2_delete(self):
        print('Delete test')
        d = DistributedDict('127.0.0.1', 9110, self.privateKey)
        d['adams'] = 'the hitchhiker guide'
        sleep(1)
        del d['adams']
        sleep(1)
        d = DistributedDict('127.0.0.1', 9110, self.privateKey)
        self.assertEqual(d, {'cluster': self.default_cluster})

    def test_3_read_from_different_client(self):
        print('Read from different client')
        d = DistributedDict('127.0.0.1', 9110, self.privateKey)
        d['adams'] = 'the hitchhiker guide'
        del d
        sleep(1)
        d = DistributedDict('127.0.0.1', 9111, self.privateKey)
        self.assertEqual(d['adams'], 'the hitchhiker guide')

    # def test_4_compacted_log_replication(self):
    #     print('Compacted log replication')
    #     d = DistributedDict('127.0.0.1', 9110)
    #     d['test'] = 0
    #     d['test'] = 1
    #     d['test'] = 2
    #     d['test'] = 3
    #     d['test'] = 4  # compaction kicks in
    #     del d
    #     sleep(1)
    #     d = DistributedDict('127.0.0.1', 9111)
    #     self.assertEqual(d, {'test': 4})

    # def test_5_add_server(self):
    #     print('Add new server')
    #     d = DistributedDict('127.0.0.1', 9110)
    #     d['test'] = 0
    #     self.pool.stop(self.pool.ids)
    #     self.pool.start(self.pool.ids)

    #     self.pool.configs[10] = {'address': ('127.0.0.1', 9120),
    #                              'cluster': {('127.0.0.1', 9120), },
    #                              'storage': '20.persist', 'debug': False}
    #     self.pool.servers[10] = Process(target=self.pool._run_server,
    #                                     args=(self.pool.configs[10],))
    #     self.pool.start(10)
    #     sleep(1)

    #     d.config_cluster('add', '127.0.0.1', 9120)
    #     sleep(1)

    #     del d
    #     d = DistributedDict('127.0.0.1', 9120)

    #     self.assertEqual(d, {'test': 0})

    # def test_6_remove_server(self):
    #     print('Remove server')
    #     d = DistributedDict('127.0.0.1', 9110)
    #     d.config_cluster('delete', '127.0.0.1', 9111)
    #     sleep(1)

    #     self.pool.stop(1)

    #     self.assertEqual(set(map(tuple, d.diagnostic['volatile']['cluster'])),
    #                      {('127.0.0.1', 9112), ('127.0.0.1', 9110)})


if __name__ == '__main__':
    unittest.TestLoader.sortTestMethodsUsing = lambda _, x, y: (y < x)-(y > x)
    unittest.main()
