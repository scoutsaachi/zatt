import socket
import random
import msgpack
from zatt.server.utils import sign, validateIndex



class AbstractClient:
    """Abstract client. Contains primitives for implementing functioning
    clients."""

    def __init__(self):
        self.privateKey = None
        self.publicKeyMap = None # (addr, port) -> pk verifier
        Crypto.Random.atfork()

    def _request(self, message):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(self.server_address)
        if 'data' in message:
            signature = sign(message['data'], self.privateKey)
            message['signature'] = signature
        # message = signDict(message, self.privateKey)
        sock.send(msgpack.packb(message, use_bin_type=True))

        buff = bytes()
        while True:
            block = sock.recv(128)
            if not block:
                break
            buff += block
        resp = msgpack.unpackb(buff, encoding='utf-8', use_list=False)
         # TODO: should broadcast to nodes since this primary is bogus instead of any of the below assertions
        if ('type' in resp and resp['type'] == 'result'):
            _, calcCommit = validateIndex(list(resp['log']), resp['proof'], self.publicKeyMap, len(self.publicKeyMap))
            if (calcCommit < resp['index'] or len(resp['log']) <= resp['index']):
                assert False
            else:
                givenVal = resp['log'][resp['index']]['data']
                if givenVal != message['data']:
                    assert False
        
        sock.close()
        if 'type' in resp and resp['type'] == 'redirect':
            self.server_address = tuple(resp['leader'])
            resp = self._request(message)
        return resp

    def _get_state(self):
        """Retrive remote state machine."""
        self.server_address = tuple(random.choice(tuple(self.data['cluster'])))
        resp = self._request({'type': 'get'})
        resp['cluster'] = set( [tuple(c) for c in resp['cluster']] )
        return resp

    def _append_log(self, payload):
        """Append to remote log."""
        return self._request({'type': 'append', 'data': payload})

    @property
    def diagnostic(self):
        return self._request({'type': 'diagnostic'})

    def config_cluster(self, action, address, port):
        return self._request({'type': 'config', 'action': action,
                              'address': address, 'port': port})
