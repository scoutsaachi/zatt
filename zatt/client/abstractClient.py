import socket
import random
import msgpack
from zatt.server.utils import signDict
from Crypto.Signature import PKCS1_PSS



class AbstractClient:
    """Abstract client. Contains primitives for implementing functioning
    clients."""

    def __init__(self, privateKey):
        self.privateKey = PKCS1_PSS.new(privateKey) # a signer with private key

    def _request(self, message):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(self.server_address)
        message = signDict(message, self.privateKey)
        sock.send(msgpack.packb(message, use_bin_type=True))

        buff = bytes()
        while True:
            block = sock.recv(128)
            if not block:
                break
            buff += block
        resp = msgpack.unpackb(buff, encoding='utf-8')
        sock.close()
        if 'type' in resp and resp['type'] == 'redirect':
            self.server_address = tuple(resp['leader'])
            resp = self._request(message)
        return resp

    def _get_state(self):
        """Retrive remote state machine."""
        self.server_address = tuple(random.choice(tuple(self.data['cluster'])))
        resp = self._request({'type': 'get'})
        print("RESPONSE", resp)
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
