import socket
import random
import msgpack
from zatt.server.utils import sign, validateIndex
# import socket.timeout as TimeoutException
import select


class AbstractClient:
    """Abstract client. Contains primitives for implementing functioning
    clients."""

    def __init__(self):
        self.privateKey = None
        self.publicKeyMap = None # (addr, port) -> pk verifier
        self.currentLeader = None
        Crypto.Random.atfork()

    def _request(self, message):
        resp, addr = self._perform_request_step(message, self.currentLeader)
        if resp is not None:
            self.currentLeader = addr
            return resp # successful
        
        if addr is not None:
            self.currentLeader = addr
            resp, _ = self._perform_request_step(message, self.currentLeader)

        while (resp is None):
            # broadcast
            resp = self.perform_broadcast(message)

        return resp

    def perform_broadcast(self, message):
        socket_list = []
        for backupAddr in self.publicKeyMap.keys():
            sock = self.get_socket(message, backupAddr)
            socket_list.append(sock)
        while (True):
            sockets, _, _ = select.select(socket_list, [], [], 5)
            if (len(sockets) == 0): return None
            finalResp = None
            for sock in sockets:
                resp = self._read_bytes(sock)
                if 'type' in resp and resp['type'] == 'redirect':
                    continue
                if self.validate_response(message, resp):
                    finalResp = resp
                    break
            for sock in sockets:
                sock.close()
            if finalResp is not None:
                return finalResp

    def get_socket(self, message, addr):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(addr)
        if 'data' in message:
            signature = sign(message
            ['data'], self.privateKey)
            message['signature'] = signature
        sock.send(msgpack.packb(message, use_bin_type=True))
        # sock.settimeout(5)
        return sock
    
    def validate_response(self, message, resp):
        if ('type' in resp and resp['type'] == 'result'):
            _, calcCommit = validateIndex(list(resp['log']), resp['proof'], self.publicKeyMap, len(self.publicKeyMap))
            if (calcCommit < resp['index'] or len(resp['log']) <= resp['index']):
                assert False
                return False
            else:
                givenVal = resp['log'][resp['index']]['data']
                if givenVal != message['data']:
                    assert False
                    return False
        return True
    
    def _read_bytes(self, sock):
        buff = bytes()
        try:
            while True:
                block = sock.recv(128)
                if not block:
                    break
                buff += block
            resp = msgpack.unpackb(buff, encoding='utf-8', use_list=False)
        except socket.timeout:
            print("TIMED OUT")
            return None
        sock.close()
        return resp
    
    def _perform_request_step(self, message, addr):
        """ Response, leader that responded """
        sock = self.get_socket(message, addr)
        sock.settimeout(5)
        resp = self._read_bytes(sock)
        if resp == None:
            assert False
            return None, None
        if 'type' in resp and resp['type'] == 'redirect':
            return None, tuple(resp['leader'])
        isValid = self.validate_response(message, resp)
        if isValid:
            return resp, addr
        else:
            return None, None
        

    def _get_state(self):
        """Retrive remote state machine."""
        # self.server_address = tuple(random.choice(tuple(self.data['cluster'])))
        resp = self._request({'type': 'get'})
        resp['cluster'] = self.data['cluster']
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
