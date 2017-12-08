import os
import json
import time
import asyncio
import logging
import collections
import msgpack
import math
from Crypto.PublicKey import RSA
from Crypto.Hash import SHA256
from Crypto.Signature import PKCS1_PSS
import Crypto
from pprint import pformat

MAX_MSGPACK_ARRAY_HEADER_LEN = 5
logger = logging.getLogger(__name__)


class PersistentDict(collections.UserDict):
    """Dictionary data structure that is automatically persisted to disk
    as json."""
    def __init__(self, path=None, data={}):
        if os.path.isfile(path):
            with open(path, 'r') as f:
                data = json.loads(f.read())
        self.path = path
        super().__init__(data)

    def __setitem__(self, key, value):
        self.data[self.__keytransform__(key)] = value
        self.persist()

    def __delitem__(self, key):
        del self.data[self.__keytransform__(key)]
        self.persist()

    def __keytransform__(self, key):
        return key

    def persist(self):
        with open(self.path, 'w+') as f:
            f.write(json.dumps(self.data))


class TallyCounter:
    def __init__(self, categories=[]):
        self.data = {c: {'current': 0, 'past': collections.deque(maxlen=10)}
                     for c in categories}
        loop = asyncio.get_event_loop()
        loop.call_later(1, self._tick)

    def _tick(self):
        for name, category in self.data.items():
            if category['current']:
                logger.debug('Completed %s %s (%s ms/op)', category['current'],
                             name, 1/category['current'] * 1000)

            category['past'].append({time.time(): category['current']})
            category['current'] = 0
        loop = asyncio.get_event_loop()
        loop.call_later(1, self._tick)

    def increment(self, category, amount=1):
        self.data[category]['current'] += amount


def msgpack_appendable_pack(o, path):
    open(path, 'a+').close()  # touch
    with open(path, mode='r+b') as f:
        packer = msgpack.Packer()
        unpacker = msgpack.Unpacker(f)

        if type(o) == list:
            try:
                previous_len = unpacker.read_array_header()
            except msgpack.OutOfData:
                previous_len = 0

            # calculate and replace header
            header = packer.pack_array_header(previous_len + len(o))
            f.seek(0)
            f.write(header)
            f.write(bytes(1) * (MAX_MSGPACK_ARRAY_HEADER_LEN - len(header)))

            # append new elements
            f.seek(0, 2)
            for element in o:
                f.write(packer.pack(element))
        else:
            f.write(packer.pack(o))


def msgpack_appendable_unpack(path):
    # if not list?
    # return msgpack.unpackb(f.read())
    with open(path, 'rb') as f:
        packer = msgpack.Packer()
        unpacker = msgpack.Unpacker(f, encoding='utf-8')
        length = unpacker.read_array_header()

        header_lenght = len(packer.pack_array_header(length))
        unpacker.read_bytes(MAX_MSGPACK_ARRAY_HEADER_LEN - header_lenght)
        f.seek(MAX_MSGPACK_ARRAY_HEADER_LEN)

        return [unpacker.unpack() for _ in range(length)]


def extended_msgpack_serializer(obj):
    """msgpack serializer for objects not serializable by default"""

    if isinstance(obj, collections.deque):
        serial = list(obj)
        return serial
    else:
        raise TypeError("Type not serializable")

# extract kth largest element from list l
def get_kth_smallest(l, k):
    l = sorted(l)
    return l[k-1]

# get 2f +1
def get_quorum_size(n):
    assert (n-1) % 3 == 0
    return math.ceil((float(n-1)*(2/3)) + 1)

# -------- KEY UTILS ------------
def createAndWriteKeys(directoryName, n):
    private_dir = "%s/private_keys" % directoryName
    public_dir = "%s/public_keys" % directoryName
    if not os.path.exists(private_dir):
        os.makedirs(private_dir)
    if not os.path.exists(public_dir):
        os.makedirs(public_dir)
    for i in range(n):
        key = RSA.generate(2048)
        privateKey = key.exportKey('PEM')
        publicKey = key.publickey().exportKey('PEM')
        privateKeyFileName = "%s/%d.pem" % (private_dir, i)
        publicKeyFileName = "%s/%d.pem" % (public_dir, i)
        f = open(privateKeyFileName, 'wb')
        f.write(privateKey)
        f.close()

        f = open(publicKeyFileName, 'wb')
        f.write(publicKey)
        f.close()
    
    # generate client keys
    key = RSA.generate(2048)
    privateKey = key.exportKey('PEM')
    publicKey = key.publickey().exportKey('PEM')
    privateKeyFileName = "%s/%d.pem" % (private_dir, "client_key")
    publicKeyFileName = "%s/%d.pem" % (public_dir, "client_key")
    f = open(privateKeyFileName, 'wb')
    f.write(privateKey)
    f.close()
    f = open(publicKeyFileName, 'wb')
    f.write(publicKey)
    f.close()

def importPublicKeys(directoryName, n):
    """ give the key directory and the expected number of public keys"""
    public_dir = "%s/public_keys" % directoryName
    keys = []
    for i in range(n):
        filename = "%s/%d.pem" % (public_dir, i)
        assert os.path.exists(filename)
        f = open(filename, 'r')
        keys.append(RSA.importKey(f.read()))
        f.close()
    return keys

def importPrivateKey(directoryName, id):
    """ give the key directory and the id for the private key we are retreiving"""
    filename = "%s/private_keys/%d.pem" % (directoryName, id)
    assert os.path.exists(filename)
    f = open(filename, 'r')
    key = RSA.importKey(f.read())
    f.close()
    return key

def importClientPublicKey(directoryName):
    filename = "%s/public_keys/client_key.pem" % (directoryName)
    assert os.path.exists(filename)
    f = open(filename, 'r')
    key = RSA.importKey(f.read())
    f.close()
    return key

def importClientPrivateKey(directoryName):
    filename = "%s/private_keys/client_key.pem" % (directoryName)
    assert os.path.exists(filename)
    f = open(filename, 'r')
    key = RSA.importKey(f.read())
    f.close()
    return key


def validateSignature(message, signature, pk):
    """ pk should be a verifier """
    h = SHA256.new()
    h.update(pformat(message).encode('utf-8'))
    # verifier = PKCS1_PSS.new(pk)
    # return verifier.verify(h, signature)
    return pk.verify(h, signature)

def sign(msg, sk):
    """ return a signed version of the message digest with the given secret key
        sk should be a signer object """
    h = SHA256.new()
    h.update(pformat(msg).encode('utf-8'))
    # signer = PKCS1_PSS.new(sk)
    # return signer.sign(h)
    return sk.sign(h)

def getLogHash(log, index):
    digest = SHA256.new()
    digest.update(pformat(log[:index + 1]).encode('utf-8'))
    return digest

def validateDict(message, pk):
    """ assuming that signature is in the message, validate if the signature is
        correct """
    assert 'signature' in message
    signature = message['signature']
    message['signature'] = 0
    isValid = validateSignature(message, signature, pk)
    message['signature'] = signature
    return isValid

def signDict(message, sk):
    message['signature'] = 0
    s = sign(message, sk)
    message['signature'] = s 
    return message

def validate_entries(entries, client_pk):
    """ validate that all entries in entries are signed by the client """
    for entry in entries:
        if not validateDict(entry['data'], client_pk): return False
    return True