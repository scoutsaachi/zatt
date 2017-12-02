import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__),'../../')) # set python path

import asyncio
import logging
import argparse
import zatt.server.config as cfg
from zatt.server.config import Config
from zatt.server.protocols import Orchestrator, PeerProtocol, ClientProtocol
from zatt.server.logger import start_logger

def setup(config):
    """Setup a node."""
    cfg.config = config
    start_logger()
    logger = logging.getLogger(__name__)

    loop = asyncio.get_event_loop()
    orchestrator = Orchestrator()
    addrTuple = cfg.config.getMyClusterInfo()
    print (addrTuple)
    coro = loop.create_datagram_endpoint(lambda: PeerProtocol(orchestrator),
                                         local_addr=addrTuple)
    transport, _ = loop.run_until_complete(coro)
    orchestrator.peer_transport = transport

    coro = loop.create_server(lambda: ClientProtocol(orchestrator), *addrTuple)
    server = loop.run_until_complete(coro)

    logger.info('Serving on %s', addrTuple)
    return server


def run():
    """Start a node."""
    parser = argparse.ArgumentParser(description='Start node for Raft BFT')
    parser.add_argument('configFile', type=str, help='the configuration file')
    parser.add_argument('id', type=int, help='the node id to be started')
    parser.add_argument('--debug',type=bool, default=False, help="print debugger log")
    args = parser.parse_args()
    config = Config.CreateConfig(args.configFile, args.id, args.debug)
    server = setup(config)
    loop = asyncio.get_event_loop()
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    # # Close the server
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()

if __name__ == '__main__':
    run()