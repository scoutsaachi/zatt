import asyncio
import logging
import statistics
from random import randrange
from os.path import join
from zatt.server.utils import PersistentDict, TallyCounter
from zatt.server.log import LogManager
from zatt.server.utils import get_kth_largest, get_quorum_size, validate_entries, signDict, validateDict, getLogHash, validateIndex
import zatt.server.config as cfg
import Crypto
logger = logging.getLogger(__name__)


class State:
    """Abstract state for subclassing."""
    def __init__(self, old_state=None, orchestrator=None):
        """State is initialized passing an orchestrator instance when first
        deployed. Subsequent state changes use the old_state parameter to
        preserve the environment.
        """
        Crypto.Random.atfork()
        if old_state:
            self.orchestrator = old_state.orchestrator
            self.persist = old_state.persist
            self.volatile = old_state.volatile
            self.log = old_state.log
        else:
            self.orchestrator = orchestrator
            self.persist = PersistentDict(join(cfg.config.getMyStorage(), 'state'),
                                          {'votedFor': None, 'currentTerm': -1})
            self.volatile = {'leaderId': None, 'cluster': cfg.config.cluster.keys(),
                             'publicKeyMap': cfg.config.cluster.copy(), # map from addresses to the verifiers
                             'address': cfg.config.getMyClusterInfo(),
                             'privateKey': cfg.config.getMyPrivateKey(), # an actual signer
                             'clientKey': cfg.config.client_key}
            self.election_timer = None
            self.log = LogManager(address=self.volatile['address'])
            self._update_cluster()
        self.term_change_messages = {} # Map of term -> term change messages for that term (for terms for which you are a valid leader)
        self.stats = TallyCounter(['read', 'write', 'append'])

    def getLeaderForTerm(self, term):
        index = term % len(self.volatile['cluster'])
        return list(self.volatile['cluster'])[index]

    def data_received_peer(self, peer, msg):
        """Receive peer messages from orchestrator and pass them to the
        appropriate method."""
        logger.debug('Received %s from %s', msg['type'], peer)

        method = getattr(self, 'on_peer_' + msg['type'], None)
        if method:
            method(peer, msg)
        else:
            logger.info('Unrecognized message from %s: %s', peer, msg)

    def data_received_client(self, protocol, msg):
        """Receive client messages from orchestrator and pass them to the
        appropriate method."""
        method = getattr(self, 'on_client_' + msg['type'], None)
        if method:
            method(protocol, msg)
        else:
            logger.info('Unrecognized message from %s: %s',
                        protocol.transport.get_extra_info('peername'), msg)

    def on_client_append(self, protocol, msg):
        """Redirect client to leader upon receiving a client_append message."""
        entryIndex = self.log.log.findEntry(msg['data'])
        if entryIndex != -1:
            protocol.send({'type': 'result', 'success': True, 'proof':self.proofOfCommit, 'log':tuple(self.log.log.data), 'index': entryIndex})
            print("Found entry immediately so just sending it back")
            return

        if hasattr(self, "election_timer") and self.election_timer is None:
            self.start_election_timer()

        msg = {'type': 'redirect',
            'leader': self.volatile['leaderId']}
        protocol.send(msg)
        logger.debug('Redirect client %s:%s to leader',
                    *protocol.transport.get_extra_info('peername'))

    def on_client_config(self, protocol, msg):
        """Redirect client to leader upon receiving a client_config message."""
        return self.on_client_append(protocol, msg)

    def on_client_get(self, protocol, msg):
        """Return state machine to client."""
        state_machine = self.log.state_machine.data.copy()
        self.stats.increment('read')
        protocol.send(state_machine)

    def on_client_diagnostic(self, protocol, msg):
        """Return internal state to client."""
        msg = {'status': self.__class__.__name__,
               'persist': {'votedFor': self.persist['votedFor'],
                           'currentTerm': self.persist['currentTerm']},
               'volatile': self.volatile,
               'log': {'commitIndex': self.log.commitIndex},
               'stats': self.stats.data}
        msg['volatile']['cluster'] = list(msg['volatile']['cluster'])

        if type(self) is Leader:
            msg.update({'leaderStatus':
                        {'netIndex': tuple(self.nextIndex.items()),
                         'prePrepareIndex': tuple(self.prePrepareIndexMap.items()),
                         'waiting_clients': {k: len(v) for (k, v) in
                                             self.waiting_clients.items()}}})
        protocol.send(msg)

    def _update_cluster(self, entries=None):
        """Scans compacted log and log, looking for the latest cluster
        configuration."""
        return
    
    def on_peer_term_change(self, peer, msg):

        # validate that the peer actually send this message
        proposedTerm = msg['term']

        if (not validateDict(msg, self.volatile['publicKeyMap'][peer])):
            return

        # should this message be for us
        if self.getLeaderForTerm(proposedTerm) != self.volatile['address']:
            return
        
        # this person has committed entries we don't even know about - we can't be leader for this term
        if msg['commitIndex'] > self.log.index:
            return

        # validate commit point and prepare point on incoming message
        hypothetical_new_log = self.log.log.data[:(msg['commitIndex'] + 1)] + list(msg['logAfterCommit'])
        commitIndex = msg['commitIndex']
        prepareIndex = commitIndex + len(msg['logAfterCommit'])

        calcPrepare = prepareIndex
        calcCommit = commitIndex
        if (msg['proof'] is not None):
            calcPrepare, calcCommit = validateIndex(hypothetical_new_log, msg['proof'], self.volatile['publicKeyMap'], len(self.volatile['cluster']))
        if not calcPrepare == prepareIndex or not calcCommit == commitIndex:
            return
        
        if commitIndex > self.log.commitIndex:
            self.log.commit(commitIndex)
            
        if proposedTerm not in self.term_change_messages:
            self.term_change_messages[proposedTerm] = {}

        self.term_change_messages[proposedTerm][peer] = msg
            
        quorum_size = get_quorum_size(len(self.volatile['cluster'])) - 1
        if len(self.term_change_messages[proposedTerm]) == quorum_size:
            self.send_new_term(proposedTerm)
    
    def get_latest_element_from_log(logSlice, commitIndex):
        if len(logSlice) == 0:
            return 

    def on_peer_new_term(self, peer, msg):
        # validate from peer
        if (not validateDict(msg, self.volatile['publicKeyMap'][peer])):
            return
        if msg['term'] <= self.persist['currentTerm']:
            return

        quorum_size = get_quorum_size(len(self.volatile['cluster'])) - 1 #2f
        if len(msg['proof']) < quorum_size:
            return # not enough messages to convince that new term
        
        for proofPeer, proofMsg in msg['proof'].items():
            if (not validateDict(proofMsg, self.volatile['publicKeyMap'][proofPeer])):
                return
            if proofMsg['term'] != msg['term']:
                return # not the right term in proof
        self.persist['currentTerm'] = msg['term']
        print(self.volatile['address'], " has received and validated a new_term message and is now reverting to follower state in term:", msg['term'])
        self.orchestrator.change_follower()

    def send_new_term(self, proposedTerm):
        peer_messages = [tuple(i) for i in self.term_change_messages[proposedTerm].items()]
        messages = [pm[1] for pm in peer_messages]
        extra_log_entries = []
        latest_term = -1
        longest = -1
        latestProof = None
        for msg in messages:
            logAfterCommit = list(msg['logAfterCommit'])
            if len(logAfterCommit) > 0:
                lastTerm = logAfterCommit[len(logAfterCommit) - 1].term
                if lastTerm > latest_term:
                    latest_term = lastTerm
                    longest = len(logAfterCommit)
                    extra_log_entries = logAfterCommit
                    latestProof = msg['proof']
                elif lastTerm == latest_term and len(logAfterCommit) > longest:
                    longest = len(logAfterCommit)
                    extra_log_entries = logAfterCommit
                    latestProof = msg['proof']
        self.log.append_entries(extra_log_entries, self.log.commitIndex)

        new_term_msg = {
            'type' : 'new_term',
            'term' : proposedTerm,
            'proof' : self.term_change_messages[proposedTerm]
        }
        # change ourselves to be a leader
        print(self.volatile['address'], " has received enough term change messages and is now the leader")
        self.persist['currentTerm'] = proposedTerm
        self.orchestrator.change_leader(latestProof)

        new_term_msg_signed = signDict(new_term_msg, self.volatile['privateKey'])
        for addr in self.volatile['cluster']:
            if addr != self.volatile['address']:
                self.orchestrator.send_peer(addr, new_term_msg_signed)

class Follower(State):
    """Follower state."""
    def __init__(self, old_state=None, orchestrator=None):
        """Initialize parent and start election timer."""
        super().__init__(old_state, orchestrator)
        self.persist['votedFor'] = None
        self.proofOfCommit = None
        self.currTimeout = 5
        self.timerStartTime = None

    def teardown(self):
        """Stop timers before changing state."""
        self.election_timer.cancel()
        self.election_timer = None

    def start_election_timer(self):
        """Delays transition to the Candidate state by timer."""

        timeout = self.currTimeout
        loop = asyncio.get_event_loop()
        self.election_timer = loop.\
            call_later(timeout, self.orchestrator.change_voter)
        logger.debug('Election timer started: %s s', timeout)

    def cancel_election_timer(self):
        if hasattr(self, "election_timer"):  
            self.election_timer.cancel()
            self.election_timer = None
        self.currTimeout = 5

    def on_peer_update(self, peer, msg):
        """Manages incoming log entries from the Leader.
        Data from log compaction is always accepted.
        In the end, the log is scanned for a new cluster config.
        """

        # validate that the leader actually sent this message
        # TODO validate that this leader is a valid leader
        if (not validateDict(msg, self.volatile['publicKeyMap'][peer])):
            return

        entries = list(msg['entries'])
        # validate that the entries proposed are all signed by the client
        if not validate_entries(entries, self.volatile['clientKey']):
            return

        status_code = 0
        # check for prev log index match
        term_is_current = msg['term'] >= self.persist['currentTerm']
        prev_log_term_match = msg['prevLogTerm'] is None or\
            self.log.term(msg['prevLogIndex']) == msg['prevLogTerm']

        if not term_is_current or not prev_log_term_match:
            # there was a mismatch in prev log index
            status_code = 1
            logger.warning('Could not append entries. cause: %s', 'wrong\
                term' if not term_is_current else 'prev log term mismatch')
        else:
            hypothetical_new_log = self.log.log.data[:(msg['prevLogIndex'] + 1)] + entries
            # validate prepare and commit
            calcPrepare, calcCommit = validateIndex(hypothetical_new_log, msg['proof'], self.volatile['publicKeyMap'], len(self.volatile['cluster']))
            if not calcPrepare == msg['leaderPrepare'] or not calcCommit == msg['leaderCommit']:
                return

            # either we don't need to overwrite a prepare or the leader
            # has a valid prepare index greater than our own
            should_copy_leader_log = (msg['leaderPrepare'] >= self.log.prepareIndex)
            if not should_copy_leader_log:
                status_code = 2 # you are trying to overwrite a prepare index without a greater leader prepare
            else:
                # we are successful so overwrite with leader's log
                self.log.append_entries(msg['entries'], msg['prevLogIndex']+1)
                self.log.prepare(msg['leaderPrepare'])
                oldCommitIndex = self.log.commitIndex
                self.log.commit(msg['leaderCommit'])
                newCommitIndex = self.log.commitIndex
                if newCommitIndex > oldCommitIndex:
                    self.cancel_election_timer()
                logger.debug('Log index is now %s', self.log.index)
                self.stats.increment('append', len(msg['entries']))
                self.proofOfCommit = msg['proof']
            self.volatile['leaderId'] = msg['leaderId']

        self._update_cluster()

        # status_code: {0: Good, 1: prev-log index mismatch, 2: tried to overwrite prepare without a greater leader prepare}
        resp = {'type': 'response_update', "status_code": status_code,
                'term': self.persist['currentTerm'],
                'prePrepareIndex': (self.log.index, self.log.getHash(self.log.index)),
                'prepareIndex' : (self.log.prepareIndex, self.log.getHash(self.log.prepareIndex))
        }
        signedResp = signDict(resp, self.volatile['privateKey'])
        self.orchestrator.send_peer(peer, signedResp)

class Voter(Follower):
    def __init__(self, old_state=None, orchestrator=None):
        super().__init__(old_state, orchestrator)
        self.election_timer = None
        self.currTimeout *= 2
        self.proposedTerm = self.persist['currentTerm'] + 1

        timeout = 1
        loop = asyncio.get_event_loop()
        self.term_change_timer = loop.call_later(timeout, self.send_term_change)

    def teardown(self):
        if self.election_timer is not None: 
            self.election_timer.cancel()
            self.election_timer = None

        self.term_change_timer.cancel()
        self.term_change_timer = None

    def send_term_change(self):
        leader = self.getLeaderForTerm(self.proposedTerm)
        if leader == self.volatile['address']:
            return
            
        msg = {
            'type': 'term_change',
            'term': self.proposedTerm,
            'proof':self.proofOfCommit,
            'commitIndex' : self.log.commitIndex,
            'logAfterCommit' : tuple(self.log.log.data[self.log.commitIndex + 1: self.log.prepareIndex + 1])
        }
        signedMsg = signDict(msg, self.volatile['privateKey'])
        self.orchestrator.send_peer(leader, signedMsg)

        timeout = randrange(1, 4) * 10 ** (-1 if cfg.config.debug else -2) 
        loop = asyncio.get_event_loop()
        self.term_change_timer = loop.call_later(timeout, self.send_term_change)

    def on_peer_update(self, peer, msg):
        print ("Received an update message in the voter state. Will ignore as term has not been confirmed")
        return

class Leader(State):
    """Leader state."""
    def __init__(self, old_state=None, orchestrator=None, proof=None):
        """Initialize parent, sets leader variables, start periodic
        append_entries"""
        super().__init__(old_state, orchestrator)
        logger.info('Leader of term: %s', self.persist['currentTerm'])
        self.volatile['leaderId'] = self.volatile['address']
        self.prePrepareIndexMap = {p: -1 for p in self.volatile['cluster']} # latest pre-Prepare point per follower
        self.nextIndexMap = {p: self.log.commitIndex + 1 for p in self.prePrepareIndexMap}
        self.prepareIndexMap = {p: -1 for p in self.volatile['cluster']} # latest prepare position per follower
        if proof is None:
            self.latestMessageMap = {p: {} for p in self.volatile['cluster']} # latest update response per follower
        else:
            self.latestMessageMap = proof
        self.waiting_clients = {} # log index -> [protocol for a client]
        self.send_update()

    def teardown(self):
        """Stop timers before changing state."""
        self.update_timer.cancel()
        if hasattr(self, 'config_timer'):
            self.config_timer.cancel()

    def send_update(self):
        """Send update to the cluster, containing:
        - nothing: if remote node is up to date.
        - compacted log: if remote node has to catch up.
        - log entries: if available.
        Finally schedules itself for later execution."""
        for peer in self.volatile['cluster']:
            if peer == self.volatile['address']: continue
            msg = {'type': 'update',
                   'term': self.persist['currentTerm'],
                   'leaderCommit': self.log.commitIndex,
                   'leaderPrepare': self.log.prepareIndex, # all logs up to this point are prepared
                   'leaderId': self.volatile['address'],
                   'prevLogIndex': self.nextIndexMap[peer] - 1,
                   'entries': tuple(self.log[self.nextIndexMap[peer]:
                                       self.nextIndexMap[peer] + 100]),
                   'proof': self.latestMessageMap}
            msg.update({'prevLogTerm': self.log.term(msg['prevLogIndex'])})
            msg = signDict(msg, self.volatile['privateKey'])

            logger.debug('Sending %s entries to %s. Start index %s',
                         len(msg['entries']), peer, self.nextIndexMap[peer])
            self.orchestrator.send_peer(peer, msg)

        timeout = randrange(1, 4) * 10 ** (-1 if cfg.config.debug else -2) 
        loop = asyncio.get_event_loop()
        self.update_timer = loop.call_later(timeout, self.send_update)

    def on_peer_response_update(self, peer, msg):
        """Handle peer response to update RPC.
        If successful RPC, try to commit new entries.
        If RPC unsuccessful, backtrack."""
        if not validateDict(msg, self.volatile['publicKeyMap'][peer]):
            return False
        status_code = msg['status_code']
        if status_code == 0:
            if len(self.latestMessageMap[peer]) != 0:
                oldPrePrepare = self.latestMessageMap[peer]['prePrepareIndex'][0]
                oldPrepare = self.latestMessageMap[peer]['prepareIndex'][0]
                # if this message is not the latest message we've received from this peer, then don't substitute
                if (msg['prePrepareIndex'][0] < oldPrePrepare or msg['prepareIndex'][0] < oldPrepare): return
            self.latestMessageMap[peer] = msg
            self.prePrepareIndexMap[peer] = msg['prePrepareIndex'][0] # only store the indices
            self.nextIndexMap[peer] = msg['prePrepareIndex'][0] + 1
            self.prepareIndexMap[peer] = msg['prepareIndex'][0] # only store the indices

            self.prePrepareIndexMap[self.volatile['address']] = self.log.index
            self.nextIndexMap[self.volatile['address']] = self.log.index + 1
            # look at match index for all followers and see where
            # global commit point is
            quorum_size = get_quorum_size(len(self.prepareIndexMap))
            prepareIndex = get_kth_largest(self.prePrepareIndexMap.values(), quorum_size)
            self.log.prepare(prepareIndex)
            self.prepareIndexMap[self.volatile['address']] = self.log.prepareIndex


            commitIndex = get_kth_largest(self.prepareIndexMap.values(), quorum_size)
            self.log.commit(commitIndex)
            self.latestMessageMap[self.volatile['address']] = self.createUpdateMessageForSelf()
            self.send_client_append_response() # TODO make sure client has proof of commit
        elif status_code == 1:
            # to aggressive so move index for this peer back one
            self.nextIndexMap[peer] = max(0, self.nextIndexMap[peer] - 1)
        # status_code = 2 do nothing

    def createUpdateMessageForSelf(self):
        """ create an *update* for ourselves with the latest prePrepareIndex and prepareIndex for the proof """
        msg = {'status_code': 0, 
            'prePrepareIndex': (self.log.index, self.log.getHash(self.log.index)),
            'prepareIndex': (self.log.prepareIndex, self.log.getHash(self.log.prepareIndex))
        }
        signedMsg = signDict(msg, self.volatile['privateKey'])
        return signedMsg

    def on_client_append(self, protocol, msg):
        """Append new entries to Leader log."""
        entry = {
            'term': self.persist['currentTerm'], 
            'data': msg['data'],
            'signature': msg['signature'] # signature over just the data
        }
        if not validate_entries([entry], self.volatile['clientKey']):
            return
        if msg['data']['key'] == 'cluster': # cannot have a key named cluster
            protocol.send({'type': 'result', 'success': False})
        self.log.append_entries([entry], self.log.index + 1) # append to our own log
        if self.log.index in self.waiting_clients:
            self.waiting_clients[self.log.index].append(protocol) # schedule client to be notified
        else:
            self.waiting_clients[self.log.index] = [protocol]      
        signedMsg = self.createUpdateMessageForSelf()
        self.on_peer_response_update(self.volatile['address'], signedMsg)

    def send_client_append_response(self):
        """Respond to client upon commitment of log entries."""
        to_delete = []
        for client_index, clients in self.waiting_clients.items():
            if client_index <= self.log.commitIndex:
                for client in clients:
                    client.send({'type': 'result', 'success': True, 'proof':self.latestMessageMap, 'log':tuple(self.log.log.data), 'index': client_index})  # TODO
                    logger.debug('Sent successful response to client')
                    self.stats.increment('write')
                to_delete.append(client_index)
        for index in to_delete:
            del self.waiting_clients[index]

    def on_client_config(self, protocol, msg):
        """Push new cluster config. When uncommitted cluster changes
        are already present, retries until they are committed
        before proceding."""
        pending_configs = tuple(filter(lambda x: x['data']['key'] == 'cluster',
                                self.log[self.log.commitIndex + 1:]))
        if pending_configs:
            timeout = randrange(1, 4) * 10 ** (0 if cfg.config.debug else -1)
            loop = asyncio.get_event_loop()
            self.config_timer = loop.\
                call_later(timeout, self.on_client_config, protocol, msg)
            return

        success = True
        cluster = set(self.volatile['cluster'])
        peer = (msg['address'], int(msg['port']))
        if msg['action'] == 'add' and peer not in cluster:
            logger.info('Adding node %s', peer)
            cluster.add(peer)
            self.nextIndexMap[peer] = 0
            self.prePrepareIndexMap[peer] = 0
        elif msg['action'] == 'delete' and peer in cluster:
            logger.info('Removing node %s', peer)
            cluster.remove(peer)
            del self.nextIndexMap[peer]
            del self.prePrepareIndexMap[peer]
        else:
            success = False
        if success:
            self.log.append_entries([
                {'term': self.persist['currentTerm'],
                 'data':{'key': 'cluster', 'value': tuple(cluster),
                         'action': 'change'}}],
                self.log.index+1)
            self.volatile['cluster'] = cluster
        protocol.send({'type': 'result', 'success': success})
