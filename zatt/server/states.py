import asyncio
import logging
import statistics
from random import randrange
from os.path import join
from zatt.server.utils import PersistentDict, TallyCounter
from zatt.server.log import LogManager
import zatt.server.config as cfg

logger = logging.getLogger(__name__)


class State:
    """Abstract state for subclassing."""
    def __init__(self, old_state=None, orchestrator=None):
        """State is initialized passing an orchestator instance when first
        deployed. Subsequent state changes use the old_state parameter to
        preserve the environment.
        """
        if old_state:
            self.orchestrator = old_state.orchestrator
            self.persist = old_state.persist
            self.volatile = old_state.volatile
            self.log = old_state.log
        else:
            self.orchestrator = orchestrator
            self.persist = PersistentDict(join(cfg.config.getMyStorage(), 'state'),
                                          {'votedFor': None, 'currentTerm': 0})
            self.volatile = {'leaderId': None, 'cluster': cfg.config.cluster.copy(),
                             'address': cfg.config.getMyClusterInfo()}
            self.log = LogManager()
            self._update_cluster()
        self.stats = TallyCounter(['read', 'write', 'append'])

    def data_received_peer(self, peer, msg):
        """Receive peer messages from orchestrator and pass them to the
        appropriate method."""
        logger.debug('Received %s from %s', msg['type'], peer)

        if self.persist['currentTerm'] < msg['term']:
            self.persist['currentTerm'] = msg['term']
            if not type(self) is Follower:
                logger.info('Remote term is higher, converting to Follower')
                self.orchestrator.change_state(Follower)
                self.orchestrator.state.data_received_peer(peer, msg)
                return
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
        if 'cluster' in self.log.compacted.data:
            self.volatile['cluster'] = self.log.compacted.data['cluster']
        for entry in (self.log if entries is None else entries):
            if entry['data']['key'] == 'cluster':
                self.volatile['cluster'] = entry['data']['value']
        self.volatile['cluster'] = tuple(map(tuple, self.volatile['cluster']))


class Follower(State):
    """Follower state."""
    def __init__(self, old_state=None, orchestrator=None):
        """Initialize parent and start election timer."""
        super().__init__(old_state, orchestrator)
        self.persist['votedFor'] = None
        self.restart_election_timer()

    def teardown(self):
        """Stop timers before changing state."""
        self.election_timer.cancel()

    def restart_election_timer(self):
        """Delays transition to the Candidate state by timer."""
        if hasattr(self, 'election_timer'):
            self.election_timer.cancel()

        timeout = randrange(1, 4) * 10 ** (0 if cfg.config.debug else -1)
        loop = asyncio.get_event_loop()
        self.election_timer = loop.\
            call_later(timeout, self.orchestrator.change_state, Candidate)
        logger.debug('Election timer restarted: %s s', timeout)

    def on_peer_request_vote(self, peer, msg):
        """Grant this node's vote to Candidates."""
        term_is_current = msg['term'] >= self.persist['currentTerm']
        can_vote = self.persist['votedFor'] in [tuple(msg['candidateId']),
                                                None]
        index_is_current = (msg['lastLogTerm'] > self.log.term() or
                            (msg['lastLogTerm'] == self.log.term() and
                             msg['lastLogIndex'] >= self.log.index))
        granted = term_is_current and can_vote and index_is_current

        if granted:
            self.persist['votedFor'] = msg['candidateId']
            self.restart_election_timer()

        logger.debug('Voting for %s. Term:%s Vote:%s Index:%s',
                     peer, term_is_current, can_vote, index_is_current)

        response = {'type': 'response_vote', 'voteGranted': granted,
                    'term': self.persist['currentTerm']}
        self.orchestrator.send_peer(peer, response)

    def on_peer_update(self, peer, msg):
        """Manages incoming log entries from the Leader.
        Data from log compaction is always accepted.
        In the end, the log is scanned for a new cluster config.
        """
        self.restart_election_timer()

        term_is_current = msg['term'] >= self.persist['currentTerm']
        prev_log_term_match = msg['prevLogTerm'] is None or\
            self.log.term(msg['prevLogIndex']) == msg['prevLogTerm']
        # check to make sure we are not pre-preparing an index that is already prepared
        does_not_overwite_prepare = self.log.prepareIndex < msg['prevLogIndex'] + 1
        success = term_is_current and prev_log_term_match and does_not_overwite_prepare

        if 'compact_data' in msg:
            assert False
            # self.log = LogManager(compact_count=msg['compact_count'],
            #                       compact_term=msg['compact_term'],
            #                       compact_data=msg['compact_data'])
            # self.volatile['leaderId'] = msg['leaderId']
            # logger.debug('Initialized Log with compact data from Leader')
        elif success:
            self.log.pre_prepare_entries(msg['entries'], msg['prevLogIndex']) # append the entries
            self.log.prepare(msg['leaderPrepare'])
            self.log.commit(msg['leaderCommit']) # update the commit point
            self.volatile['leaderId'] = msg['leaderId']
            logger.debug('Log index is now %s', self.log.index)
            self.stats.increment('append', len(msg['entries']))
        else:
            logger.warning('Could not append entries. cause: %s', 'wrong\
                term' if not term_is_current else 'prev log term mismatch')

        self._update_cluster()

        resp = {'type': 'response_update', 'success': success,
                'term': self.persist['currentTerm'],
                'prePrepareIndex': self.log.index,
                'prepareIndex': self.log.prepareIndex
                }
        self.orchestrator.send_peer(peer, resp)


class Candidate(Follower):
    """Candidate state. Notice that this state subclasses Follower."""
    def __init__(self, old_state=None, orchestrator=None):
        """Initialize parent, increase term, vote for self, ask for votes."""
        super().__init__(old_state, orchestrator)
        self.persist['currentTerm'] += 1
        self.votes_count = 0
        logger.info('New Election. Term: %s', self.persist['currentTerm'])
        self.send_vote_requests()

        def vote_self():
            self.persist['votedFor'] = self.volatile['address']
            self.on_peer_response_vote(
                self.volatile['address'], {'voteGranted': True})
        loop = asyncio.get_event_loop()
        loop.call_soon(vote_self)

    def send_vote_requests(self):
        """Ask peers for votes."""
        logger.info('Broadcasting request_vote')
        msg = {'type': 'request_vote', 'term': self.persist['currentTerm'],
               'candidateId': self.volatile['address'],
               'lastLogIndex': self.log.index,
               'lastLogTerm': self.log.term()}
        self.orchestrator.broadcast_peers(msg)

    def on_peer_update(self, peer, msg):
        """Transition back to Follower upon receiving an update RPC."""
        logger.debug('Converting to Follower')
        self.orchestrator.change_state(Follower)
        self.orchestrator.state.on_peer_update(peer, msg)

    def on_peer_response_vote(self, peer, msg):
        """Register peers votes, transition to Leader upon majority vote."""
        self.votes_count += msg['voteGranted']
        logger.info('Vote count: %s', self.votes_count)
        if self.votes_count > len(self.volatile['cluster']) / 2:
            self.orchestrator.change_state(Leader)


class Leader(State):
    """Leader state."""
    def __init__(self, old_state=None, orchestrator=None):
        """Initialize parent, sets leader variables, start periodic
        append_entries"""
        super().__init__(old_state, orchestrator)
        logger.info('Leader of term: %s', self.persist['currentTerm'])
        self.volatile['leaderId'] = self.volatile['address']
        self.prePrepareIndexMap = {p: 0 for p in self.volatile['cluster']} # latest pre-Prepare point per follower
        self.nextIndexMap = {p: self.log.commitIndex + 1 for p in self.prePrepareIndexMap}
        self.prepareIndexMap = {p: 0 for p in self.volatile['cluster']} # latest prepare position per follower
        self.waiting_clients = {} # log index -> [protocol for a client]
        self.send_update()

        if 'cluster' not in self.log.state_machine:
            self.log.pre_prepare_entries([
                {'term': self.persist['currentTerm'],
                 'data':{'key': 'cluster',
                         'value': tuple(self.volatile['cluster']),
                         'action': 'change'}}],
                self.log.index)
            self.log.commit(self.log.index)

    def teardown(self):
        """Stop timers before changing state."""
        self.update_timer.cancel()
        if hasattr(self, 'config_timer'):
            self.config_timer.cancel()
        for clients in self.waiting_clients.values():
            for client in clients:
                client.send({'type': 'result', 'success': False})
                logger.error('Sent unsuccessful response to client')

    def send_update(self):
        """Send update to the cluster, containing:
        - nothing: if remote node is up to date.
        - compacted log: if remote node has to catch up.
        - log entries: if available.
        Finally schedules itself for later execution."""
        for peer in self.volatile['cluster']:
            if peer == self.volatile['address']:
                continue
            msg = {'type': 'update',
                   'term': self.persist['currentTerm'],
                   'leaderCommit': self.log.commitIndex,
                   'leaderPrepare': self.log.prepareIndex, # all logs up to this point are prepared
                   'leaderId': self.volatile['address'],
                   'prevLogIndex': self.nextIndexMap[peer] - 1,
                   'entries': self.log[self.nextIndexMap[peer]:
                                       self.nextIndexMap[peer] + 100]}
            msg.update({'prevLogTerm': self.log.term(msg['prevLogIndex'])})

            if self.nextIndexMap[peer] <= self.log.compacted.index:
                assert False # should never happen since we're not compacting
                # msg.update({'compact_data': self.log.compacted.data,
                #             'compact_term': self.log.compacted.term,
                #             'compact_count': self.log.compacted.count})

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
        if msg['success']:
            self.prePrepareIndexMap[peer] = msg['prePrepareIndex']
            self.nextIndexMap[peer] = msg['prePrepareIndex'] + 1
            self.prepareIndexMap[peer] = msg['prepareIndex']

            self.prePrepareIndexMap[self.volatile['address']] = self.log.index
            self.nextIndexMap[self.volatile['address']] = self.log.index + 1
            self.prepareIndexMap[self.volatile['address']] = self.log.prepareIndex
            # look at match index for all followers and see where
            # global commit point is
            prepareIndex = statistics.median_low(self.prePrepareIndexMap.values())
            self.log.prepare(prepareIndex)

            commitIndex = statistics.median_low(self.prepareIndexMap.values())
            self.log.commit(commitIndex)
            self.send_client_append_response()
        else:
            # to aggressive so move index for this peer back one
            self.nextIndexMap[peer] = max(0, self.nextIndexMap[peer] - 1)

    def on_client_append(self, protocol, msg):
        """Append new entries to Leader log."""
        entry = {'term': self.persist['currentTerm'], 'data': msg['data']}
        print("entry:", entry)
        if msg['data']['key'] == 'cluster': # cannot have a key named cluster
            protocol.send({'type': 'result', 'success': False})
        self.log.pre_prepare_entries([entry], self.log.index) # append to our own log
        if self.log.index in self.waiting_clients:
            self.waiting_clients[self.log.index].append(protocol) # schedule client to be notified
        else:
            self.waiting_clients[self.log.index] = [protocol]
        self.on_peer_response_update(
            self.volatile['address'], {'success': True,
                                       'prePrepareIndex': self.log.index,
                                       'prepareIndex': self.log.prepareIndex})

    def send_client_append_response(self):
        """Respond to client upon commitment of log entries."""
        to_delete = []
        for client_index, clients in self.waiting_clients.items():
            if client_index <= self.log.commitIndex:
                for client in clients:
                    client.send({'type': 'result', 'success': True})  # TODO
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
            self.log.pre_prepare_entries([
                {'term': self.persist['currentTerm'],
                 'data':{'key': 'cluster', 'value': tuple(cluster),
                         'action': 'change'}}],
                self.log.index)
            self.volatile['cluster'] = cluster
        protocol.send({'type': 'result', 'success': success})
