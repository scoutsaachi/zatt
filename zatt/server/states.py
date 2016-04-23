import asyncio
import random
from os.path import join
from collections import Counter, OrderedDict
from .persistence import PersistentDict, LogManager
from .logger import logger
from .config import config


class State:
    def __init__(self, config, old_state=None, orchestrator=None):
        self.config = config
        if old_state:
            self.orchestrator = old_state.orchestrator
            self.persist = old_state.persist
            self.volatile = old_state.volatile
            self.log = old_state.log
        else:
            self.orchestrator = orchestrator
            self.persist = PersistentDict(join(config['storage'], 'state'),
                                          {'votedFor': None, 'currentTerm': 0})
            self.volatile = {'leaderId': None, 'Id': config['id']}
            self.log = LogManager()

    def data_received_peer(self, peer_id, message):
        logger.debug('Received {} from {}'.format(message['type'], peer_id))

        if message['term'] > self.persist['currentTerm']:
            self.persist['currentTerm'] = message['term']
            if not type(self) is Follower:
                logger.info('Remote term is higher, converting to Follower')
                self.orchestrator.change_state(Follower)
                self.orchestrator.state.data_received_peer(peer_id, message)
                return
        else:
            method = getattr(self, 'handle_peer_' + message['type'], None)
            if method:
                method(peer_id, message)
            else:
                logger.info('Unrecognized message from {}: {}'.format(peer_id,
                                                                      message))

    def data_received_client(self, protocol, message):
        method = getattr(self, 'handle_client_' + message['type'], None)
        if method:
            method(protocol, message)
        else:
            logger.info('Unrecognized message from {}: {}'.
                        format(protocol.transport.get_extra_info('peername'),
                               message))

    def handle_client_append(self, protocol, message):
        message = {'type': 'redirect',
                   'leader': self.config['cluster'][self.volatile['leaderId']]}
        protocol.send(message)
        logger.info('Redirect client {}:{} to leader'.format(
                     *protocol.transport.get_extra_info('peername')))

    def handle_client_get(self, protocol, message):
        protocol.send(self.log.state_machine.data)

    def handle_client_diagnostic(self, protocol, message):
        msg = {'status': self.__class__.__name__,
               'persist': {'votedFor': self.persist['votedFor'],
                           'currentTerm': self.persist['currentTerm']},
               'volatile': {'leaderId': self.volatile['leaderId'],
                            'Id': self.volatile['Id']},
               'log': {'commitIndex': self.log.commitIndex,
                       'log': self.log.log.__dict__,
                       'state_machine': self.log.state_machine.__dict__,
                       'compacted': self.log.compacted.__dict__}
               }
        if type(self) is Leader:
            msg.update({'leaderStatus':
                        {'waiting_clients': self.waiting_clients,
                         'netIndex': self.nextIndex}})
        protocol.send(msg)


class Follower(State):
    def __init__(self, config, old_state=None, orchestrator=None):
        super().__init__(config, old_state, orchestrator)
        self.persist['votedFor'] = None
        self.restart_election_timer()

    def teardown(self):
        self.election_timer.cancel()

    def restart_election_timer(self):
        if hasattr(self, 'election_timer'):
            self.election_timer.cancel()

        timeout = random.randrange(1, 4)
        timeout = timeout * 10 ** (0 if self.config['debug'] else -1)

        loop = asyncio.get_event_loop()
        self.election_timer = loop.call_later(timeout,
                                              self.orchestrator.change_state,
                                              Candidate)
        logger.debug('Election timer restarted: {}s'.format(timeout))

    def handle_peer_request_vote(self, peer_id, message):
        self.restart_election_timer()
        term_is_current = message['term'] >= self.persist['currentTerm']
        can_vote = self.persist['votedFor'] in [None, message['candidateId']]
        index_is_current = message['lastLogIndex'] >= self.log.index
        vote = term_is_current and can_vote and index_is_current

        if vote:
            self.persist['votedFor'] = message['candidateId']

        logger.debug('Voting for {}. Term:{} Vote:{} Index:{}'.format(peer_id,
                     term_is_current, can_vote, index_is_current))

        response = {'type': 'response_vote', 'voteGranted': vote,
                    'term': self.persist['currentTerm']}
        self.orchestrator.send_peer(peer_id, response)

    def handle_peer_append_entries(self, peer_id, message):
        self.restart_election_timer()

        term_is_current = message['term'] >= self.persist['currentTerm']
        prev_log_term_match = message['prevLogTerm'] is None or\
            self.log.term(message['prevLogIndex']) == message['prevLogTerm']
        success = term_is_current and prev_log_term_match

        if 'compact_data' in message:
            self.log = LogManager(compact_count=message['compact_count'],
                                  compact_term=message['compact_term'],
                                  compact_data=message['compact_data'])
            self.volatile['leaderId'] = message['leaderId']
            logger.debug('Initialized Log with compact data from Leader')
        elif success:
            self.log.append_entries(message['entries'], message['prevLogIndex'])
            self.log.commit(message['leaderCommit'])
            self.volatile['leaderId'] = message['leaderId']
            logger.debug('Log index is now {}'.format((self.log.index)))
        else:
            logger.warning('Couldnt append entries. cause: {}'.format('wrong\
                term' if not term_is_current else 'prev log term mismatch'))

        resp = {'type': 'response_append', 'next_index': self.log.index + 1,
                'term': self.persist['currentTerm']}
        self.orchestrator.send_peer(peer_id, resp)


class Candidate(Follower):
    def __init__(self, config, old_state=None, orchestrator=None):
        super().__init__(config, old_state, orchestrator)
        self.persist['currentTerm'] += 1
        self.persist['votedFor'] = self.volatile['Id']
        self.votes_count = 1
        logger.info('New Election. Term: '+str(self.persist['currentTerm']))
        self.send_vote_requests()

    def send_vote_requests(self):
        logger.info('Broadcasting request_vote')
        message = {'type': 'request_vote', 'term': self.persist['currentTerm'],
                   'candidateId': self.volatile['Id'],
                   'lastLogIndex': self.log.index,
                   'lastLogTerm': self.log.term()}
        self.orchestrator.broadcast_peers(message)

    def handle_peer_append_entries(self, peer_id, message):
        logger.debug('Converting to Follower')
        self.orchestrator.change_state(Follower)
        self.orchestrator.state.handle_peer_append_entries(peer_id, message)

    def handle_peer_response_vote(self, peer_id, message):
        self.votes_count += message['voteGranted']
        logger.info('Vote count: {}'.format(self.votes_count))
        if self.votes_count > len(self.config['cluster']) / 2:
            self.orchestrator.change_state(Leader)


class Leader(State):
    def __init__(self, config, old_state=None, orchestrator=None):
        super().__init__(config, old_state, orchestrator)
        logger.info('Leader of term: {}'.format(self.persist['currentTerm']))
        self.nextIndex = {x: self.log.commitIndex + 1
                          for x in self.config['cluster']}
        self.send_append_entries()
        self.waiting_clients = {}

    def teardown(self):
        self.append_timer.cancel()

    def send_append_entries(self):
        for peer_id in config['cluster']:
            if peer_id == self.volatile['Id']:
                continue
            msg = {'type': 'append_entries',
                   'term': self.persist['currentTerm'],
                   'leaderCommit': self.log.commitIndex,
                   'leaderId': self.volatile['Id'],
                   'prevLogIndex': self.nextIndex[peer_id] - 1,
                   'entries': self.log[self.nextIndex[peer_id]:
                                       self.nextIndex[peer_id] + 2]}
            msg.update({'prevLogTerm': self.log.term(msg['prevLogIndex'])})

            if self.nextIndex[peer_id] <= self.log.compacted.index:
                msg.update({'compact_data': self.log.compacted.data,
                            'compact_term': self.log.compacted.term,
                            'compact_count': self.log.compacted.count})

            logger.debug('Sending {} entries to {}. Start index {}'
                         .format(len(msg['entries']), peer_id,
                                 self.nextIndex[peer_id]))
            self.orchestrator.send_peer(peer_id, msg)

        timeout = random.randrange(1, 4)
        timeout = timeout * 10 ** (-1 if self.config['debug'] else -2)
        loop = asyncio.get_event_loop()
        self.append_timer = loop.call_later(timeout, self.send_append_entries)

    def handle_peer_response_append(self, peer_id, message):
        self.nextIndex[peer_id] = message['next_index']

        self.nextIndex[self.volatile['Id']] = self.log.index + 1
        index_counter = Counter(map(lambda x: x-1, self.nextIndex.values()))
        index_counter = OrderedDict(reversed(sorted(index_counter.items())))
        total = 0
        for index, count in index_counter.items():
            total += count
            if total / len(config['cluster']) > 0.5:
                self.log.commit(index)
                self.send_client_append_response()
                break

    def handle_client_append(self, protocol, message):
        entry = {'term': self.persist['currentTerm'], 'data': message['data']}
        self.log.append_entries([entry], self.log.index)
        if self.log.index in self.waiting_clients:
            self.waiting_clients[self.log.index].append(protocol)
        else:
            self.waiting_clients[self.log.index] = [protocol]

    def send_client_append_response(self):
        to_delete = []
        for client_index, clients in self.waiting_clients.items():
            if client_index >= self.log.commitIndex:
                for client in clients:
                    client.send({'type': 'result', 'success': True})  # TODO
                    logger.debug('Sent successful response to client')
                to_delete.append(client_index)
        for index in to_delete:
            del self.waiting_clients[client_index]
