from message import *
from math import floor
from tabulate import tabulate
from utils import *
import threading


class Node(object):
    """ The Node class represents a server with all the necessary
    components for the implementation of the raft consensus algorithm. """

    def __init__(self, node_id, address, state, node_list, socket):

        self.node_id = node_id  # Node unique identifier
        self.address = address  # Address of the node (udp_ip, udp_port)
        self.state = state      # Current node status (LEADER / FOLLOWER / CANDIDATE).
        self.node_list = node_list  # List of all servers in the system [(node_id, address)]
        self.leader_address = None  # Address of the current leader
        self.dictionary_data = None  # Shared resource on the system
        self.socket = socket
        self.lock_leader = threading.Lock()
        self.lock_follower = threading.Lock()

        # Number of nodes required to reach consensus.
        self.quorum_size = floor((len(node_list) + 1) / 2) + 1

        # Timeout to wait for a 'AppendEntries' message
        self.election_timeout = random_timeout()

        # Timeout to sent a 'AppendEntries' message
        self.heartbeat_timeout = None

        # -------------------------------------------------------------------------------------
        # Persistent state on all servers:
        # (Updated on stable storage before responding to RPCs)

        self.current_term = 0  # Latest term server has seen
        self.voted_for = None  # Candidate Id that received vote in current term
        self.votes = set()  # Amount of votes received in current term
        self.logs = list()  # Log entries; each entry contains command for state machine, and term

        # -------------------------------------------------------------------------------------
        # Volatile state on all servers:

        self.commit_index = 0  # Index of highest log entry known to be committed
        self.last_applied = 0  # Index of highest log entry applied to state machine

        # -------------------------------------------------------------------------------------
        # Volatile state on leaders:

        # For each server, index of the next log entry to send to that server
        self.next_index = {node.node_id: 1 for node in node_list}

        # For each server, index of highest log entry known to be replicated on server
        self.match_index = {node.node_id: 0 for node in node_list}

    def __str__(self):
        return tabulate({'Node ID': [str(self.node_id)],
                         'Address': [str(self.address)],
                         'State': [str(self.state)]},
                        headers="keys", tablefmt='fancy_grid', colalign=("center", "center", "center"))

    """ ----------------------------------------------------------------------------------------------------------- """
    """ Utils ----------------------------------------------------------------------------------------------------- """

    def log_term(self, index):
        """ Returns the term of the log corresponding to the assigned position (index). """
        if index < 1 or len(self.logs) < index:
            return 0
        return self.logs[index - 1].term

    def step_down(self, term):
        """ If one server’s current term is smaller than the other’s,
        then it updates its current term to the larger value. If a candidate or leader discovers
        that its term is out of date, it immediately reverts to follower state. """

        self.state = 'FOLLOWER'
        self.current_term = term
        self.voted_for = None
        self.votes = set()
        self.heartbeat_timeout = None

    def advance_commit_index(self):
        """ The leader commits (advance the commitIndex) all log entries
        that has been replicated it on a majority of the servers. """

        # If there exists an N such that N > commitIndex,
        # a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
        match_list = [self.match_index[i] for i in self.match_index]
        match_list.append(len(self.logs))
        match_list.sort(reverse=True)
        n = match_list[self.quorum_size - 1]

        if self.state == "LEADER" and self.log_term(n) == self.current_term:
            self.commit_index = n

    def apply_log_commands(self):
        """ Applies the log commands that are safe to be executed to the state machine.
        Responds to the client request if the server is the leader. """

        # If commitIndex > lastApplied:
        # increment lastApplied, apply log[lastApplied] to state machine
        while self.commit_index > self.last_applied:
            self.last_applied += 1
            cmd = self.logs[self.last_applied-1].command
            self.execute_command(cmd)

            if self.state == "LEADER":
                # Responds to the client
                message = Message("ClientRequest", from_address=tuple(cmd.client_address), to_address=self.address)
                message.from_id = self.node_id
                message.response = "Command executed successfully!"
                message.reply(self.socket)

    def execute_command(self, command):
        """ Applies the current command in the state machine, if it has not already been applied. """
        if not command.executed:
            command.old_value = self.dictionary_data[command.position]
            self.dictionary_data[command.position] = command.new_value
            command.executed = True

    def revert_command(self, command):
        """ Reverts the current command in the state machine, if it has already been applied. """
        if command.executed:
            self.dictionary_data[command.position] = command.old_value

    def save_state(self):
        """ Saves the current node status to a json configuration file. """

        file_name = "configs/server-{0}.json".format(self.node_id)

        data = dict()
        data['node_id'] = self.node_id
        data['port'] = self.address[1]
        data['node_list'] = self.node_list
        data['term'] = self.current_term
        data['voted_for'] = self.voted_for
        data['logs'] = self.logs
        data['dict_data'] = self.dictionary_data

        # 1.Make a Json string
        # 2.Make a Json object
        json_data = json.dumps(data, default=lambda o: o.__dict__, indent=2)
        json_data = json.loads(json_data)

        with open(file_name, 'w', encoding='utf-8') as file:
            json.dump(json_data, file, ensure_ascii=False, indent=2)
            file.close()

    def update_state(self):
        """ updates the current node status
        with information obtained from a json configuration file. """

        file_name = "configs/server-{0}.json".format(self.node_id)

        with open(file_name, "r") as file:
            data = json.loads(file.read())

            self.current_term = int(data['term'])
            self.voted_for = data['voted_for']
            self.dictionary_data = data['dict_data']

            if data['logs']:
                for log in data['logs']:
                    cmd = Command(**log['command'])
                    self.logs.append(Log(cmd, log['term']))

            file.close()

    """ ----------------------------------------------------------------------------------------------------------- """
    """ Leader Election ------------------------------------------------------------------------------------------- """

    def election_timeout_due(self):
        """ If a follower receives no communication over a period of time called the election timeout,
        then it assumes there is no viable leader and begins an election to choose a new leader. """
        if self.election_timeout and (time.time() >= self.election_timeout):
            print("\n>>> Election Timeout <<<")
            self.start_election()

    def start_election(self):
        """ Invoked by candidates to gather votes.
        To begin an election, a follower increments its current term and transitions to candidate state.
        It then votes for itself and issues RequestVote RPCs in parallel
        to each of the other servers in the cluster. """

        # The node increments its current term
        # and transitions to candidate state
        self.state = "CANDIDATE"
        self.current_term += 1

        # Votes for itself
        self.votes = set()
        self.votes.add(self.node_id)
        self.voted_for = self.node_id

        # Send RequestVote
        self.send_request_vote()

        # Election timeout is updated
        self.election_timeout = random_timeout()

    def send_request_vote(self):
        """ Issues RequestVote RPCs to each of the other servers in the cluster. """

        last_log_index = len(self.logs)  # Index of candidate’s last log entry
        last_log_term = self.log_term(last_log_index)  # Term of candidate’s last log entry

        print("\nSending RequestVote")
        for node in self.node_list:
            message = Message('RequestVote', from_address=self.address, to_address=node.address, from_id=self.node_id,
                              term=self.current_term, last_log_index=last_log_index, last_log_term=last_log_term)
            # Send message...
            message.send(self.socket)

    def receive_request_vote(self, req):
        """ Receives a RequestVote RPC from a candidate server.
        Each server will vote for at most one candidate in a given term, on a first-come-first-served basis,
        and it will deny its vote if its own log is more up-to-date than that of the candidate. """

        # Request: [from_id, term, last_log_index, last_log_term]

        granted = False  # True means candidate received vote

        # Server's current term is out of date
        if self.current_term < req.term:
            self.step_down(req.term)

        if (self.voted_for in [None, req.from_id]) and (self.current_term == req.term):

            # The voter denies its vote if its own log is more up-to-date than that of the candidate:
            # If the logs have last entries with different terms, then the log with the later term is more up-to-date.
            # If the logs end with the same term, then whichever log is longer is more up-to-date
            last_log_index = len(self.logs)
            last_log_term = self.log_term(last_log_index)

            if (last_log_term < req.last_log_term) or (last_log_term == req.last_log_term and last_log_index <= req.last_log_index):
                granted = True
                self.voted_for = req.from_id

                # Election timeout is updated
                self.election_timeout = random_timeout()

        # --------------------------------------
        # Send a reply for 'RequestVote' request
        # Arguments:
        req.from_id = self.node_id
        req.term = self.current_term
        req.granted = granted

        # Send message...
        print(" -> Reply To:", req.from_address, end="\n\n")
        req.reply(self.socket)

    def receive_request_vote_reply(self, req):
        """ Receives a response to the RequestVote RPC previously sent.
        A candidate wins an election if it receives votes from a majority
        of the servers in the full cluster for the same term"""

        # Request: [from_id, from_term, granted]

        # Server's current term is out of date
        if self.current_term < req.term:
            self.step_down(req.term)

        if self.state == "CANDIDATE" and self.current_term == req.term:

            # The vote is true and it has not yet voted for me
            if req.granted and req.from_id not in self.votes:

                self.votes.add(req.from_id)

                # The candidate gets most of the votes
                if len(self.votes) >= self.quorum_size:
                    self.become_leader()

    def become_leader(self):
        """ Once a candidate wins an election, it becomes leader.
        It then sends heartbeat messages to all of the other servers
        to establish its authority and prevent new elections. """

        print("\n<<< I AM THE LEADER >>>")
        self.state = "LEADER"
        self.leader_address = self.address

        # It initializes all next_index values to the index just after the last one in its log
        self.next_index = {node.node_id: (len(self.logs) + 1) for node in self.node_list}

        # Stops waiting for election timeout
        self.election_timeout = None

        # Begins to send heartbeats
        self.start_heartbeat()

    """ ----------------------------------------------------------------------------------------------------------- """
    """ Log Replication ------------------------------------------------------------------------------------------- """

    # When sending an AppendEntries RPC, the leader includes the index and term of the entry
    # in its log that immediately precedes the new entries.

    # If the follower does not find an entry in its log with the same index and term,
    # then it refuses the new entries.

    # Whenever AppendEntries returns successfully, the leader knows that the follower’s log
    # is identical to its own log up through the new entries

    # the leader must find the latest log entry where the two logs agree, delete any entries in the
    # follower’s log after that point, and send the follower all of the leader’s entries after that point.
    # All of these actions happen in response o the consistency check performed by AppendEntries RPCs.

    # The leader maintains a nextIndex for each follower, which is the index of the next log entry
    # the leader will send to that follower

    # If a follower’s log is inconsistent with the leader’s, the AppendEntries consistency check will fail
    # in the next AppendEntries RPC. After a rejection, the leader decrements nextIndex and retries
    # the AppendEntries RPC. Eventually nextIndex will reach a point where the leader and follower logs match.
    # When this happens, AppendEntries will succeed, which removes any conflicting entries in the follower’s log
    # and appends entries from the leader’s log (if any)

    # If last log index ≥ nextIndex for a follower:
    # send AppendEntries RPC with log entries starting at nextIndex
    #   • If successful: update nextIndex and matchIndex for
    #   follower (§5.3)
    #   • If AppendEntries fails because of log inconsistency:
    #   decrement nextIndex and retry

    def heartbeat_timeout_due(self):
        """ It's time to send a heartbeat """
        if self.state == "LEADER":
            if self.heartbeat_timeout and (time.time() >= self.heartbeat_timeout):
                self.start_heartbeat()

    def start_heartbeat(self):
        """ Sends AppendEntries RPCs to each of the other servers in the cluster. """

        print("\nSending AppendEntries\n")
        self.save_state()

        for node in self.node_list:
            self.send_append_entries(node)

        self.heartbeat_timeout = time.time() + HEARTBEAT_TIMEOUT

    def send_append_entries(self, node):
        """ AppendEntries RPCs are initiated by leaders
        to replicate log entries and to provide a form of heartbeat. """

        with self.lock_leader:

            # The leader maintains a nextIndex for each follower, which is the index
            # of the next log entry the leader will send to that follower
            prev_index = self.next_index[node.node_id] - 1  # Index of log entry immediately preceding new ones
            prev_term = self.log_term(prev_index)  # Term of prevLogIndex entry (from the leader)

            # If last log index ≥ nextIndex for a follower:
            # send AppendEntries RPC with log entries starting at nextIndex
            if len(self.logs) >= self.next_index[node.node_id]:
                begin_entries = (self.next_index[node.node_id] - 1)
                entries = []
                for log in self.logs[begin_entries:]:
                    cmd = log.command
                    cmd_entry = Command(cmd.client_address, cmd.serial, cmd.action, cmd.position, cmd.new_value)
                    log_entry = Log(cmd_entry, log.term)
                    entries.append(log_entry)
            else:
                entries = []

            commit_index = self.commit_index

            # ------------------------------------------------------------------------------------------------------------

            message = Message('AppendEntries',
                              from_address=self.address,
                              to_address=node.address,
                              leader_address=self.address,
                              from_id=self.node_id,
                              term=self.current_term,
                              prev_index=prev_index,
                              prev_term=prev_term,
                              entries=entries,
                              commit_index=commit_index)
            # Send message...
            message.send(self.socket)

    def receive_append_entries(self, req):
        """ Receives a AppendEntries RPC from the leader. """

        # Request: [term, prev_log_index, prev_log_term, entries, commit_index]

        # 1. Reply false if term < currentTerm
        # 2. Reply false if log does not contain an entry at prevLogIndex whose term matches prevLogTerm.
        # 3. If an existing entry conflicts with a new one (same index but different terms),
        # delete the existing entry and all that follow it.
        # 4. Append any new entries not already in the log
        # 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

        with self.lock_follower:

            success = False  # True if follower contained entry matching prevLogIndex and prevLogTerm
            match_index = 0

            # Server's current term is out of date
            if self.current_term < req.term:
                self.step_down(req.term)

            if self.current_term == req.term:
                self.state = 'FOLLOWER'
                self.leader_address = tuple(req.from_address)

                # Election timeout is updated
                self.election_timeout = random_timeout()

                prev_term = self.log_term(req.prev_index)

                # Log contains an entry at prevLogIndex whose term matches prevLogTerm
                if req.prev_index == 0 or (req.prev_index <= len(self.logs) and prev_term == req.prev_term):
                    success = True

                    index = req.prev_index
                    for i in range(len(req.entries)):

                        # Entry conflicts with a new one (same index but different terms)
                        if self.log_term(index + 1) != req.entries[i].term:

                            # Delete the existing entry and all that follow it
                            while len(self.logs) > index:
                                self.revert_command(self.logs[-1].command)
                                self.logs.pop()

                            # Append any new entries not already in the log
                            self.logs.append(req.entries[i])

                        index += 1

                    match_index = index

                    # If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
                    if req.commit_index > self.commit_index:
                        self.commit_index = min(req.commit_index, len(self.logs))

                    # Execute ready commands
                    self.apply_log_commands()

                    self.save_state()

            # ----------------------------------------
            # Send a reply for 'AppendEntries' request
            # Arguments:
            req.from_id = self.node_id
            req.term = self.current_term
            req.success = success
            req.match_index = match_index

            # Send message...
            print(" -> Reply To:", req.from_address, end="\n\n")
            req.reply(self.socket)

    def receive_append_entries_reply(self, req):
        """ The leader receives a response to the AppendEntries RPC previously sent. """

        # Request: [term, success, match_index]

        with self.lock_leader:

            # Server's current term is out of date
            if self.current_term < req.term:
                self.step_down(req.term)

            if self.state == "LEADER" and self.current_term == req.term:

                # The leader and follower logs match
                if req.success:

                    # Update nextIndex and matchIndex for follower
                    self.match_index[req.from_id] = req.match_index  # max(self.match_index[req.from_id], req.match_index)
                    self.next_index[req.from_id] = req.match_index + 1

                    # Commit all safe log entries
                    self.advance_commit_index()

                    # Execute ready commands
                    # Responds to client requests
                    self.apply_log_commands()

                else:
                    # Follower’s log is inconsistent with the leader’s,
                    # decrements next_index and retries the AppendEntries RPC
                    self.next_index[req.from_id] = max(1, self.next_index[req.from_id] - 1)

    """ ----------------------------------------------------------------------------------------------------------- """
    """ Client Interaction ---------------------------------------------------------------------------------------- """

    def get_log_by_serial(self, serial):
        """ Returns the log that contains this serial number. """
        for log in self.logs:
            if log.command.serial == serial:
                return log
        return None

    def receive_client_request(self, request):
        """ Receives a request from a client.
        The leader accepts log entries from clients, replicates them on other servers,
        and tells servers when it is safe to apply log entries to their state machines.
        • If the server is not the leader, it rejects the client’s request
        and supply information about the most recent leader it has heard.
        • If it receives a command whose serial number has already been executed,
        it responds immediately without re-executing the request.
        • Read-only operations can be handled without writing anything into the log. """

        cmd = request.command

        if self.state == "LEADER":

            with self.lock_leader:

                if cmd.action == "GET":
                    # Reply with dictionary content
                    self.start_heartbeat()
                    time.sleep(SERVER_TIMEOUT/3)
                    request.from_id = self.node_id
                    request.response = self.dictionary_data[cmd.position]
                    request.reply(self.socket)
                else:

                    # Check if the request has already been executed before
                    log = self.get_log_by_serial(cmd.serial)

                    if log:
                        if log.command.executed:
                            request.from_id = self.node_id
                            request.response = "Command already executed successfully!"
                            request.reply(self.socket)
                    else:
                        # Append the new command to the log,
                        # and reply once it has been applied to the state machine
                        self.logs.append(Log(cmd, self.current_term))
                        self.start_heartbeat()

        else:
            # Reply with the leader's address
            request.from_id = self.node_id
            request.leader_address = self.leader_address
            request.reply(self.socket)
