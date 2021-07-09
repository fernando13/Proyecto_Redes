from utils import *
from message import *
import time
from tabulate import tabulate
import random
from math import floor


class Node(object):

    def __init__(self, node_id, address, state, node_list, socket):
        self.node_id = node_id
        self.address = address  # (UDP_IP, UDP_PORT)
        self.state = state  # LEADER/FOLLOWER/CANDIDATE
        self.quorum_size = floor((len(node_list) + 1) / 2)
        self.node_list = node_list  # List of all nodes in the system --> (node_id, address, node_type)
        self.socket = socket
        self.dictionary_data = {1: "okote", 2: "waldo", 3: "pijui", 4: "hector"}

        self.leader_address = None  # Address of the current leader

        # -----------------------------------------------------
        # Persistent state on all servers:
        # (Updated on stable storage before responding to RPCs)

        self.current_term = 0  # Latest term server has seen
        self.voted_for = None  # Candidate Id that received vote in current term
        self.votes = set()     # Amount of votes received in current term
        self.logs = []         # Log entries; each entry contains command for state machine, and term

        # -----------------------------------------------------
        # Volatile state on all servers:

        self.commit_index = 0  # Index of highest log entry known to be committed
        self.last_applied = 0  # Index of highest log entry applied to state machine

        # -----------------------------------------------------
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

    # heartbeat_timeout = 0
    # election_timeout = 0

    heartbeat_time = 2     # Timeout to sent a heartbeat message (AppendEntries)
    last_heartbeat = None  # Time of last heartbeat sent

    election_time = random.randint(4, 8)  # Timed out to wait for a heartbeat message
    waiting_election = time.time()        # Time from which it's began to wait a heartbeat message

    def log_term(self, index):
        if index < 1 or len(self.logs) < index:
            return 0
        return self.logs[index - 1].term

    def step_down(self, term):
        """ if one server’s current term is smaller than the other’s, then it updates its current
        term to the larger value. If a candidate or leader discovers
        that its term is out of date, it immediately reverts to follower state."""

        self.state = 'FOLLOWER'
        self.current_term = term
        self.voted_for = None
        self.votes = set()
        self.last_heartbeat = None

    def update_election_time(self):
        self.election_time = random.randint(3, 8) # now + random
        self.waiting_election = time.time()

    """ ----------------------------------------------------------------------------------------------------------- """

    def start_election(self):
        """ Invoked by candidates to gather votes """

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
        self.update_election_time()

    def send_request_vote(self):

        last_log_index = len(self.logs)                # Index of candidate’s last log entry
        last_log_term = self.log_term(last_log_index)  # Term of candidate’s last log entry

        print("\nSending RequestVote")
        for node in self.node_list:

            message = Message('RequestVote', from_address=self.address, to_address=node.address, from_id=self.node_id,
                              term=self.current_term, last_log_index=last_log_index, last_log_term=last_log_term)
            # Send message...
            message.send(self.socket)

    def election_timeout(self):
        """ Timed out to wait for a heartbeat message """
        if self.waiting_election is not None and (time.time() - self.waiting_election >= self.election_time):
            print("\n>>> Election Timeout <<<")
            self.start_election()

    def receive_request_vote(self, req):
        # Request: [from_id, term, last_log_index, last_log_term]

        granted = False  # True means candidate received vote

        # Server's current term is out of date
        if self.current_term < req.term:
            self.step_down(req.term)

        if (self.voted_for in [None, req.from_id]) and (self.current_term == req.term):

            # The voter denies its vote if its own log is more up-to-date than that of the candidate.
            # If the logs have last entries with different terms, then the log with the later term is more up-to-date.
            # If the logs end with the same term, then whichever log is longer is more up-to-date
            last_log_index = len(self.logs)
            last_log_term = self.log_term(last_log_index) # (self.logs[-1].term if self.logs else 0)

            if (last_log_term < req.last_log_term) or \
                    (last_log_term == req.last_log_term and last_log_index <= req.last_log_index):

                granted = True
                self.voted_for = req.from_id

                # Election timeout is updated
                self.update_election_time()

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
        print("\n<<< I AM THE LEADER >>>")
        self.state = "LEADER"
        self.leader_address = self.address

        # It initializes all next_index values to the index just after the last one in its log
        self.next_index = {node.node_id: (len(self.logs) + 1) for node in self.node_list}

        # Stops waiting for election timeout
        self.waiting_election = None

        # Begins to send heartbeats
        self.start_heartbeat()

    """ ----------------------------------------------------------------------------------------------------------- """

    def start_heartbeat(self):
        print("\nSending AppendEntries")
        for node in self.node_list:
            self.send_append_entries(node)

        self.last_heartbeat = time.time()

    def send_append_entries(self, node):

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

        # When a leader first comes to power, it initializes all nextIndex values
        # to the index just after the last one in its log ???????????

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
        #   decrement nextIndex and retry (§5.3)

        # ------------------------------------------------------------------------------------------------------------
        # My Version -------------------------------------------------------------------------------------------------

        # The leader maintains a nextIndex for each follower, which is the index of the next log entry
        # the leader will send to that follower
        prev_index = self.next_index[node.node_id] - 1  # Index of log entry immediately preceding new ones
        prev_term = self.log_term(prev_index)           # Term of prevLogIndex entry (from the leader)

        # there are entries to send
        if len(self.logs) >= self.next_index[node.node_id]:
            begin_entries = (self.next_index[node.node_id] - 1)
            entries = self.logs[begin_entries:-1]
        else:
            entries = []

        commit_index = self.commit_index

        # ------------------------------------------------------------------------------------------------------------

        message = Message('AppendEntries', from_address=self.address, to_address=node.address,
                          from_id=self.node_id, term=self.current_term,
                          prev_index=prev_index, prev_term=prev_term,
                          entries=entries, commit_index=commit_index)
        # Send message...
        message.send(self.socket)

    def receive_append_entries(self, req):
        # Request: [term, prev_log_index, prev_log_term, entries, commit_index]

        # 1. Reply false if term < currentTerm (§5.1)
        # 2. Reply false if log doesn’t contain an entry at prevLogIndex
        # whose term matches prevLogTerm (§5.3)
        # 3. If an existing entry conflicts with a new one (same index
        # but different terms), delete the existing entry and all that
        # follow it (§5.3)
        # 4. Append any new entries not already in the log
        # 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

        success = False  # True if follower contained entry matching prevLogIndex and prevLogTerm
        match_index = 0

        # Server's current term is out of date
        if self.current_term < req.term:
            self.step_down(req.term)

        if self.current_term == req.term:
            self.state = 'FOLLOWER'
            self.leader_address = tuple(req.from_address)

            # Election timeout is updated
            self.update_election_time()

            prev_term = self.log_term(req.prev_index)

            # Log contains an entry at prev_log_index whose term matches prev_log_term
            if req.prev_index == 0 or (req.prev_index <= len(self.logs) and prev_term == req.prev_term):
                success = True

                index = req.prev_index
                for i in range(len(req.entries)):

                    # Entry conflicts with a new one (same index but different terms)
                    if self.logs[index] != req.entries[i][1]:  # comparar con el termino

                        # Delete the existing entry and all that follow it
                        while len(self.logs) > index:
                            self.revert_command(self.logs[-1].command)
                            self.logs.pop()

                        # Append any new entries not already in the log
                        self.logs[index] = (req.entries[i])
                        # ***if not self.logs[index] or (self.logs[index].term != req.entries[i][1]):
                        # if self.log_term[index + 1]!= req.entries[i][1]):

                    index += 1

                match_index = index

                # If leaderCommit > commitIndex, set commitIndex =
                if req.commit_index > self.commit_index:
                    self.commit_index = min(req.commit_index, len(self.logs))

                # If commitIndex > lastApplied: increment lastApplied, apply
                # log[lastApplied] to state machine (§5.3)
                while self.commit_index > self.last_applied:
                    self.last_applied += 1
                    cmd = self.logs[self.last_applied]
                    self.execute_command(cmd)

        # ----------------------------------------
        # Send a reply for 'AppendEntries' request
        # Arguments:
        req.term = self.current_term
        req.success = success
        req.match_index = match_index

        # Send message...
        print(" -> Reply To:", req.from_address, end="\n\n")
        req.reply(self.socket)

    def receive_append_entries_reply(self, req):
        # Request: [term, success, match_index]

        # Server's current term is out of date
        if self.current_term < req.term:
            self.step_down(req.term)

        if self.state == "LEADER" and self.current_term == req.term:
            if req.success:
                # Update next_index and match_index for follower
                self.match_index[req.from_id] = req.match_index  # max(self.match_index[req.from_id], req.match_index)
                self.next_index[req.from_id] = req.match_index + 1

                #  If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
                # and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
                match_list = [self.match_index[i] for i in self.match_index]
                match_list.append(len(self.logs))
                match_list.sort(reverse=True)
                n = match_list[self.quorum_size]
                if self.state == "LEADER" and self.log_term(n) == self.current_term:
                    self.commit_index = n

                # If commitIndex > lastApplied: increment lastApplied, apply
                # log[lastApplied] to state machine (§5.3)
                while self.commit_index > self.last_applied:
                    self.last_applied += 1
                    cmd = self.logs[self.last_applied]
                    self.execute_command(cmd)

            else:
                # Follower’s log is inconsistent with the leader’s,
                # Decrements next_index and retries the AppendEntries RPC.
                self.next_index[req.from_id] = max(1, self.next_index[req.from_id] - 1)

    def heartbeat_timeout(self):
        """ It's time to send a heartbeat """

        if self.state == "LEADER":
            if self.last_heartbeat and (time.time() - self.last_heartbeat >= self.heartbeat_time):
                self.start_heartbeat()

    """ ----------------------------------------------------------------------------------------------------------- """

    def receive_client_request(self, request):
        """ Receives a request from a client.
        If the node is not the leader, forwards the message to the current leader"""

        cmd = request.command

        if self.state == "LEADER":

            # encolar...............

            if cmd.action == "GET":
                # Reply with dictionary content
                request.response = self.dictionary_data[cmd.position]
                request.reply(self.socket)
            else:
                # Check if the request has already been sent before
                for log in reversed(self.logs):
                    if log.command.serial == cmd.serial:
                        request.response = "Success!"
                        request.reply(self.socket)
                        return

                self.logs.append(Log(cmd, self.current_term))  # Como le aviso al cliente...?

        else:
            # Reply with the leader's direction
            request.leader_address = self.leader_address
            request.reply(self.socket)

    def execute_command(self, command):
        command.old_value = self.dictionary_data[command.position]
        self.dictionary_data[command.position] = command.new_value

    def revert_command(self, command):
        self.dictionary_data[command.position] = command.old_value

