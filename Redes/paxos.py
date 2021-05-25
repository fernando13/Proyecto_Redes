from utils import *
import time


class Node(object):

    def __init__(self, node_id, local_address, node_type, quorum_size, node_list, socket):
        self.node_id = node_id
        # self.node_id = int(str(local_address[0]).replace(".", "") + str(local_address[1]))
        self.local_address = local_address  # (UDP_IP, UDP_PORT)
        self.node_type = node_type  # Proposer/Acceptor/Learner
        self.quorum_size = quorum_size
        self.node_list = node_list  # List of all nodes in the system --> (address, node_type)
        self.socket = socket
        self.log = None   # index ==> [command]?


class Proposer(Node):
    leader = False          # Indicates if this node is the leader
    leader_address = None   # Address of the current leader

    round_number = 0  # Round number 'N'
    round = None      # Round --> tuple(round_number, node_id)

    proposed_value = None  # Proposed value 'v'

    last_accepted_round = None  # Highest round reported by the Acceptors
    promises_received = None    # Promises received from the Acceptors

    time_quorum = 1         # Maximum time to wait for a quorum
    waiting_quorum = None   # Indicates if the proposer is waiting for a quorum (None/time)

    def __init__(self, node_id, local_address, node_type, quorum_size, node_list, socket, leader=False, leader_address=None):
        super().__init__(node_id, local_address, node_type, quorum_size, node_list, socket)
        self.leader = leader
        self.leader_address = leader_address

    """---------------------------------------------------------------------------------"""
    """---------------------------------Bully Algorithm---------------------------------"""

    time_answer = 1         # Maximum time to wait for a 'answer' message
    waiting_answer = None   # Indicates if the node is waiting for a 'answer' message

    time_heartbeat = 4                # Maximum time to wait for a 'heartbeat' message from the leader
    waiting_heartbeat = time.time()   # *Indicates if the node is waiting for a 'heartbeat' message

    time_send_heartbeat = 3   # Time to wait to send a 'heartbeat' message
    last_heartbeat = None     # Indicates when the last heartbeat message was sent

    def send_election(self):
        """ Sends a 'election' message to all nodes with ID larger than mine,
        when it detected that the leader is down """

        proposers_list = [node for node in self.node_list if node.type == 'PROPOSER']  # and node_id > self.node_id

        self.waiting_answer = time.time()

        for node in proposers_list:
            message = Message('ELECTION')
            message = message.serialize().encode()

            # send message...
            self.socket.sendto(message, node.address)

    def send_answer(self, address):
        message = Message('ANSWER')
        message = message.serialize().encode()

        # send message...
        self.socket.sendto(message, address)

    def send_coordinator(self):
        """ indicates to the other nodes that it is the leader """

        print("\nI am the Leader!")

        self.leader = True
        self.leader_address = self.local_address
        self.last_heartbeat = time.time()

        proposers_list = [node for node in self.node_list if node.type == 'PROPOSER']
        data = [self.local_address]

        for node in proposers_list:
            message = Message('COORDINATOR', paxos_data=data)
            message = message.serialize().encode()

            # send message...
            self.socket.sendto(message, node.address)

    def receive_election(self, from_address):
        """ It responds with a 'answer' message and starts a new election """
        self.send_answer(from_address)
        self.send_election()

    def receive_answer(self):
        self.waiting_answer = None
        time.sleep(1)

    def receive_coordinator(self, new_leader):
        """ Choose the process that sent the message as the leader """
        self.leader = False
        self.leader_address = new_leader
        self.last_heartbeat = None
        self.waiting_heartbeat = time.time()

    def send_heartbeat(self):
        """ If this process is the leader, it must send a 'heartbeat' message
        to the other nodes every so often to let them know that it is still alive"""

        if self.leader and (time.time() - self.last_heartbeat >= self.time_send_heartbeat):
            print("\n Sending Heartbeat")

            proposers_list = [node for node in self.node_list if node.type == 'PROPOSER']  # and node_id > self.node_id

            for node in proposers_list:
                message = Message('HEARTBEAT')
                message = message.serialize().encode()

                print("\n To: ", node.address)
                # send message...
                self.socket.sendto(message, node.address)

            self.last_heartbeat = time.time()

    def receive_heartbeat(self):
        self.waiting_heartbeat = time.time()

    def heartbeat_timeout(self):
        """ Timed out to wait for an 'heartbeat' message from the leader """
        if not self.leader and self.waiting_heartbeat and (time.time() - self.waiting_heartbeat >= self.time_heartbeat):
            self.waiting_heartbeat = None
            self.send_election()
            print("Leader is Down")

    def answer_timeout(self):
        """ Timed out to wait for an 'answer' message """
        if self.waiting_answer is not None and (time.time() - self.waiting_answer >= self.time_answer):
            self.waiting_answer = None
            self.waiting_heartbeat = None
            print("\nAnswer Timeout")
            self.send_coordinator()

    """---------------------------------------------------------------------------------"""
    """---------------------------------------------------------------------------------"""

    def receive_request(self, client_data):
        self.proposed_value = client_data
        self.prepare()

        # if self.leader:
        #     self.proposed_value = client_data
        #     self.prepare()
        # else:
        #
        #     message = Message('REQUEST', client_data=client_data)
        #     message = message.serialize().encode()
        #
        #     # send message...
        #     self.socket.sendto(message, self.leader_address)

    """----------------------------------------------------------------------"""

    # Multi-paxos
    def set_proposal(self, value):
        """ Sets the proposal value for this node iff this node is not already aware of
        another proposal having already been accepted. """
        if self.proposed_value is None:
            self.proposed_value = value

            if self.leader:
                self.send_accept(self.round, value)

    def prepare(self):
        """ Sends a prepare request to all Acceptors as the first step
        in attempting to acquire leadership of the Paxos instance """

        self.promises_received = set()
        self.waiting_quorum = time.time()

        self.round_number += 1
        self.round = (self.round_number, self.node_id)

        self.send_prepare(self.round)

    def send_prepare(self, round):
        """ Broadcasts a Prepare message to all Acceptors """

        acceptors_list = [node for node in self.node_list if node.type == 'ACCEPTOR']

        data = [round]

        for node in acceptors_list:
            print("Sending 'Prepare' to: ", node.address)
            message = Message('PREPARE', paxos_data=data)  # self.local_address, node[0],
            message = message.serialize().encode()

            # send message...
            self.socket.sendto(message, node.address)

    """----------------------------------------------------------------------"""

    def receive_promise(self, from_id, round, prev_accepted_round, prev_accepted_value):
        """ Called when a Promise message is received from an Acceptor """

        """ If a Proposer receives a majority of Promises from a Quorum of Acceptors,
        it needs to set a value v to its proposal. If any Acceptors had previously accepted any proposal,
        then they'll have sent their values to the Proposer, who now must set the value of its proposal,
        to the value associated with the highest proposal number reported by the Acceptors.
        If none of the Acceptors had accepted a proposal up to this point, then the Proposer may choose the value
        it originally wanted to propose. """

        # Ignore the message if it's for an old proposal or we have already received a response from this Acceptor.
        if round[0] != self.round[0] or from_id in self.promises_received:  # if self.leader or
            return

        self.promises_received.add(from_id)

        # Save the highest proposal number reported by the Acceptors.
        if prev_accepted_round is not None:
            if self.last_accepted_round is None or prev_accepted_round[0] > self.last_accepted_round[0]:
                self.last_accepted_round = prev_accepted_round
                self.proposed_value = prev_accepted_value

        # Receives a majority of Promises from a Quorum of Acceptors.
        if len(self.promises_received) == self.quorum_size:

            # Multi-paxos ----------------
            # self.leader = True
            # self.on_leadership_acquired()
            # ----------------------------

            self.waiting_quorum = None

            if self.proposed_value is not None:
                self.send_accept(self.round, self.proposed_value)

    def send_accept(self, round, proposal_value):
        """ Broadcasts an Accept! message to all Acceptors """

        acceptors_list = [node for node in self.node_list if node.type == 'ACCEPTOR']

        data = [round, proposal_value]

        for node in acceptors_list:
            message = Message('ACCEPT', paxos_data=data)  # self.local_address, node[0],
            message = message.serialize().encode()

            # send message...
            self.socket.sendto(message, node.address)

    """----------------------------------------------------------------------"""

    # Multi-paxos
    def on_leadership_acquired(self):
        """ Called when leadership has been acquired. This is not a guaranteed position.
        Another node may assume leadership at any time and it's even possible that another
        may have successfully done so before this callback is executed. Use this method with care.
        The safe way to guarantee leadership is to use a full Paxos instance
        with the resolution value being the UID of the leader node.
        To avoid potential issues arising from timing and/or failure,
        the election result may be restricted to a certain time window.
        Prior to the end of the window the leader may attempt to re-elect itself to extend its term in office. """

        proposers_list = [node for node in self.node_list if node.type == 'PROPOSER']

        data = [self.local_address]

        for node in proposers_list:
            message = Message('LEADERSHIP', paxos_data=data)
            message = message.serialize().encode()

            # send message...
            self.socket.sendto(message, node.address)

    def quorum_timeout(self):
        """ Timed out to wait for a quorum. Not enough Acceptors have answered on time. """
        if self.waiting_quorum is not None and (time.time() - self.waiting_quorum >= self.time_quorum):
            self.waiting_quorum = None
            print("Quorum TimeOut")


class Acceptor(Node):
    promised_round = None  # Last promised proposal number.
    accepted_round = None  # Last accepted proposal number.
    accepted_value = None  # Last accepted value.

    def receive_prepare(self, from_address, round):
        """ Called when a Prepare message is received from a Proposer """

        """ If N is higher than every previous proposal number received, from any of the Proposers, by the Acceptor,
        then the Acceptor must return a message "Promise", to the Proposer, to ignore all future proposals
        having a number less than n. If the Acceptor accepted a proposal at some point in the past, it must
        include the previous proposal number and the corresponding accepted value, in its response to the Proposer.
        Otherwise the Acceptor can ignore the received proposal.
        For the sake of optimization, sending a denial (Nack) response would tell the Proposer
        that it can stop its attempt to create consensus with proposal n. """

        if not self.promised_round or round[0] > self.promised_round[0]:
            self.promised_round = round
            self.send_promise(from_address, round, self.accepted_round, self.accepted_value)

    def send_promise(self, proposer_address, round, accepted_round, accepted_value):
        """ Sends a Promise message to the specified Proposer """

        data = [self.node_id, round, accepted_round, accepted_value]

        message = Message('PROMISE', paxos_data=data)  # self.local_address, proposer_address,
        message = message.serialize().encode()

        # send message...
        self.socket.sendto(message, proposer_address)  # ----> proposer_id es la address?

    """----------------------------------------------------------------------"""

    def receive_accept(self, from_address, round, value):
        """ Called when an Accept! message is received from a Proposer """

        # It must accept it if and only if it has not already promised (in Phase 1b of the Paxos protocol) to only
        # consider proposals having an identifier greater than n.
        if round[0] >= self.promised_round[0]:

            # Register the value v (of the just received Accept message) as the accepted value (of the Protocol),
            # and send an Accepted message to the Proposer and every Learner.
            self.accepted_round = round
            self.accepted_value = value
            self.send_accepted(from_address, round, self.accepted_value)

    def send_accepted(self, from_address, round, accepted_value):
        """ Broadcasts an Accepted message to all Learners and to the Proposer """

        learner_list = [node for node in self.node_list if node.type == 'LEARNER']
        learner_list.append(Host(None, from_address, 'PROPOSER'))  # Add the proposer address to the list.

        data = [self.node_id, round, accepted_value]

        for node in learner_list:
            message = Message('ACCEPTED', paxos_data=data)  # self.local_address, node[0],
            message = message.serialize().encode()

            # send message...
            self.socket.sendto(message, node.address)


class Learner(Node):

    accepted_rounds = None  # maps round_number => [accept_count, value]

    final_value = None  # Final value accepted
    final_round = None  # Final round accepted

    dictionary_data = {1: "okote", 2: "waldo", 3: "pijui", 4: "hector"}

    def complete(self):
        return self.final_value is not None

    def receive_accepted(self, from_id, round, accepted_value):
        """
        Called when an Accepted message is received from an acceptor
        """

        round_number = round[0]

        # The paxos protocol is already done.
        if self.complete():
            return

        if self.accepted_rounds is None:
            self.accepted_rounds = dict()

        # If the current round is not registered, I register it.
        if round_number not in self.accepted_rounds:
            self.accepted_rounds[round_number] = [set(), accepted_value]

        # proposal_id => [accept_count, value]
        current_round = self.accepted_rounds[round_number]
        current_round[0].add(from_id)

        # Receives a majority of Accepted messages from a Quorum of Acceptors.
        if len(current_round[0]) >= self.quorum_size:
            self.final_value = accepted_value
            self.final_round = round_number
            self.accepted_rounds = None

            self.on_resolution(round_number, accepted_value)

    def on_resolution(self, proposer_id, value):
        """ Called when a resolution is reached """
        if value[1] == 'SET':
            key = int(value[2])
            new_value = value[3]
            self.dictionary_data[key] = new_value

        message = str(self.dictionary_data)

        # send message...
        self.socket.sendto(message.encode(), tuple(value[0]))
