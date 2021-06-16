from utils import *
import time
from tabulate import tabulate


class Node(object):

    def __init__(self, node_id, local_address, node_type, quorum_size, node_list, socket):
        self.node_id = node_id
        # self.node_id = int(str(local_address[0]).replace(".", "") + str(local_address[1]))
        self.local_address = local_address  # (UDP_IP, UDP_PORT)
        self.node_type = node_type  # Proposer/Acceptor/Learner
        self.quorum_size = quorum_size
        self.node_list = node_list  # List of all nodes in the system --> (node_id, address, node_type)
        self.socket = socket
        self.logs = [None for x in range(10)]  # index ==> [command, status]
        self.dictionary_data = {1: "okote", 2: "waldo", 3: "pijui", 4: "hector"}

    def find_unknown_entry(self, start_index=0):
        """Find s the smallest log position that is unknown to this node"""
        index = start_index
        for index in range(start_index, len(self.logs)):
            if not self.logs[index]:  # or self.logs[index][1] == "UNKNOWN"
                return index

        self.logs.append(None)
        return index + 1

    def __str__(self):
        return tabulate({'Node Type': [self.node_type],
                         'Node ID': [str(self.node_id)],
                         'Address': [str(self.local_address)]},
                        headers="keys", tablefmt='fancy_grid', colalign=("center", "center", "center"))


class Proposer(Node):
    leader = False  # Indicates if this node is the leader
    leader_address = None  # Address of the current leader

    # round_number = 0  # Round number 'N'
    # round = None  # Round --> tuple(round_number, node_id)

    # proposed_value = None  # Proposed value 'v'

    # last_accepted_round = None  # Highest round reported by the Acceptors
    # promises_received = None  # Promises received from the Acceptors

    time_quorum = 1  # Maximum time to wait for a quorum
    waiting_quorum = None  # Indicates if the proposer is waiting for a quorum (None/time)

    skip_phase_one = False

    instances = dict()   # instances[index] = [round_number, proposed_value, last_accepted_round, promises_received]

    # if index not in self.instances:
    #     self.instances[index] = [promised_round, accepted_round, accepted_value]

    def __init__(self, node_id, local_address, quorum_size, node_list, socket, leader=False, leader_address=None):
        super().__init__(node_id, local_address, 'PROPOSER', quorum_size, node_list, socket)
        self.leader = leader
        self.leader_address = leader_address

    def __str__(self):
        return super(Proposer, self).__str__()

    """---------------------------------------------------------------------------------"""
    """---------------------------------Bully Algorithm---------------------------------"""

    time_answer = 2  # Maximum time to wait for a 'answer' message
    waiting_answer = None  # Indicates if the node is waiting for a 'answer' message

    time_send_heartbeat = 2  # Time to wait to send a 'heartbeat' message
    last_heartbeat = None  # Indicates when the last heartbeat message was sent

    time_heartbeat = time_send_heartbeat * 2  # Maximum time to wait for a 'heartbeat' message from the leader
    waiting_heartbeat = time.time()           # Indicates if the node is waiting for a 'heartbeat' message

    def self_proclaimed_leader(self):
        """ If the node has the largest ID then it proclaims himself a leader """
        larger_nodes = [host for host in self.node_list if host.type == "PROPOSER" and host.node_id > self.node_id]
        if not larger_nodes:
            print("\nI proclaim myself leader")
            self.send_coordinator()

    def send_election(self):
        """ Sends a 'election' message to all nodes with ID larger than mine,
        when it detected that the leader is down """

        proposers_list = [node for node in self.node_list if node.type == 'PROPOSER' and node.node_id > self.node_id]
        data = [self.local_address]

        self.waiting_answer = time.time()

        print("\nSending Election")
        for node in proposers_list:
            message = Message('ELECTION', paxos_data=data)
            message = message.serialize().encode()

            # send message...
            print(" -> To:", node.address)
            self.socket.sendto(message, node.address)

    def send_answer(self, address):
        message = Message('ANSWER', paxos_data=[self.local_address])
        message = message.serialize().encode()

        # send message...
        self.socket.sendto(message, address)

    def send_coordinator(self):
        """ Indicates to the other nodes that it is the leader """

        print("\n>>> ON LEADERSHIP <<<")
        self.leader = True
        self.leader_address = tuple(self.local_address)
        self.waiting_heartbeat = None
        self.last_heartbeat = time.time()

        proposers_list = [node for node in self.node_list if node.type == 'PROPOSER']
        data = [self.local_address]

        print("\nSending Coordinator")
        for node in proposers_list:
            message = Message('COORDINATOR', paxos_data=data)
            message = message.serialize().encode()

            # send message...
            print(" -> To:", node.address)
            self.socket.sendto(message, node.address)

    def receive_election(self, from_address):
        """ It responds with a 'answer' message and starts a new election """
        self.send_answer(from_address)
        self.send_election()

    def receive_answer(self):
        """ Received an 'answer' message from the last election it started,
        and now it must wait for a 'coordinator' message of the next leader """
        self.waiting_answer = None

        # Wait a while for the elections ends
        self.waiting_heartbeat = time.time()

    def receive_coordinator(self, new_leader):
        """ Choose the process that sent the message as the leader """
        # Si yo soy mayor entonces enviar una eleccion?
        self.leader = False
        self.leader_address = new_leader
        self.last_heartbeat = None
        self.waiting_answer = None
        self.waiting_heartbeat = time.time()

    def send_heartbeat(self):
        """ If this process is the leader, it must send a 'heartbeat' message
        to the other nodes every so often to let them know that it is still alive"""

        if self.leader and (time.time() - self.last_heartbeat >= self.time_send_heartbeat):

            proposers_list = [node for node in self.node_list if node.type == 'PROPOSER']

            # Send the data of this node
            data = [self.node_id, self.local_address]

            # print("\nSending Heartbeat")
            for node in proposers_list:
                message = Message('HEARTBEAT', paxos_data=data)
                message = message.serialize().encode()

                # send message...
                # print(" -> To:", node.address)
                self.socket.sendto(message, node.address)

            self.last_heartbeat = time.time()

    def receive_heartbeat(self, from_id, from_address):
        if not self.leader and self.node_id < from_id:
            if not self.leader_address:
                self.leader_address = from_address
        else:
            self.send_election()

        self.waiting_heartbeat = time.time()

    def heartbeat_timeout(self):
        """ Timed out to wait for an 'heartbeat' message from the leader """
        if not self.leader and self.waiting_heartbeat and (time.time() - self.waiting_heartbeat >= self.time_heartbeat):
            print("\nLeader is down")
            self.waiting_heartbeat = None
            self.send_election()

    def answer_timeout(self):
        """ Timed out to wait for an 'answer' message """
        if self.waiting_answer is not None and (time.time() - self.waiting_answer >= self.time_answer):
            self.waiting_answer = None
            print("\nAnswer timeout")
            self.send_coordinator()

    """---------------------------------------------------------------------------------"""
    """---------------------------------------------------------------------------------"""

    def receive_request(self, client_data):
        """ Receives a request from a client.
        If the node is not the leader, forwards the message to the current leader"""
        if self.leader:
            if client_data[1] == "GET":
                key = int(client_data[2])
                client_address = tuple(client_data[0])

                message = str(self.dictionary_data[key])

                # Send response message to the client...
                self.socket.sendto(message.encode(), client_address)

            if self.skip_phase_one:
                index = self.find_unknown_entry(0)

                # If that instance has no data, I create a new.
                if index not in self.instances:
                    self.instances[index] = [(0, self.node_id), client_data, None, set()]

                self.send_accept(index, (0, self.node_id), client_data)
            else:
                self.prepare(client_data)

        else:

            message = Message('REQUEST', client_data=client_data)
            message = message.serialize().encode()

            # send message...
            self.socket.sendto(message, tuple(self.leader_address))

    """----------------------------------------------------------------------"""

    def prepare(self, client_value, start_index=0):
        """ Sends a prepare request to all Acceptors """
        # [round_number, proposed_value, last_accepted_round, promises_received]

        index = self.find_unknown_entry(start_index)

        # If that instance has no data, I create a new.
        if index not in self.instances:
            self.instances[index] = [(0, self.node_id), client_value, None, set()]

        round = self.instances[index][0]
        round = (round[0] + 1, round[1])  # round =+ 1

        self.instances[index][0] = round

        self.waiting_quorum = time.time()

        self.send_prepare(index, round)

        # -------------------------------------------------------------
        # ------------------------ Old Version ------------------------
        # self.promises_received = set()
        # self.waiting_quorum = time.time()
        #
        # self.round_number += 1
        # self.round = (self.round_number, self.node_id)
        #
        # self.send_prepare(self.round)

    def send_prepare(self, index, round):
        """ Broadcasts a Prepare message to all Acceptors """

        acceptors_list = [node for node in self.node_list if node.type == 'ACCEPTOR']

        data = [index, round]

        print("\nSending Prepare")
        for node in acceptors_list:
            message = Message('PREPARE', paxos_data=data)
            message = message.serialize().encode()

            # send message...
            print(" -> To:", node.address)
            self.socket.sendto(message, node.address)

    """----------------------------------------------------------------------"""

    def receive_promise(self, from_id, index, round, prev_accepted_round, prev_accepted_value):
        """ Called when a Promise message is received from an Acceptor """

        """ If a Proposer receives a majority of Promises from a Quorum of Acceptors,
        it needs to set a value v to its proposal. If any Acceptors had previously accepted any proposal,
        then they'll have sent their values to the Proposer, who now must set the value of its proposal,
        to the value associated with the highest proposal number reported by the Acceptors.
        If none of the Acceptors had accepted a proposal up to this point, then the Proposer may choose the value
        it originally wanted to propose. """

        # [round_number, proposed_value, last_accepted_round, promises_received]

        # Get data from the given instance.
        round_i = self.instances[index][0]
        proposed_value = self.instances[index][1]
        last_accepted_round = self.instances[index][2]
        promises_received = self.instances[index][3]

        # Ignore the message if it's for an old proposal or we have already received a response from this Acceptor.
        if round[0] != round_i[0] or from_id in promises_received:
            return

        promises_received.add(from_id)

        # Save the highest proposal number reported by the Acceptors.
        if prev_accepted_round is not None:
            if last_accepted_round is None or prev_accepted_round[0] > last_accepted_round[0]:
                last_accepted_round = prev_accepted_round
                proposed_value = prev_accepted_value

        # Save the current instance data.
        self.instances[index] = [round_i, proposed_value, last_accepted_round, promises_received]

        # Receives a majority of Promises from a Quorum of Acceptors.
        if len(promises_received) == self.quorum_size:

            self.waiting_quorum = None
            self.skip_phase_one = True

            if proposed_value is not None:
                self.send_accept(index, round_i, proposed_value)

        # -------------------------------------------------------------
        # ------------------------ Old Version ------------------------

        # # Ignore the message if it's for an old proposal or we have already received a response from this Acceptor.
        # if round[0] != self.round[0] or from_id in self.promises_received:
        #     return
        #
        # self.promises_received.add(from_id)
        #
        # # Save the highest proposal number reported by the Acceptors.
        # if prev_accepted_round is not None:
        #     if self.last_accepted_round is None or prev_accepted_round[0] > self.last_accepted_round[0]:
        #         self.last_accepted_round = prev_accepted_round
        #         self.proposed_value = prev_accepted_value
        #
        # # Receives a majority of Promises from a Quorum of Acceptors.
        # if len(self.promises_received) == self.quorum_size:
        #
        #     self.waiting_quorum = None
        #     self.skip_phase_one = True
        #
        #     if self.proposed_value is not None:
        #         self.send_accept(self.round, self.proposed_value)

    def send_accept(self, index, round, proposal_value):
        """ Broadcasts an Accept! message to all Acceptors """

        acceptors_list = [node for node in self.node_list if node.type == 'ACCEPTOR']

        data = [index, round, proposal_value]

        print("\nSending Accept")
        for node in acceptors_list:
            message = Message('ACCEPT', paxos_data=data)
            message = message.serialize().encode()

            # send message...
            print(" -> To:", node.address)
            self.socket.sendto(message, node.address)

    """----------------------------------------------------------------------"""

    def send_replicate(self, data):

        proposers_list = [node for node in self.node_list if node.type == 'PROPOSER']

        for node in proposers_list:
            message = Message('REPLICATE', paxos_data=data)
            message = message.serialize().encode()

            # send message...
            self.socket.sendto(message, node.address)

    def quorum_timeout(self):
        """ Timed out to wait for a quorum. Not enough Acceptors have answered on time. """
        if self.waiting_quorum is not None and (time.time() - self.waiting_quorum >= self.time_quorum):
            self.waiting_quorum = None
            print("\nQuorum TimeOut")


class Acceptor(Node):
    # promised_round = None  # Last promised proposal number.
    # accepted_round = None  # Last accepted proposal number.
    # accepted_value = None  # Last accepted value.

    instance = dict()  # instances[index] = [promised_round, accepted_round, accepted_value]

    def __init__(self, node_id, local_address, node_list, socket):
        super().__init__(node_id, local_address, 'ACCEPTOR', None, node_list, socket)

    def receive_prepare(self, from_address, index, round):
        """ Called when a Prepare message is received from a Proposer """

        """ If N is higher than every previous proposal number received, from any of the Proposers, by the Acceptor,
        then the Acceptor must return a message "Promise", to the Proposer, to ignore all future proposals
        having a number less than n. If the Acceptor accepted a proposal at some point in the past, it must
        include the previous proposal number and the corresponding accepted value, in its response to the Proposer.
        Otherwise the Acceptor can ignore the received proposal.
        For the sake of optimization, sending a denial (Nack) response would tell the Proposer
        that it can stop its attempt to create consensus with proposal n. """

        # Get data from the given instance.
        if index not in self.instance:
            self.instance[index] = [None, None, None]

        # Get data from the given instance.
        promised_round = self.instance[index][0]
        accepted_round = self.instance[index][1]
        accepted_value = self.instance[index][2]

        # This round is higher than every previous round received.
        if not promised_round or round[0] > promised_round[0]:
            promised_round = round

            # Save the current instance data.
            self.instance[index] = [promised_round, accepted_round, accepted_value]

            # Send a Promise message.
            self.send_promise(from_address, index, round, accepted_round, accepted_value)

        # -------------------------------------------------------------
        # ------------------------ Old Version ------------------------
        # if not self.promised_round or round[0] > self.promised_round[0]:
        #     self.promised_round = round
        #     self.send_promise(from_address, round, self.accepted_round, self.accepted_value)

    def send_promise(self, proposer_address, index, round, accepted_round, accepted_value):
        """ Sends a Promise message to the specified Proposer """

        data = [self.node_id, index, round, accepted_round, accepted_value]

        message = Message('PROMISE', paxos_data=data)
        message = message.serialize().encode()

        print("\nSending Promise")
        print(" -> To:", proposer_address)

        # send message...
        self.socket.sendto(message, proposer_address)

    """----------------------------------------------------------------------"""

    def receive_accept(self, from_address, index, round, value):
        """ Called when an Accept! message is received from a Proposer """

        # If that instance has no data, I create a new.
        if index not in self.instance:
            self.instance[index] = [None, None, None]

        # Get data from the given instance.
        promised_round = self.instance[index][0]
        accepted_round = self.instance[index][1]
        accepted_value = self.instance[index][2]

        # [promised_round, accepted_round, accepted_value]
        print(self.instance[index])

        # It must accept it if and only if it has not already promised (in Phase 1b of the Paxos protocol) to only
        # consider proposals having an identifier greater than n.
        if not promised_round or round[0] >= promised_round[0]:
            # Register the value v (of the just received Accept message) as the accepted value (of the Protocol),
            # and send an Accepted message to the Proposer and every Learner.

            accepted_round = round
            accepted_value = value

            # Save the current instance data.
            self.instance[index] = [promised_round, accepted_round, accepted_value]

            # Send a Accepted message.
            self.send_accepted(from_address, index, round, accepted_value)

        # -------------------------------------------------------------
        # ------------------------ Old Version ------------------------
        # # It must accept it if and only if it has not already promised (in Phase 1b of the Paxos protocol) to only
        # # consider proposals having an identifier greater than n.
        # if round[0] >= self.promised_round[0]:
        #     # Register the value v (of the just received Accept message) as the accepted value (of the Protocol),
        #     # and send an Accepted message to the Proposer and every Learner.
        #     self.accepted_round = round
        #     self.accepted_value = value
        #     self.send_accepted(from_address, round, self.accepted_value)

    def send_accepted(self, proposer_address, index, round, accepted_value):
        """ Broadcasts an Accepted message to all Learners and to the Proposer """

        learner_list = [node for node in self.node_list if node.type == 'LEARNER']
        # learner_list.append(Host(None, from_address, 'PROPOSER'))  # Add the proposer address to the list.

        data = [proposer_address, self.node_id, index, round, accepted_value]

        print("\nSending Accepted")
        for node in learner_list:
            message = Message('ACCEPTED', paxos_data=data)
            message = message.serialize().encode()

            # send message...
            print(" -> To:", node.address)
            self.socket.sendto(message, node.address)


class Learner(Node):
    accepted_rounds = None  # maps round_number => [accept_count, value]
    final_value = None  # Final value accepted
    final_round = None  # Final round accepted

    instance = dict()  # instances[index] = [accepted_rounds, final_value, final_round]

    def __init__(self, node_id, local_address, quorum_size, node_list, socket):
        super().__init__(node_id, local_address, 'LEARNER', quorum_size, node_list, socket)

    def receive_accepted(self, proposer_address, from_id, index, round, accepted_value):
        """
        Called when an Accepted message is received from an acceptor
        """
        round_number = round[0]

        # If that instance has no data, I create a new.
        if index not in self.instance:
            self.instance[index] = [dict(), None, None]

        # Get data from the given instance.
        accepted_rounds = self.instance[index][0]
        final_round = self.instance[index][1]
        final_value = self.instance[index][2]

        # The paxos protocol is already done.****
        if final_value is not None:
            return

        # If the current round is not registered, I register it.
        if round_number not in accepted_rounds:
            accepted_rounds[round_number] = [set(), accepted_value]

        # Increments the quantity of 'Accepted' messages received for this round.
        # accepted_rounds[round_num] => [accept_count, value]
        current_round = accepted_rounds[round_number]
        current_round[0].add(from_id)

        # Receives a majority of 'Accepted' messages from a Quorum of Acceptors.
        if len(current_round[0]) >= self.quorum_size:
            final_value = accepted_value
            final_round = round_number

            # Save the current instance data.
            self.instance[index] = [accepted_rounds, final_round, final_value]

            self.on_resolution(proposer_address, index, round_number, accepted_value)
        else:
            # Save the current instance data.
            self.instance[index] = [accepted_rounds, final_round, final_value]

        # -------------------------------------------------------------
        # ------------------------ Old Version ------------------------
        # round_number = round[0]
        #
        # # The paxos protocol is already done.
        # if self.complete():
        #     return
        #
        # if self.accepted_rounds is None:
        #     self.accepted_rounds = dict()
        #
        # # If the current round is not registered, I register it.
        # if round_number not in self.accepted_rounds:
        #     self.accepted_rounds[round_number] = [set(), accepted_value]
        #
        # # proposal_id => [accept_count, value]
        # current_round = self.accepted_rounds[round_number]
        # current_round[0].add(from_id)
        #
        # # Receives a majority of Accepted messages from a Quorum of Acceptors.
        # if len(current_round[0]) >= self.quorum_size:
        #     self.final_value = accepted_value
        #     self.final_round = round_number
        #     self.accepted_rounds = None
        #
        #     self.on_resolution(round_number, accepted_value)

    def on_resolution(self, proposer_address, index, round, value):
        """ Called when a resolution is reached """

        data = [index, value]
        message = Message('RESOLUTION', paxos_data=data)
        message = message.serialize().encode()

        # send message...
        self.socket.sendto(message, tuple(proposer_address))

        # -------------------------------------------------------------
        # ------------------------ Old Version ------------------------
        # if value[1] == 'SET':
        #     key = int(value[2])
        #     new_value = value[3]
        #     self.dictionary_data[key] = new_value
        #
        # message = str(self.dictionary_data)
        #
        # # send message...
        # self.socket.sendto(message.encode(), tuple(value[0]))

