import json
import time
import random


with open('configs/parameters/params.json', 'r') as file:
    config = json.load(file)

# Maximum waiting time to wait for a response message
# from the distributed system to a previously sent request.
TIME_TO_RETRY = config['TIME_TO_RETRY']

# Maximum waiting time, to wait for a response message
# from a single server to which the request was sent.
SERVER_TIMEOUT = config['SERVER_TIMEOUT']

# Timeout for a leading server to send an
# AppendEntries message to the other servers in the cluster.
HEARTBEAT_TIMEOUT = config['HEARTBEAT_TIMEOUT']

# Wait interval, to wait for an AppendEntries message sent by the leader
# (A random number contained in the given interval is taken).
ELECTION_INTERVAL = config['ELECTION_INTERVAL']


def random_timeout():
    """ Returns a timeout chosen randomly from a fixed interval (150-300ms). """
    return time.time() + (random.uniform(*ELECTION_INTERVAL))


class Host(object):
    """ A Host is the minimum representation of a node,
    and they are used to facilitate communication with the cluster servers.
    These only contain the id of the node and its address. """

    def __init__(self, node_id, address):
        self.node_id = node_id
        self.address = tuple(address)

    def __str__(self):
        string = "\n"
        string += "Node Id: " + str(self.node_id) + "\n"
        string += "Address: " + str(self.address)
        return string

    def serialize(self):
        return json.dumps(self, default=lambda o: o.__dict__, indent=4)


class Command(object):
    """
    A simple command sent as a request by a client to a server for it to execute in its state machine.
    A command contains:
        • client_address: The address of the client that created it.
        • serial: A unique serial number to identify it.
        • action: The action to be executed (GET/SET).
        • position: The position on which the action will take effect.
        • new_value: The value to be applied (in case the action is 'SET').
        • old_value: The value that the machine contains before executing this command.
        • executed: A field that identifies if the command has already been executed.
    """

    def __init__(self, client_address, serial, action, position, new_value=None, old_value=None, executed=False):
        self.client_address = client_address
        self.serial = serial
        self.action = action
        self.position = position
        self.new_value = new_value
        self.old_value = old_value
        self.executed = executed

    def __str__(self):
        if self.action == "GET":
            return "(" + self.action + ", " + str(self.position) + ")"
        else:
            return "(" + self.action + ", " + str(self.position) + ", " + str(self.new_value) + ")"

    def serialize(self):
        return json.dumps(self, default=lambda o: o.__dict__, indent=4)


class Log(object):
    """ A a log is a field belonging to a server's log record and they are used
    in the process of log replication between all the servers in the cluster.
    These contain a command sent by a client and a term that identifies the moment the log was added. """

    def __init__(self, command, term):
        self.command = command
        self.term = term

    def serialize(self):
        return json.dumps(self, default=lambda o: o.__dict__, indent=4)

    def __str__(self):
        return "(" + str(self.command) + " " + str(self.term) + ")"
