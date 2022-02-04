from utils import *
from tabulate import tabulate


class Message(object):
    """ The Message class represents a message which will be used
    for communication between the different servers of the cluster and the clients.
    These messages contain all the necessary components to be able to carry out a
    correct communication in the implementation of the raft consensus algorithm. """

    def __init__(self, msg_type, from_address, to_address, direction=None, from_id=None, term=None, command=None,
                 response=None, leader_address=None, last_log_index=None, last_log_term=None, granted=None,
                 prev_index=None, prev_term=None, entries=None, commit_index=None, success=None, match_index=None):

        # Common fields
        self.msg_type = msg_type  # Type of message to send (RequestVote, AppendEntries, ClientRequest)
        self.from_address = from_address  # Sender's address
        self.to_address = to_address  # Recipient's address
        self.direction = direction  # Way of the message (Request / Reply)
        self.from_id = from_id  # Id of the server that sends the message
        self.term = term  # Term number owned by the server that sends the message

        # RequestVote
        self.last_log_index = last_log_index  # Index of the last candidate record entry
        self.last_log_term = last_log_term  # Term of last candidate record entry

        # RequestVote-Reply
        self.granted = granted  # True means the candidate received the vote

        # AppendEntries
        self.prev_index = prev_index  # Log entry index immediately prior to the new ones
        self.prev_term = prev_term  # Term of the prev_index entry
        self.entries = entries  # Log entries to store (empty for heartbeats)
        self.commit_index = commit_index  # Leader's commit index.

        # AppendEntries-Reply
        self.success = success  # True if the follower contains an entry that matches prev_index and prev_term
        self.match_index = match_index  # Log entry index in which the follower matched the leader

        # ClientRequest
        self.command = command  # Operation requested by the client to be executed in the distributed system

        # ClientRequest-Reply
        self.response = response  # Response message to the operation requested by the client previously
        self.leader_address = leader_address  # Address of the last leader known to the server

    def send(self, socket):
        self.direction = "request"
        socket.sendto(self.serialize().encode(), self.to_address)

    def reply(self, socket):
        self.direction = "reply"
        socket.sendto(self.serialize().encode(), tuple(self.from_address))

    def serialize(self):
        return json.dumps(self, default=lambda o: o.__dict__, indent=4)

    @staticmethod
    def deserialize(json_data):
        msg = Message(**json.loads(json_data))

        if msg.command:
            msg.command = Command(**msg.command)

        if msg.entries:
            msg.entries = [Log(Command(**log['command']), log['term']) for log in msg.entries]

        return msg

    def __str__(self):

        if self.msg_type == "RequestVote":
            if self.direction == "request":
                return tabulate({'Type': [self.msg_type],
                                 'Node': [str(self.from_id)],
                                 'Term': [str(self.term)],
                                 'Last_log_index': [str(self.last_log_index)],
                                 'Last_log_term': [str(self.last_log_term)]},
                                headers="keys", tablefmt='fancy_grid',
                                colalign=("center", "center", "center", "center", "center"))
            else:
                return tabulate({'Type': [self.msg_type + "-Reply"],
                                 'Node': [str(self.from_id)],
                                 'Term': [str(self.term)],
                                 'Granted': [self.granted]},
                                headers="keys", tablefmt='fancy_grid',
                                colalign=("center", "center", "center", "center"))

        if self.msg_type == "AppendEntries":
            if self.direction == "request":
                return tabulate({'Type': [self.msg_type],
                                 'Node': [str(self.from_id)],
                                 'Term': [str(self.term)],
                                 'Prev_index': [str(self.prev_index)],
                                 'Prev_term': [str(self.prev_term)],
                                 'Entries': [[str(log) for log in self.entries]],
                                 'Commit_index': [str(self.commit_index)]},
                                headers="keys", tablefmt='fancy_grid',
                                colalign=("center", "center", "center", "center", "center", "center", "center"))
            else:
                return tabulate({'Type': [self.msg_type + "-Reply"],
                                 'Node': [str(self.from_id)],
                                 'Term': [str(self.term)],
                                 'Success': [str(self.success)],
                                 'Match_index': [str(self.match_index)]},
                                headers="keys", tablefmt='fancy_grid',
                                colalign=("center", "center", "center", "center", "center"))

        if self.msg_type == "ClientRequest":
            if self.direction == "request":
                return tabulate({'Type': [self.msg_type],
                                 'Command': [str(self.command)]},
                                headers="keys", tablefmt='fancy_grid',
                                colalign=("center", "center"))
            else:
                return tabulate({'Type': [self.msg_type + "-Reply"],
                                 'Node': [str(self.from_id)],
                                 'Response': [str(self.response)],
                                 'Leader': [str(self.leader_address)]},
                                headers="keys", tablefmt='fancy_grid',
                                colalign=("center", "center", "center", "center"))
