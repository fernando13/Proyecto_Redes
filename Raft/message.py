from utils import *
from tabulate import tabulate


class Message(object):

    def __init__(self, msg_type, from_address, to_address, direction=None, from_id=None, term=None, command=None,
                 response=None, leader_address=None, last_log_index=None, last_log_term=None, granted=None,
                 prev_index=None, prev_term=None, entries=None, commit_index=None, success=None, match_index=None):

        # Common fields
        self.msg_type = msg_type
        self.from_address = from_address
        self.to_address = to_address
        self.direction = direction
        self.from_id = from_id
        self.term = term

        # RequestVote
        self.last_log_index = last_log_index
        self.last_log_term = last_log_term

        # RequestVote-Reply
        self.granted = granted

        # AppendEntries
        self.prev_index = prev_index
        self.prev_term = prev_term
        self.entries = entries
        self.commit_index = commit_index

        # AppendEntries-Reply
        self.success = success
        self.match_index = match_index

        # ClientRequest
        self.command = command

        # ClientRequest-Reply
        self.response = response
        self.leader_address = leader_address

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
            msg.entries = [Log(log) for log in msg.entries]

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
                                 'Entries': [str(self.entries)],
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
                                 'Response': [str(self.response)]},
                                headers="keys", tablefmt='fancy_grid',
                                colalign=("center", "center", "center"))


# if __name__ == '__main__':
    # from_address = ("192.168.0.54", 3004)
    # to_address = ("192.168.0.54", 3005)
    #
    # #  -----------------------------------
    # # RequestVote
    # msg = Message("RequestVote", from_address, to_address,
    #               direction="request", from_id=1, term=2,
    #               last_log_index=0, last_log_term=0)
    # print(msg)
    #
    # #  -----------------------------------
    # # RequestVote-Reply
    # msg.direction = "reply"
    # msg.granted = True
    # print(msg, "\n")
    #
    # #  -----------------------------------
    # # AppendEntries
    # msg = Message("AppendEntries", from_address, to_address,
    #               direction="request", from_id=1, term=2,
    #               prev_index=0, prev_term=0, entries=[], commit_index=1)
    # print(msg)
    #
    # #  -----------------------------------
    # # AppendEntries-Reply
    # msg.direction = "reply"
    # msg.success = False
    # msg.match_index = 4
    # print(msg, "\n")
    #
    # #  -----------------------------------
    # # ClientRequest
    # msg = Message("ClientRequest", from_address, to_address,
    #               direction="request", command=Command(1234, "GET", 2))
    # print(msg)
    #
    # #  -----------------------------------
    # # ClientRequest-Reply
    # msg.direction = "reply"
    # msg.response = "Shared Source"
    # print(msg, "\n")

    # #  -----------------------------------
    # # Json Test
    # msg = Message("ClientRequest", from_address, to_address, direction="request", from_id=1, term=2,
    #               last_log_index=0, last_log_term=0, granted=True,
    #               prev_index=0, prev_term=0, entries=[], commit_index=1, success=False, match_index=1,
    #               command=Command(1234, "GET", 2), response="Shared Source")
    #
    # msg_json = msg.serialize()
    # print(msg_json)
    #
    # msg = deserialize(Message, msg_json)
    # print(msg)
