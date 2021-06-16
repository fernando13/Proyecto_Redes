import json
from tabulate import tabulate


class Message(object):
    def __init__(self, msg_type, client_data=None, paxos_data=None):
        # self.origin = origin             # Message from...
        # self.destination = destination   # Message to...
        self.msg_type = msg_type         # REQUEST/PREPARE/PROMISE/ACCEPT/ACCEPTED/RESOLUTION/RESPONSE/NACK
        self.client_data = client_data   # (client_address, operation, position, value)
        self.paxos_data = paxos_data

    def __str__(self):

        msg = "\nMessage received: \n"

        # -------------------------------------------------------------------------------------------------------------

        if self.msg_type == "REQUEST":
            msg += tabulate(
                [[self.msg_type, self.client_data[0], (self.client_data[1], self.client_data[2], self.client_data[3])]],
                headers=["Type", "Client address", "Operation"], tablefmt='fancy_grid',
                colalign=("center", "center", "center"))
            return msg

        # -------------------------------------------------------------------------------------------------------------

        if self.msg_type == "PREPARE":
            msg += tabulate([[self.msg_type,
                              "Index: " + str(self.paxos_data[0]),
                              "Round: " + str(self.paxos_data[1])]],
                            tablefmt='fancy_grid', colalign=("center", "center", "center"))
            return msg

        # -------------------------------------------------------------------------------------------------------------

        if self.msg_type == "PROMISE":
            msg += tabulate(
                [[self.msg_type, self.paxos_data[0], self.paxos_data[1], self.paxos_data[2], self.paxos_data[3],
                  (self.paxos_data[4][1:] if self.paxos_data[4] else None)]],
                headers=["Type", "Node ID", "Index", "Round", "Accepted round", "Accepted value"],
                tablefmt='fancy_grid', missingval='N/A',
                colalign=("center", "center", "center", "center", "center", "center"))
            return msg

        # -------------------------------------------------------------------------------------------------------------

        if self.msg_type == "ACCEPT":
            msg += tabulate([[self.msg_type, self.paxos_data[0], self.paxos_data[1], self.paxos_data[2][1:]]],
                            headers=["Type", "Index", "Round", "Proposed value"], tablefmt='fancy_grid',
                            colalign=("center", "center", "center", "center"))
            return msg

        # -------------------------------------------------------------------------------------------------------------

        if self.msg_type == "ACCEPTED":
            msg += tabulate([[self.msg_type, self.paxos_data[1], self.paxos_data[2], self.paxos_data[3], self.paxos_data[4][1:]]],
                            headers=["Type", "Node ID", "Index", "Round", "Accepted value"], tablefmt='fancy_grid',
                            colalign=("center", "center", "center", "center", "center"))
            return msg

        # -------------------------------------------------------------------------------------------------------------

        if self.msg_type == "COORDINATOR":
            msg += tabulate([[self.msg_type, "New leader: " + str(self.paxos_data[0])]],
                            tablefmt='fancy_grid', colalign=("center", "center"))
            return msg

        # -------------------------------------------------------------------------------------------------------------

        if self.msg_type == "ELECTION":
            msg += tabulate([[self.msg_type, "From: " + str(self.paxos_data[0])]],
                            tablefmt='fancy_grid', colalign=("center", "center"))
            return msg

        # -------------------------------------------------------------------------------------------------------------

        if self.msg_type == "ANSWER":
            msg += tabulate([[self.msg_type, "From: " + str(self.paxos_data[0])]],
                            tablefmt='fancy_grid', colalign=("center", "center"))
            return msg

        return "\n---" + self.msg_type + "---"

    def serialize(self):
        return json.dumps(self, default=lambda o: o.__dict__, indent=4)

    @staticmethod
    def deserialize(json_data):
        return Message(**json.loads(json_data))


class Host(object):

    def __init__(self, node_id, address, type):
        self.node_id = node_id
        self.address = tuple(address)
        self.type = type

    def __str__(self):
        host = "\nNode Id: " + str(self.node_id)
        host += "\nAddress: " + str(self.address)
        host += "\nType: " + str(self.type)
        return host

    @staticmethod
    def set_id(address):
        return int(str(address[0]).replace(".", "") + str(address[1]))

    def serialize(self):
        return json.dumps(self, default=lambda o: o.__dict__, indent=4)

    @staticmethod
    def deserialize(json_data):
        return Host(**json.loads(json_data))


def read_file(json_file):
    file = open(json_file, "r")
    content = file.read()
    data = json.loads(content)

    node_id = int(data["node_id"])
    port = int(data["port"])
    node_list = [Host(**node) for node in data["node_list"]]

    return node_id, port, node_list


# if __name__ == '__main__':
    # read_file("config/proposer-1.json")
    #
    # msg = Message('REQUEST', ('get', 5, 'waldo'))
    #
    # msgJson = msg.serialize()
    # print(msgJson)
    #
    # msg2 = Message.deserialize(msgJson)
    # print(msg2)

    # h = Host(1, ('192.168.0.0', 3001), 'PROPOSER')
    #
    # hostJson = h.serialize()
    # print(hostJson)
    #
    # h2 = Host.deserialize(hostJson)
    # print(h2)
