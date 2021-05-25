import json


class Message(object):
    def __init__(self, msg_type, client_data=None, paxos_data=None):  # origin, destination,
        # self.origin = origin             # Message from...
        # self.destination = destination   # Message to...
        self.msg_type = msg_type         # REQUEST/PREPARE/PROMISE/ACCEPT/ACCEPTED/RESOLUTION/RESPONSE/NACK
        self.client_data = client_data   # (client_address, operation, position, value)
        self.paxos_data = paxos_data

    def __str__(self):
        msg = "\n----- " + self.msg_type + " -------------------------------"
        # msg += "\nFrom: " + str(self.origin)
        # msg += "\nTo:   " + str(self.destination)
        msg += "\nClient Data: " + str(self.client_data)
        msg += "\nPaxos  Data: " + str(self.paxos_data)
        msg += "\n---------------------------------------------"
        return msg

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

    # @staticmethod
    # def get_host_list(list_json_hosts):
    #     # return [Host.deserialize(host) for host in json.loads(list_json_hosts)]
    #     return json.loads(list_json_hosts)


def read_file(json_file):
    file = open(json_file, "r")
    content = file.read()
    data = json.loads(content)

    node_id = int(data["node_id"])
    port = int(data["port"])
    leader = eval(data["leader"])
    leader_address = tuple(data["leader_address"])
    node_list = [Host(**node) for node in data["node_list"]]

    # print("ID: ", node_id)
    # print("Is leader: ", leader)
    # print("Leader Address: ", leader_address)

    return node_id, port, leader, leader_address, node_list


if __name__ == '__main__':
    read_file("config/proposer-1.json")

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

    # print(Host.get_host_list(7))