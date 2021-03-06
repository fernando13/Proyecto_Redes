import collections
import json
from tabulate import tabulate


def deserialize(object_type, json_data):
    return object_type(**json.loads(json_data))


def read_file(json_file):
    file = open(json_file, "r")
    content = file.read()
    data = json.loads(content)

    node_id = int(data["node_id"])
    port = int(data["port"])
    node_list = [Host(**node) for node in data["node_list"]]

    return node_id, port, node_list


Log = collections.namedtuple('Log', ['command', 'term'])


class Host(object):

    def __init__(self, node_id, address, node_type):
        self.node_id = node_id
        self.address = tuple(address)
        self.node_type = node_type

    def __str__(self):
        string = "\n"
        string += "Node Id: " + str(self.node_id) + "\n"
        string += "Address: " + str(self.address) + "\n"
        string += "Type: " + self.node_type
        return string

    def serialize(self):
        return json.dumps(self, default=lambda o: o.__dict__, indent=4)


class Command(object):

    def __init__(self, action, position, new_value=None, old_value=None, serial=None):
        self.action = action
        self.position = position
        self.new_value = new_value
        self.old_value = old_value
        self.serial = serial

    def __str__(self):
        if self.action == "GET":
            return "(" + self.action + ", " + str(self.position) + ")"
        else:
            return "(" + self.action + ", " + str(self.position) + ", " + str(self.new_value) + ")"

    def serialize(self):
        return json.dumps(self, default=lambda o: o.__dict__, indent=4)


# if __name__ == '__main__':

    # list = []
    # print(len(list))

    # host_list = [Host(4, ('192.168.0.0', 3001), "FOLLOWER"),
    #              Host(5, ('192.168.0.0', 3001), "FOLLOWER"),
    #              Host(6, ('192.168.0.0', 3001), "FOLLOWER")]
    #
    # # di = {k: v for k.node_id in host_list, v in 0}
    # my_dict = {node.node_id: 0 for node in host_list}
    # print(my_dict)

    # acceptors_list = [node for node in self.node_list if node.type == 'ACCEPTOR']

    # # -----------------------------------
    # # Host Test
    # host = Host(1, ('192.168.0.0', 3001), "FOLLOWER")
    # print(host)
    #
    # host_json = host.serialize()
    # print("\n" + host_json)
    #
    # host = deserialize(Host, host_json)
    # print(host)

    # # -----------------------------------
    # # Command Test
    # cmd = Command(1234, "GET", 1)
    # print(cmd)
    #
    # cmd = Command(1234, "SET", 1, "test")
    # print(cmd)
    #
    # cmd_json = cmd.serialize()
    # print("\n" + cmd_json)
    #
    # cmd = deserialize(Command, cmd_json)
    # print("\n" + str(cmd))
