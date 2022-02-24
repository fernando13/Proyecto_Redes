import sys
import socket
from node import Node
from message import Message
from utils import *
import threading

# run a server
# python server.py configs\server-1.json
# sys.argv[1] -->  file.json


def get_server_info(file_name):
    with open(file_name, "r") as file:
        data = json.loads(file.read())

        node_id = int(data["node_id"])
        port = int(data["port"])
        node_list = [Host(**node) for node in data["node_list"]]

        file.close()

    return node_id, port, node_list


if __name__ == '__main__':

    # Get data from the json file
    json_file = sys.argv[1]
    node_id, port, node_list = get_server_info(json_file)

    # Server Address
    udp_host = socket.gethostbyname(socket.gethostname())  # Host IP
    udp_port = port  # Specified port to connect
    server_address = (udp_host, udp_port)

    # Create a UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(server_address)

    server = Node(node_id, server_address, 'FOLLOWER', node_list, sock)
    server.update_state()
    print(server)

    server.socket.setblocking(False)

    while True:
        try:
            # Receive response
            data, address = sock.recvfrom(4096)

            message = Message.deserialize(data.decode())
            print(message)

            # It's time to send a heartbeat message
            server.heartbeat_timeout_due()

            # Timed out to wait for a heartbeat message
            server.election_timeout_due()

            if message.msg_type == "AppendEntries":
                if message.direction == "request":
                    server.receive_append_entries(message)
                else:
                    server.receive_append_entries_reply(message)

            elif message.msg_type == "RequestVote":
                if message.direction == "request":
                    server.receive_request_vote(message)
                else:
                    server.receive_request_vote_reply(message)

            elif message.msg_type == "ClientRequest":
                if message.direction == "request":
                    # server.receive_client_request(message)
                    threading.Thread(target=server.receive_client_request(message)).start()

        except socket.error as e:
            # Error: 10035 --> server didn't receive data from 'sock.recvfrom(4096)'
            # Error: 10054 --> problems contacting another node
            if e.args[0] == 10035 or e.args[0] == 10054 or e.args[0] == 11:

                # It's time to send a heartbeat message
                server.heartbeat_timeout_due()

                # Timed out to wait for a heartbeat message
                server.election_timeout_due()

            else:
                print("Error :", e)

        except Exception as e:
            print("Error :", e)
