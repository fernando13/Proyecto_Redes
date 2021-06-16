from paxos import *
from utils import *
import sys
import socket

# sys.argv[1] -->  file.json

if __name__ == '__main__':

    # Get data from the json file
    json_file = sys.argv[1]
    data = tuple(read_file(json_file))

    # Server Address
    udp_host = socket.gethostbyname(socket.gethostname())  # Host IP
    udp_port = data[1]                                     # Specified port to connect
    server_address = (udp_host, udp_port)

    # Create a UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(server_address)

    # Create Node
    node_id = data[0]
    node_list = data[2]

    server = Acceptor(node_id, server_address, node_list, sock)
    print(server)

    while True:
        try:
            # Receive response
            data, address = sock.recvfrom(4096)

            message = Message.deserialize(data.decode())
            print(message)

            # -----------------------------------------------

            if message.msg_type == "PREPARE":
                index = message.paxos_data[0]
                round = message.paxos_data[1]

                server.receive_prepare(address, index, round)

            # -----------------------------------------------

            elif message.msg_type == "ACCEPT":
                index = message.paxos_data[0]
                round = message.paxos_data[1]
                value = message.paxos_data[2]

                server.receive_accept(address, index, round, value)

            # -----------------------------------------------

            elif message.msg_type == "RESOLUTION":
                continue

        except Exception as e:
            print("Error :", e)
            continue
