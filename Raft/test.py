from server import *
from utils import *
from node import *
import sys
import socket

if __name__ == '__main__':

    # Get data from the json file
    json_file = "configs\server-1.json"
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
    print("Current term: ", server.current_term)
    print("Voted for: ", server.voted_for)
    for log in server.logs:
        print(log.command)

    server.current_term = 5
    server.voted_for = 3

    # Add commands to the log
    cmd1 = Command(server.address, "1-1", "GET", 2)
    cmd2 = Command(server.address, "1-2", "SET", 3, "TEST")

    server.logs.append(Log(cmd1, 1))
    server.logs.append(Log(cmd2, 1))

    msg = Message('RequestVote', server.address, server.address)
    msg.entries = server.logs

    msg = Message('AppendEntries', server.address, server.address)
    msg.direction = "request"
    msg.entries = server.logs
    print(msg)

    # server.save_state()
    #
    # # -------------------------------------------------------------------
    #
    # server_new = Node(node_id, server_address, 'FOLLOWER', node_list, sock)
    # server_new.update_state()
    #
    # print(server_new)
    # print("Current term: ", server_new.current_term)
    # print("Voted for: ", server_new.voted_for)
    # for log in server_new.logs:
    #     print(log.command)