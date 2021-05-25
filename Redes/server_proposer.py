from paxos import *
import utils as msg
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

    print(udp_host)
    print('Proposer address: ', server_address)

    # Create a UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(server_address)

    # Create Node
    node_type = 'PROPOSER'
    quorum_size = 1
    node_id = data[0]
    leader = data[2]
    leader_address = data[3]
    node_list = data[4]

    server = Proposer(node_id, server_address, node_type, quorum_size, node_list, sock, leader, leader_address)
    print('NODE ID: ', server.node_id)

    server.socket.setblocking(False)
    print('\nWaiting to receive...\n')
    while True:
        try:

            # Receive response
            data, address = sock.recvfrom(4096)

            message = msg.Message.deserialize(data.decode())
            print(message)

            """--------------------Bully-Algorithm--------------------"""

            if message.msg_type == "ELECTION":
                server.receive_election(address)
                continue

            if message.msg_type == "ANSWER":
                server.receive_answer()
                continue

            if message.msg_type == "COORDINATOR":
                server.receive_coordinator(message.paxos_data[0])
                continue

            # enviar cada tanto tiempo...
            # server.send_heartbeat()

            """--------------------End-Bully-Algorithm------------------"""
            """---------------------------------------------------------"""

            # Timed out to wait for a quorum.
            server.quorum_timeout()

            """--------------------------------------------"""

            if message.msg_type == "REQUEST":
                server.receive_request(message.client_data)
                continue

            """--------------------------------------------"""

            if message.msg_type == "PROMISE":
                node_id = message.paxos_data[0]
                round = message.paxos_data[1]
                prev_accepted_round = message.paxos_data[2]
                prev_accepted_value = message.paxos_data[3]

                server.receive_promise(node_id, round, prev_accepted_round, prev_accepted_value)
                continue

            """--------------------------------------------"""

            if message.msg_type == "LEADERSHIP":
                server.leader_address = tuple(message.paxos_data[0])
                continue

            """--------------------------------------------"""

            if message.msg_type == "ACCEPTED":
                continue

            """--------------------------------------------"""

            if message.msg_type == "RESOLUTION":
                continue

        except socket.error as e:
            if e.args[0] == 10035 or e.args[0] == 10054:

                # Timed out to wait for a quorum.
                server.quorum_timeout()

                # # Timed out to wait for an 'answer' message.
                # server.answer_timeout()
                #
                # # Timed out to wait for a 'heartbeat' message
                # server.heartbeat_timeout()
                #
                # # It's time to send a 'heartbeat' message
                # server.send_heartbeat()

            else:
                print(e)

        except Exception as e:
            print(e)


