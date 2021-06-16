from paxos import *
import utils as msg
import sys
import socket

# python server_proposer.py config/proposer-1.json
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
    quorum_size = 1
    node_id = data[0]
    node_list = data[2]

    server = Proposer(node_id, server_address, quorum_size, node_list, sock)
    print(server)

    server.socket.setblocking(False)

    # If the node has the largest ID then it proclaims himself a leader
    server.self_proclaimed_leader()

    while True:
        try:
            # Receive response
            data, address = sock.recvfrom(4096)

            message = msg.Message.deserialize(data.decode())
            print(message)

            """-------------------------------------------------------"""

            # Timed out to wait for a quorum.
            server.quorum_timeout()

            """-------------------------------------------------------"""
            """--------------------Bully-Algorithm--------------------"""

            # Timed out to wait for an 'answer' message.
            server.answer_timeout()

            # Timed out to wait for a 'heartbeat' message
            server.heartbeat_timeout()

            # It's time to send a 'heartbeat' message
            server.send_heartbeat()

            if message.msg_type == "COORDINATOR":
                new_leader = tuple(message.paxos_data[0])
                server.receive_coordinator(new_leader)
                continue

            if message.msg_type == "ELECTION":
                server.receive_election(address)
                continue

            if message.msg_type == "ANSWER":
                server.receive_answer()
                continue

            if message.msg_type == "HEARTBEAT":
                from_id = message.paxos_data[0]
                from_address = tuple(message.paxos_data[1])

                server.receive_heartbeat(from_id, from_address)
                continue

            """--------------------End-Bully-Algorithm------------------"""
            """---------------------------------------------------------"""

            if message.msg_type == "REQUEST":
                server.receive_request(message.client_data)
                continue

            """--------------------------------------------"""

            if message.msg_type == "PROMISE":
                node_id = message.paxos_data[0]
                index = message.paxos_data[1]
                round = message.paxos_data[2]
                prev_accepted_round = message.paxos_data[3]
                prev_accepted_value = message.paxos_data[4]

                server.receive_promise(node_id, index, round, prev_accepted_round, prev_accepted_value)
                continue

            """--------------------------------------------"""

            if message.msg_type == "ACCEPTED":
                continue

            """--------------------------------------------"""

            if message.msg_type == "RESOLUTION":
                index = message.paxos_data[0]
                value = message.paxos_data[1]

                # Update the log.
                server.logs[index] = value[1:]

                # Do the received command.
                key = int(value[2])
                new_value = value[3]
                server.dictionary_data[key] = new_value

                # Replicate the command to the others nodes
                server.send_replicate(message.paxos_data)

                print("\nLogs: ", server.logs)
                message = str(server.logs)

                # Send response to the client...
                server.socket.sendto(message.encode(), tuple(value[0]))
                continue

            """--------------------------------------------"""

            if message.msg_type == "REPLICATE":
                index = message.paxos_data[0]
                value = message.paxos_data[1]

                # Update the log.
                server.logs[index] = value[1:]

                # Do the received command.
                key = int(value[2])
                new_value = value[3]
                server.dictionary_data[key] = new_value
                continue

        except socket.error as e:
            # Error: 10035 --> server didn't receive data from 'sock.recvfrom(4096)'
            # Error: 10054 --> problems contacting another node
            if e.args[0] == 10035 or e.args[0] == 10054:

                # Timed out to wait for a quorum.
                server.quorum_timeout()

                # Timed out to wait for an 'answer' message.
                server.answer_timeout()

                # Timed out to wait for a 'heartbeat' message
                server.heartbeat_timeout()

                # It's time to send a 'heartbeat' message
                server.send_heartbeat()

            else:
                print("Error :", e)

        except Exception as e:
            print("Error :", e)


