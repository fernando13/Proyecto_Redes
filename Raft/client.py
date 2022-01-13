import socket
import sys
import message as msg
from utils import *
from tkinter import *
from random import choice
from datetime import datetime

# python client.py configs\client-1.json

"""
Clients of Raft send all of their requests to the leader.
When a client first starts up, it connects to a randomly chosen server. If the client’s first choice is not the leader,
that server will reject the client’s request and supply information about the most recent leader it has heard from
(AppendEntries requests include the network address of the leader). If the leader crashes, client requests will time
out; clients then try again with randomly-chosen servers.
"""


client_address = None
server_address = None
server_list = []


def generate_serial(address):
    """ Generates a unique serial number to assign to a command. """
    return str(address) + "-" + str(datetime.now())


def get_client_data(file_name):
    with open(file_name, "r") as file:
        data = json.loads(file.read())

        # Client Address
        udp_host = socket.gethostbyname(socket.gethostname())  # Host IP
        address = tuple((udp_host, data["port"]))

        servers = [Host(**node) for node in data["server_list"]]

        file.close()

    return address, servers


def clear_all():
    """ Clear all fields. """
    operation.set("")
    position.set("")
    value.set("")
    txt.delete("1.0", END)


def check_data(action, index, new_value):
    """ Checks if the given data is consistent. """
    ok_data = True

    if action != 'SET' and action != 'GET':
        txt.insert(END, "Wrong command!\n")
        ok_data = False

    if not index or int(index) not in [1, 2, 3, 4, 5]:
        txt.insert(END, "Position out of range!\n")
        ok_data = False

    if action == 'SET' and new_value == "":
        txt.insert(END, "You must enter a value!\n")
        ok_data = False

    return ok_data


def send_request():
    global client_address
    global server_address
    global server_list

    op = operation.get().upper()
    pos = position.get()
    val = value.get()

    txt.delete("1.0", END)

    if not check_data(op, pos, val):
        return

    # Create a UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(client_address)

    # Create Command
    serial = generate_serial(client_address)
    new_value = (None if op == 'GET' else val)
    command = Command(client_address, serial, op, pos, new_value)

    response_ok = False
    timeout = time.time() + TIME_TO_RETRY

    while True:

        if not server_address:
            server_address = choice(server_list).address

        message = msg.Message('ClientRequest', from_address=client_address, to_address=server_address, command=command)

        try:
            # Send request
            print('Sending message:')
            print(' -> ', server_address)
            message.send(sock)

            # Receive response
            sock.settimeout(SERVER_TIMEOUT)
            data, server = sock.recvfrom(4096)

            message = msg.Message.deserialize(data.decode())
            print(message)

            if message.msg_type == "ClientRequest":
                if message.direction == "reply":
                    # Receives the response to the request
                    if message.response:
                        txt.delete("1.0", END)
                        txt.insert(END, message.response + "\n")
                        response_ok = True
                        break
                    else:
                        # Current server is not the leader, It rejected the client’s
                        # request and supply information about the most recent leader
                        if message.leader_address:
                            server_address = tuple(message.leader_address)
                        else:
                            # Current server has no leader's info
                            # Try again with another server
                            server_address = None

        except socket.timeout as e:
            print(e)
            server_address = None

        except socket.error as e:
            print(e)
            server_address = None

        if time.time() >= timeout:
            break

    if not response_ok:
        txt.delete("1.0", END)
        txt.insert(END, "Impossible to connect, try again")

    print('Closing socket')
    sock.close()


if __name__ == '__main__':

    # Client Data
    json_file = sys.argv[1]
    client_address, server_list = get_client_data(json_file)

    # Windows
    root = Tk()
    root.title("Client")
    root.resizable(0, 0)

    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()

    window_height = 350
    window_width = 350

    x_cord = int((screen_width/2) - (window_width/2))
    y_cord = int((screen_height/2) - (window_height/2))

    root.geometry("{}x{}+{}+{}".format(window_width, window_height, x_cord, y_cord))

    # Variables
    operation = StringVar()
    position = StringVar()
    value = StringVar()

    # Operation
    label_op = Label(root, text="Operation ", bd=4)
    label_op.place(x=10, y=30)

    entry_op = Entry(root, textvariable=operation, width=25, bd=3)
    entry_op.place(x=90, y=30)

    label_op2 = Label(root, text="(GET/SET)", bd=4)
    label_op2.place(x=260, y=30)

    # Position
    label_port = Label(root, text="Position ", bd=4)
    label_port.place(x=10, y=60)

    entry_port = Entry(root, textvariable=position, width=25, bd=3)
    entry_port.place(x=90, y=60)

    label_op2 = Label(root, text="[1..5]", bd=4)
    label_op2.place(x=260, y=60)

    # Value
    label_port = Label(root, text="Value", bd=4)
    label_port.place(x=10, y=90)

    entry_port = Entry(root, textvariable=value, width=25, bd=3)
    entry_port.place(x=90, y=90)

    # Clear Button
    btn_clear = Button(root, text="Clear", command=clear_all, width=10, bd=4)
    btn_clear.place(x=70, y=150)

    # Send Button
    button = Button(root, text="Send", command=send_request, width=10, bd=4)
    button.place(x=190, y=150)

    # Information
    txt = Text(root, height=8, width=40)
    txt.place(x=11, y=200)

    root.mainloop()
