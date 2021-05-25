import socket
import utils as msg
from tkinter import *


if __name__ == '__main__':

    def check_data(op, position, value):
        ok_data = True

        if op != 'SET' and op != 'GET':
            txt.insert(END, "Wrong command!\n")
            ok_data = False

        if position == "" or int(position) not in range(6):
            txt.insert(END, "Position out of range!\n")
            ok_data = False

        if op == 'SET' and value == "":
            txt.insert(END, "You must enter a value!\n")
            ok_data = False

        return ok_data

    def send_request():
        op = operation.get().upper()
        pos = position.get()
        val = value.get()

        txt.delete("1.0", END)

        if not check_data(op, pos, val):
            return

        # Server Address
        server_address = (udp_host.get(), int(udp_port.get()))

        # Client Address
        client_address = (udp_host.get(), 12345)

        # Create a UDP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(client_address)

        # Prepare request message
        arguments = (client_address, op, pos, None if op == 'GET' else val)
        message = msg.Message('REQUEST', arguments)

        try:
            # Send data
            print('Sending message:')
            print(message)
            sock.sendto(message.serialize().encode(), server_address)

            # Receive response
            print('\nWaiting to receive...\n')
            sock.settimeout(5.0)
            data, server = sock.recvfrom(4096)

            txt.delete("1.0", END)
            txt.insert(END, data.decode()+"\n")

        except socket.timeout:
            txt.delete("1.0", END)
            txt.insert(END, "Error: timeout")

        except Exception as e:
            txt.delete("1.0", END)
            txt.insert(END, str(e)+"\n")

        print('Closing socket')
        sock.close()

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
    udp_host = StringVar()
    udp_port = StringVar()
    operation = StringVar()
    position = StringVar()
    value = StringVar()

    udp_host.set(socket.gethostbyname(socket.gethostname()))
    udp_port.set(3001)
    operation.set('GET')
    position.set(1)

    # Host IP
    label_host = Label(root, text="Host IP ", bd=4)
    label_host.place(x=10, y=10)

    entry_host = Entry(root, textvariable=udp_host, width=25, bd=3)
    entry_host.place(x=90, y=10)

    # Port
    label_port = Label(root, text="Port ", bd=4)
    label_port.place(x=10, y=40)

    entry_port = Entry(root, textvariable=udp_port, width=25, bd=3)
    entry_port.place(x=90, y=40)

    # Operation
    label_op = Label(root, text="Operation ", bd=4)
    label_op.place(x=10, y=90)

    entry_op = Entry(root, textvariable=operation, width=25, bd=3)
    entry_op.place(x=90, y=90)

    label_op2 = Label(root, text="(GET/SET)", bd=4)
    label_op2.place(x=260, y=90)

    # Position
    label_port = Label(root, text="Position ", bd=4)
    label_port.place(x=10, y=120)

    entry_port = Entry(root, textvariable=position, width=25, bd=3)
    entry_port.place(x=90, y=120)

    label_op2 = Label(root, text="[0..5]", bd=4)
    label_op2.place(x=260, y=120)

    # Value
    label_port = Label(root, text="Value", bd=4)
    label_port.place(x=10, y=150)

    entry_port = Entry(root, textvariable=value, width=25, bd=3)
    entry_port.place(x=90, y=150)

    # Send Button
    button = Button(root, text="Send", command=send_request, width=15, bd=4)
    button.place(x=110, y=200)

    # Information
    txt = Text(root, height=4, width=40)
    txt.place(x=11, y=250)

    root.mainloop()
