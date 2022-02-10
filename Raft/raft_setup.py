import sys
import socket
import json

""" Create an initial setting for each of the servers (json files).
By default, It creates settings for 5 servers and 3 clients. """

if __name__ == '__main__':

    if len(sys.argv) > 1:
        cant_servers = int(sys.argv[1])
        cant_clients = int(sys.argv[2])
    else:
        cant_servers = 5
        cant_clients = 3

    # List of all servers
    address_ip = socket.gethostbyname(socket.gethostname())
    servers = []
    for j in range(1, cant_servers + 1):
        server = dict()
        server['node_id'] = j
        server['address'] = (address_ip, 3000 + j)
        servers.append(server)

    # Initialization of the shared resource
    dict_data = {1: "Blue", 2: "Yellow", 3: "Red", 4: "Green", 5: "White"}

    # Create configurations for servers
    for i in range(1, cant_servers + 1):
        config = dict()
        config['node_id'] = i
        config['port'] = 3000 + i

        server_list = servers.copy()
        server_list.pop(i - 1)

        config['node_list'] = server_list

        config['term'] = 0
        config['voted_for'] = None
        config['logs'] = None
        config['dict_data'] = dict_data

        # Serializing json
        json_config = json.dumps(config, indent=2)

        # Writing to file
        file_name = "configs/server-{0}.json".format(i)
        with open(file_name, "w") as file:
            file.write(json_config)

    # Create configurations for clients
    for i in range(1, cant_clients + 1):
        config = dict()
        config['port'] = 4000 + i
        config['server_list'] = servers

        # Serializing json
        json_config = json.dumps(config, indent=2)

        # Writing to file
        file_name = "configs/client-{0}.json".format(i)
        with open(file_name, "w") as file:
            file.write(json_config)
