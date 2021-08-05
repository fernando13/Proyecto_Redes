""" Create an initial setting for each of the servers (json files). """

import socket
import json

if __name__ == '__main__':

	address_ip = socket.gethostbyname(socket.gethostname())
	servers = []
	for j in range(1, 6):
		server = dict()
		server['node_id'] = j
		server['address'] = (address_ip, 3000 + j)
		servers.append(server)

	for i in range(1, 6):
			
		config = dict()
		config['node_id'] = i
		config['port'] = 3000 + i
			
		server_list = servers.copy()
		server_list.pop(i - 1)
		
		config['node_list'] = server_list
		
		config['term'] = 0
		config['voted_for'] = None
		config['log'] = None
		config['dict_data'] = None
		
		# Serializing json 
		json_config = json.dumps(config, indent = 4)
		
		# Writing to file
		file_name = "configs/server-{0}.json".format(i)
		with open(file_name, "w") as file:
			file.write(json_config)
	
	

		
	
	
