#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <stdlib.h>
#include <strings.h>
#include <string.h>

#define BUF_SIZE 4096
#define MAX_LONG_STRN 20

char dictionary[][MAX_LONG_STRN] = {"Yarara", "Tejon", "Pibe Play", "Dinosaurio", "Zapato"};


void error(char *msg)
{
    perror(msg);
    exit(1);
}


void service(int s, char *msg)
{
	// get first argument
	char *token = strtok(msg, " ");

	if(strcmp(token, "get") == 0){

		// get second argument
    token = strtok(NULL, " ");
  	int id = atoi(token);

    if (write(s, dictionary[id], sizeof(dictionary[id])) == -1)
    	error("ERROR writing to socket"); 

	}
	else if (strcmp(token, "set") == 0){

		// get second argument
    token = strtok(NULL, " ");
    int id = atoi(token);

    // get third command
    token = strtok(NULL, " ");
   	
		strcpy(dictionary[id], token);

		if (write(s,"Successful change!\n", 20) == -1)
    	error("ERROR writing to socket"); 
	}
	else{
		 if (write(s, "Comando no valido\n", 18) == -1)
    	error("ERROR writing to socket"); 
	}  
}



int main(int argc, char *argv[])
{
	int sockfd, sock_connect, port;
	char buffer[BUF_SIZE];
	struct sockaddr_in serv_addr, cli_addr;
	int n;
	socklen_t clilen = sizeof(cli_addr);

	if (argc < 2) {
	   fprintf(stderr, "ERROR, no port provided\n");
	   exit(1);
	}

	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd < 0)
	  error("ERROR opening socket");

	bzero((char *) &serv_addr, sizeof(serv_addr));
	port = atoi(argv[1]);
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = INADDR_ANY;
	serv_addr.sin_port = htons(port);

	/* bind server address to socket */
	if (bind(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0)
	  error("ERROR on binding");

	listen(sockfd, 5); 


	/* concurrent server */
	while (1) {

		/* wait for client connection */
		// sock_connect: nuevo socket que tiene la conexion con el cliente
		sock_connect = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen); 

		cli_addr
		if(fork()==0){ 
		  if (sock_connect < 0)
		    error("ERROR on accept");

		  write(sock_connect, "Connected..\n", 12); 
		  do {

				bzero(buffer, BUF_SIZE);
				if ( (n = read(sock_connect, buffer, BUF_SIZE)) == -1 )
				    error("ERROR reading from socket");

				printf("Client %i message: %s\n", cli_addr.sin_port ,buffer);
				if(strcmp(buffer,"q")==0)
				    break;

				service(sock_connect, buffer);

		  } while (1);
		  close(sock_connect);
		  exit(0);

		}
		else{ close(sock_connect); }
	}

	close(sockfd);
	return 0;
}

