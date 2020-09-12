/* tcpclient.c */
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>

int main(int argc, char *argv[])

{
    int sock, bytes_recieved, port;  
    char send_data[1024], recv_data[1024];
    struct hostent *host;
    struct sockaddr_in server_addr;  

    host = gethostbyname("192.168.0.7");

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        perror("Socket");
        exit(1);
    }

    port = atoi(argv[1]);
    server_addr.sin_family = AF_INET;     
    server_addr.sin_port = htons(port);   
    server_addr.sin_addr = *((struct in_addr *)host->h_addr);
    bzero(&(server_addr.sin_zero),8); 

    if (connect(sock, (struct sockaddr *)&server_addr, sizeof(struct sockaddr)) == -1) {
        perror("Connect");
        exit(1);
    }


    while(1) {
        bytes_recieved=recv(sock, recv_data, 1024, 0);
        recv_data[bytes_recieved] = '\0';
 
        printf("Recieved data = %s " , recv_data);
           
        printf("\n\n-------------------------------------------------------------");   
        printf("\nRequest options: ");
		printf("\n* [get + id] --> to get the value in the position 'id'");
		printf("\n* [set + id + new_value] --> to set the value 'new_value' in the position 'id'");

        printf("\n\nSend your request: ");
        gets(send_data);
           
        if (strcmp(send_data , "q") != 0 && strcmp(send_data , "Q") != 0)
            send(sock, send_data, strlen(send_data), 0); 
        else {
            send(sock, send_data, strlen(send_data), 0);   
            close(sock);
            break;
        }
        
    }  
    return 0;
}



