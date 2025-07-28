#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

#define PORT 65432
#define BUFFER_SIZE 1024

void print_menu() {
    printf("\nFile Server Client Menu:\n");
    printf("1. Open File\n");
    printf("2. Read File\n");
    printf("3. Write to File\n");
    printf("4. Close File\n");
    printf("5. Delete File\n");
    printf("6. List Files\n");
    printf("7. Exit\n");
    printf("Enter your choice: ");
}

int connect_to_server() {
    int sock = 0;
    struct sockaddr_in serv_addr;   
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        printf("\nSocket creation error\n");
        return -1;
    }
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);
    if (inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0) {
        printf("\nInvalid address/ Address not supported\n");
        return -1;
    }
    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        printf("\nConnection Failed\n");
        return -1;
    }
    return sock;
}

void send_request(int sock, int request_type, const char *data) {
    char buffer[BUFFER_SIZE] = {0}; 
    buffer[0] = '0' + request_type;
    if (data != NULL) {
        strncpy(buffer + 1, data, BUFFER_SIZE - 2);
    }
    send(sock, buffer, strlen(buffer), 0);
    memset(buffer, 0, BUFFER_SIZE);
    read(sock, buffer, BUFFER_SIZE);
    printf("Server response: %s\n", buffer);
}

void open_file(int sock) {
    char filename[BUFFER_SIZE];
    printf("Enter filename to open: ");
    scanf("%s", filename);
    send_request(sock, 1, filename);
}

void read_file(int sock) {
    char filename[BUFFER_SIZE];
    printf("Enter filename to read: ");
    scanf("%s", filename);
    send_request(sock, 2, filename);
}

void write_file(int sock) {
    char filename[BUFFER_SIZE];
    char content[BUFFER_SIZE];

    printf("Enter filename to write: ");
    scanf("%s", filename);
    printf("Enter content to write (max %d chars):\n", BUFFER_SIZE - strlen(filename) - 2);
    getchar(); 
    fgets(content, BUFFER_SIZE - strlen(filename) - 2, stdin);
    content[strcspn(content, "\n")] = 0;

    char request_data[BUFFER_SIZE];
    snprintf(request_data, BUFFER_SIZE, "%s\n%s", filename, content);
    send_request(sock, 3, request_data);
}

void close_file(int sock) {
    char filename[BUFFER_SIZE];
    printf("Enter filename to close: ");
    scanf("%s", filename);
    send_request(sock, 4, filename);
}

void delete_file(int sock) {
    char filename[BUFFER_SIZE];
    printf("Enter filename to delete: ");
    scanf("%s", filename);
    send_request(sock, 5, filename);
}

void list_files(int sock) {
    send_request(sock, 6, NULL);
}

int main() {
    int sock = connect_to_server();
    if (sock < 0) {
        return -1;
    }

    int choice;
    do {
        print_menu();
        scanf("%d", &choice);
       
        switch(choice) {
            case 1: open_file(sock); break;
            case 2: read_file(sock); break;
            case 3: write_file(sock); break;
            case 4: close_file(sock); break;
            case 5: delete_file(sock); break;
            case 6: list_files(sock); break;
            case 7: printf("Exiting...\n"); break;
            default: printf("Invalid choice!\n");
        }
    } while (choice != 7);   
    close(sock);
    return 0;
}