# Operating System File Server Project

A console-based **Operating System project in C** that implements a client-server file management system using socket programming, multithreading, synchronization, scheduling, and deadlock avoidance concepts.

The project contains two main programs:

```text
server.c
client.c

Features

Client Features

Connect to server using TCP socket
Open a file
Read a file
Write content to a file
Close a file
Delete a file
List all files stored on the server
Exit the client program

Server Features

Handles multiple clients
Uses TCP socket communication
Creates a separate thread for each client
Stores files in a server-side storage directory
Supports concurrent file access
Uses mutex locks and semaphores for synchronization
Uses Round Robin scheduling for client requests
Uses Banker’s Algorithm for resource allocation and deadlock avoidance
Limits maximum connected clients
Limits concurrent readers for file access

Operating System Concepts Used

Process communication
Socket programming
Client-server architecture
Multithreading
Thread synchronization
Mutex locks
Semaphores
Reader-writer problem
Round Robin scheduling
Banker’s Algorithm
Deadlock avoidance
File handling
Resource allocation
Critical section handling

Technologies Used

C Programming Language
POSIX Threads
TCP Sockets
Linux System Calls
File Handling
Semaphores
Mutex Locks
Header Files Used

Server
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <pthread.h>
#include <dirent.h>
#include <errno.h>
#include <semaphore.h>
#include <stdbool.h>

Client
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

Project Structure
Operating-System-File-Server/
│
├── server.c
├── client.c
└── server_storage/

The server_storage folder is created automatically by the server if it does not already exist.

Request Types

  Code	  Operation
1	Open   File
2	Read   File
3	Write  File
4	Close  File
5	Delete File
6	List   Files

How the System Works

Client
  ↓
Sends request to server
  ↓
Server receives request
  ↓
Request is added to Round Robin queue
  ↓
Banker's Algorithm checks resource availability
  ↓
Server processes request
  ↓
Synchronization locks protect shared files
  ↓
Response is sent back to client

Server Workflow

Start Server
│
├── Create server_storage directory
├── Initialize semaphores and mutex locks
├── Create Round Robin scheduler thread
├── Create TCP socket
├── Bind socket to port 65432
├── Listen for clients
├── Accept client connections
├── Create thread for each client
└── Process client requests

Client Workflow

Start Client
│
├── Connect to server at 127.0.0.1:65432
├── Display menu
├── Take user choice
├── Send request to server
├── Receive server response
└── Continue until user selects Exit

Client Menu

File Server Client Menu:
1. Open File
2. Read File
3. Write to File
4. Close File
5. Delete File
6. List Files
7. Exit
Enter your choice:
