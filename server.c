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

#define PORT 65432
#define MAX_CLIENTS 10
#define BUFFER_SIZE 1024
#define STORAGE_DIR "./server_storage"
#define MAX_FILENAME 256
#define MAX_CONCURRENT_READERS 5
#define QUANTUM 2  // Time quantum for Round Robin in seconds

// Request types
#define OPEN_FILE 1
#define READ_FILE 2
#define WRITE_FILE 3
#define CLOSE_FILE 4
#define DELETE_FILE 5
#define LIST_FILES 6

// File tracking structure
typedef struct {
    char filename[MAX_FILENAME];
    int ref_count;
    int reader_count;
    pthread_mutex_t lock;
    sem_t write_lock;       // Binary semaphore for writers
    sem_t reader_sem;       // Counting semaphore for readers
} file_track_t;

// Client request structure for Round Robin
typedef struct {
    int client_socket;
    int request_type;
    char data[BUFFER_SIZE];
    time_t arrival_time;
    time_t processing_time;
    struct timespec start_time;
    bool completed;
} client_request_t;

// Banker's algorithm data structures
#define MAX_RESOURCES 100  // Maximum number of file resources
int available = MAX_CLIENTS;  // Initially all connections are available
int maximum[MAX_CLIENTS] = {0};  // Maximum demand per client
int allocation[MAX_CLIENTS] = {0};  // Allocated resources per client
int need[MAX_CLIENTS] = {0};  // Need per client (maximum - allocation)
pthread_mutex_t banker_lock = PTHREAD_MUTEX_INITIALIZER;

// Round Robin queue
client_request_t request_queue[MAX_CLIENTS];
int queue_front = 0;
int queue_rear = -1;
int queue_count = 0;
pthread_mutex_t queue_lock = PTHREAD_MUTEX_INITIALIZER;
sem_t queue_sem;  // Signals when requests are available

// Global variables
file_track_t tracked_files[MAX_RESOURCES];  // Track open files
int num_tracked_files = 0;
pthread_mutex_t global_lock = PTHREAD_MUTEX_INITIALIZER;
sem_t server_sem;           // Main server semaphore for connection limiting

void *handle_client(void *arg);
void *round_robin_scheduler(void *arg);
int process_request(int client_socket, int request_type, char *data);
file_track_t *get_file_lock(const char *filename);
void release_file_lock(const char *filename);
int open_file_operation(char *filename, int client_socket);
int read_file_operation(char *filename, int client_socket);
int write_file_operation(char *filename, char *content, int client_socket);
int close_file_operation(char *filename, int client_socket);
int delete_file_operation(char *filename, int client_socket);
int list_files_operation(int client_socket);
bool bankers_algorithm(int client_id, int requested_resources);
void enqueue_request(client_request_t request);
client_request_t dequeue_request();

void init_sync_primitives() {
    sem_init(&server_sem, 0, MAX_CLIENTS);
   
    for (int i = 0; i < MAX_RESOURCES; i++) {
        tracked_files[i].ref_count = 0;
        tracked_files[i].reader_count = 0;
        pthread_mutex_init(&tracked_files[i].lock, NULL);
        sem_init(&tracked_files[i].write_lock, 0, 1);  // Binary semaphore
        sem_init(&tracked_files[i].reader_sem, 0, MAX_CONCURRENT_READERS);
    }
    sem_init(&queue_sem, 0, 0);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        maximum[i] = 5;  // Each client can request up to 5 file handles
        need[i] = maximum[i];
    }
}
file_track_t *get_file_lock(const char *filename) {
    pthread_mutex_lock(&global_lock);
    for (int i = 0; i < num_tracked_files; i++) {
        if (strcmp(tracked_files[i].filename, filename) == 0) {
            pthread_mutex_unlock(&global_lock);
            return &tracked_files[i];
        }
    }
    if (num_tracked_files < MAX_RESOURCES) {
        strncpy(tracked_files[num_tracked_files].filename, filename, MAX_FILENAME);
        tracked_files[num_tracked_files].ref_count = 0;
        tracked_files[num_tracked_files].reader_count = 0;
        pthread_mutex_init(&tracked_files[num_tracked_files].lock, NULL);
        sem_init(&tracked_files[num_tracked_files].write_lock, 0, 1);
        sem_init(&tracked_files[num_tracked_files].reader_sem, 0, MAX_CONCURRENT_READERS);
       
        file_track_t *result = &tracked_files[num_tracked_files];
        num_tracked_files++;
        pthread_mutex_unlock(&global_lock);
        return result;
    }   
    pthread_mutex_unlock(&global_lock);
    return NULL;
}

void release_file_lock(const char *filename) {
    pthread_mutex_lock(&global_lock);

    for (int i = 0; i < num_tracked_files; i++) {
        if (strcmp(tracked_files[i].filename, filename) == 0) {
            pthread_mutex_unlock(&global_lock);
            return;
        }
    }
    pthread_mutex_unlock(&global_lock);
}

int open_file_operation(char *filename, int client_socket) {
    char response[BUFFER_SIZE] = {0};
    char filepath[MAX_FILENAME + strlen(STORAGE_DIR) + 2];
    snprintf(filepath, sizeof(filepath), "%s/%s", STORAGE_DIR, filename);
   
    file_track_t *file_track = get_file_lock(filename);
    if (!file_track) {
        sprintf(response, "ERROR: Maximum number of open files reached");
        send(client_socket, response, strlen(response), 0);
        return -1;
    }   
    pthread_mutex_lock(&file_track->lock);
    file_track->ref_count++;
    pthread_mutex_unlock(&file_track->lock);
   
    int fd = open(filepath, O_RDONLY | O_CREAT, 0644);
    if (fd < 0) {
        sprintf(response, "ERROR: Could not open file %s", filename);
        send(client_socket, response, strlen(response), 0);
       
        pthread_mutex_lock(&file_track->lock);
        file_track->ref_count--;
        pthread_mutex_unlock(&file_track->lock);       
        return -1;
    }
    close(fd);
    sprintf(response, "OK: File %s opened successfully", filename);
    send(client_socket, response, strlen(response), 0);
    return 0;
}

int read_file_operation(char *filename, int client_socket) {
    char response[BUFFER_SIZE] = {0};
    char filepath[MAX_FILENAME + strlen(STORAGE_DIR) + 2];
    snprintf(filepath, sizeof(filepath), "%s/%s", STORAGE_DIR, filename);
   
    file_track_t *file_track = get_file_lock(filename);
    if (!file_track) {
        sprintf(response, "ERROR: File %s not found or too many open files", filename);
        send(client_socket, response, strlen(response), 0);
        return -1;
    }
   
    sem_wait(&file_track->reader_sem);
    
    pthread_mutex_lock(&file_track->lock);
    file_track->reader_count++;
    if (file_track->reader_count == 1) {
        sem_wait(&file_track->write_lock); // First reader locks writers out
    }
    pthread_mutex_unlock(&file_track->lock);
   
    int fd = open(filepath, O_RDONLY);
    if (fd < 0) {
        sprintf(response, "ERROR: Could not open file %s for reading", filename);
        send(client_socket, response, strlen(response), 0);
       
        pthread_mutex_lock(&file_track->lock);
        file_track->reader_count--;
        if (file_track->reader_count == 0) {
            sem_post(&file_track->write_lock);
        }
        pthread_mutex_unlock(&file_track->lock);
       
        sem_post(&file_track->reader_sem);
        return -1;
    }   
    char buffer[BUFFER_SIZE];
    ssize_t bytes_read;
    while ((bytes_read = read(fd, buffer, BUFFER_SIZE)) > 0) {
        send(client_socket, buffer, bytes_read, 0);
    }
    close(fd);
    pthread_mutex_lock(&file_track->lock);
    file_track->reader_count--;
    if (file_track->reader_count == 0) {
        sem_post(&file_track->write_lock); // Last reader releases write lock
    }
    pthread_mutex_unlock(&file_track->lock);
    sem_post(&file_track->reader_sem);
    return 0;
}

int write_file_operation(char *filename, char *content, int client_socket) {
    char response[BUFFER_SIZE] = {0};
    char filepath[MAX_FILENAME + strlen(STORAGE_DIR) + 2];
    snprintf(filepath, sizeof(filepath), "%s/%s", STORAGE_DIR, filename);
   
    file_track_t *file_track = get_file_lock(filename);
    if (!file_track) {
        sprintf(response, "ERROR: Maximum number of open files reached");
        send(client_socket, response, strlen(response), 0);
        return -1;
    }   
    sem_wait(&file_track->write_lock);
    pthread_mutex_lock(&file_track->lock);
    file_track->ref_count++;
    pthread_mutex_unlock(&file_track->lock);
    int fd = open(filepath, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        sprintf(response, "ERROR: Could not open file %s for writing", filename);
        send(client_socket, response, strlen(response), 0);       
        sem_post(&file_track->write_lock);
        return -1;
    }
    ssize_t bytes_written = write(fd, content, strlen(content));
    close(fd);
    pthread_mutex_lock(&file_track->lock);
    file_track->ref_count--;
    pthread_mutex_unlock(&file_track->lock);
    sem_post(&file_track->write_lock);
    
    if (bytes_written < 0) {
        sprintf(response, "ERROR: Could not write to file %s", filename);
        send(client_socket, response, strlen(response), 0);
        return -1;
    }
    sprintf(response, "OK: File %s written successfully (%zd bytes)", filename, bytes_written);
    send(client_socket, response, strlen(response), 0);
    return 0;
}

int close_file_operation(char *filename, int client_socket) {
    char response[BUFFER_SIZE] = {0};
   
    pthread_mutex_lock(&global_lock);
    for (int i = 0; i < num_tracked_files; i++) {
        if (strcmp(tracked_files[i].filename, filename) == 0) {
            pthread_mutex_lock(&tracked_files[i].lock);
            if (tracked_files[i].ref_count > 0) {
                tracked_files[i].ref_count--;
                pthread_mutex_unlock(&tracked_files[i].lock);
                pthread_mutex_unlock(&global_lock);
               
                sprintf(response, "OK: File %s closed (remaining references: %d)",
                       filename, tracked_files[i].ref_count);
                send(client_socket, response, strlen(response), 0);
                return 0;
            }
            pthread_mutex_unlock(&tracked_files[i].lock);
            break;
        }
    }
    pthread_mutex_unlock(&global_lock);   
    sprintf(response, "ERROR: File %s not open or not found", filename);
    send(client_socket, response, strlen(response), 0);
    return -1;
}

int delete_file_operation(char *filename, int client_socket) {
    char response[BUFFER_SIZE] = {0};
    char filepath[MAX_FILENAME + strlen(STORAGE_DIR) + 2];
    snprintf(filepath, sizeof(filepath), "%s/%s", STORAGE_DIR, filename);
   
    file_track_t *file_track = get_file_lock(filename);
    if (!file_track) {
        sprintf(response, "ERROR: Maximum number of open files reached");
        send(client_socket, response, strlen(response), 0);
        return -1;
    }
   
    sem_wait(&file_track->write_lock);  
    pthread_mutex_lock(&file_track->lock);
    if (file_track->ref_count > 0 || file_track->reader_count > 0) {
        pthread_mutex_unlock(&file_track->lock);
        sem_post(&file_track->write_lock);
       
        sprintf(response, "ERROR: File %s is currently in use", filename);
        send(client_socket, response, strlen(response), 0);
        return -1;
    }
    pthread_mutex_unlock(&file_track->lock);
    if (unlink(filepath) == 0) {
        sprintf(response, "OK: File %s deleted successfully", filename);
        send(client_socket, response, strlen(response), 0);
    } else {
        sprintf(response, "ERROR: Could not delete file %s", filename);
        send(client_socket, response, strlen(response), 0);
        sem_post(&file_track->write_lock);
        return -1;
    }
   
    sem_post(&file_track->write_lock);
    return 0;
}

// List files operation
int list_files_operation(int client_socket) {
    DIR *dir;
    struct dirent *ent;
    char response[BUFFER_SIZE] = {0};
   
    if ((dir = opendir(STORAGE_DIR)) != NULL) {
        int pos = 0;
        pos += snprintf(response + pos, BUFFER_SIZE - pos, "Files in server storage:\n");
       
        while ((ent = readdir(dir)) != NULL && pos < BUFFER_SIZE - 1) {
            if (ent->d_type == DT_REG) {  // Only regular files
                pos += snprintf(response + pos, BUFFER_SIZE - pos, "- %s\n", ent->d_name);
            }
        }
        closedir(dir);
       
        if (pos >= BUFFER_SIZE - 1) {
            // Truncate the response if too long
            strcpy(response + BUFFER_SIZE - 20, "... (truncated)");
        }
       
        send(client_socket, response, strlen(response), 0);
        return 0;
    } else {
        sprintf(response, "ERROR: Could not open storage directory");
        send(client_socket, response, strlen(response), 0);
        return -1;
    }
}
// Banker's algorithm implementation
bool bankers_algorithm(int client_id, int requested_resources) {
    pthread_mutex_lock(&banker_lock);
   
    if (requested_resources > need[client_id]) {
        pthread_mutex_unlock(&banker_lock);
        return false;
    }
   
    if (requested_resources > available) {
        pthread_mutex_unlock(&banker_lock);
        return false;
    }
   
    available -= requested_resources;
    allocation[client_id] += requested_resources;
    need[client_id] -= requested_resources;
   
    bool safe = false;
    int work = available;
    bool finish[MAX_CLIENTS] = {false};
    int count = 0;
   
    while (count < MAX_CLIENTS) {
        bool found = false;
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (!finish[i] && need[i] <= work) {
                work += allocation[i];
                finish[i] = true;
                found = true;
                count++;
            }
        }
        if (!found) break;
    }
    if (count == MAX_CLIENTS) {
        safe = true;
    }
    if (!safe) {
        available += requested_resources;
        allocation[client_id] -= requested_resources;
        need[client_id] += requested_resources;
    }   
    pthread_mutex_unlock(&banker_lock);
    return safe;
}

// Release resources for a client
void release_resources(int client_id, int released_resources) {
    pthread_mutex_lock(&banker_lock);
    available += released_resources;
    allocation[client_id] -= released_resources;
    need[client_id] += released_resources;
    pthread_mutex_unlock(&banker_lock);
}

// Round Robin scheduler thread
void *round_robin_scheduler(void *arg) {
    while (1) {
        sem_wait(&queue_sem);
       
        client_request_t request = dequeue_request();
       
        if (request.client_socket != -1) {
            printf("Processing request from client %d (Type: %d)\n", request.client_socket, request.request_type);
           
            bool granted = false;
            int resources_needed = 1;  // Default for most operations

            if (request.request_type == OPEN_FILE || request.request_type == READ_FILE ||
                request.request_type == WRITE_FILE) {
                resources_needed = 1;
            } else {
                resources_needed = 0;  // Other operations don't need file handles
            }
           
            if (resources_needed > 0) {
                granted = bankers_algorithm(request.client_socket % MAX_CLIENTS, resources_needed);
            } else {
                granted = true;
            }
           
            if (granted) {
                clock_gettime(CLOCK_MONOTONIC, &request.start_time);
                process_request(request.client_socket, request.request_type, request.data);
               
                struct timespec end_time;
                clock_gettime(CLOCK_MONOTONIC, &end_time);
                double elapsed = (end_time.tv_sec - request.start_time.tv_sec) +
                                 (end_time.tv_nsec - request.start_time.tv_nsec) / 1e9;
               
                if (elapsed > QUANTUM && !request.completed) {
                    printf("Request from client %d exceeded quantum, requeuing...\n", request.client_socket);
                    enqueue_request(request);
                } else {
                    // Release resources if this was a file operation
                    if (resources_needed > 0) {
                        release_resources(request.client_socket % MAX_CLIENTS, resources_needed);
                    }
                }
            } else {
                char response[BUFFER_SIZE] = "ERROR: Resource allocation denied by Banker's algorithm";
                send(request.client_socket, response, strlen(response), 0);
            }
        }
    }
    return NULL;
}

void enqueue_request(client_request_t request) {
    pthread_mutex_lock(&queue_lock);
   
    if (queue_count < MAX_CLIENTS) {
        queue_rear = (queue_rear + 1) % MAX_CLIENTS;
        request_queue[queue_rear] = request;
        queue_count++;
        sem_post(&queue_sem);  // Signal scheduler
    }   
    pthread_mutex_unlock(&queue_lock);
}

client_request_t dequeue_request() {
    client_request_t request = { .client_socket = -1 };
    pthread_mutex_lock(&queue_lock);
   
    if (queue_count > 0) {
        request = request_queue[queue_front];
        queue_front = (queue_front + 1) % MAX_CLIENTS;
        queue_count--;
    }
    pthread_mutex_unlock(&queue_lock);
    return request;
}

void *handle_client(void *arg) {
    int client_socket = *(int*)arg;
    free(arg);
   
    char buffer[BUFFER_SIZE] = {0};
    int bytes_read;
    while ((bytes_read = read(client_socket, buffer, BUFFER_SIZE)) > 0) {
        int request_type = buffer[0] - '0';  // First byte is request type
        char data[BUFFER_SIZE - 1];
        memcpy(data, buffer + 1, bytes_read - 1);
        data[bytes_read - 1] = '\0';
    
        client_request_t request;
        request.client_socket = client_socket;
        request.request_type = request_type;
        strncpy(request.data, data, BUFFER_SIZE - 1);
        request.completed = false;
        time(&request.arrival_time);
    
        enqueue_request(request);    
        memset(buffer, 0, BUFFER_SIZE);
    }
   
    if (bytes_read == 0) {
        printf("Client disconnected\n");
    } else {
        perror("recv failed");
    }   
    close(client_socket);
    sem_post(&server_sem);  // Release server semaphore
    pthread_exit(NULL);
}

int process_request(int client_socket, int request_type, char *data) {
    char response[BUFFER_SIZE] = {0};
   
    printf("Processing request type: %d, Data: %s\n", request_type, data);   
    switch (request_type) {
        case OPEN_FILE: {
            return open_file_operation(data, client_socket);
        }
        case READ_FILE: {
            return read_file_operation(data, client_socket);
        }
        case WRITE_FILE: {
            char *content = strchr(data, '\n');
            if (content) {
                *content = '\0';  // Terminate filename string
                content++;  // Move to content portion
                return write_file_operation(data, content, client_socket);
            } else {
                sprintf(response, "ERROR: Invalid write request format");
                send(client_socket, response, strlen(response), 0);
                return -1;
            }
        }
        case CLOSE_FILE: {
            return close_file_operation(data, client_socket);
        }
        case DELETE_FILE: {
            return delete_file_operation(data, client_socket);
        }
        case LIST_FILES: {
            return list_files_operation(client_socket);
        }
        default: {
            sprintf(response, "ERROR: Unknown request type");
            send(client_socket, response, strlen(response), 0);
            return -1;
        }
    }
}

int main() {
    int server_fd, new_socket;
    struct sockaddr_in address;
    int addrlen = sizeof(address);
    pthread_t thread_id, scheduler_thread;
    
    struct stat st = {0};
    if (stat(STORAGE_DIR, &st) == -1) {
        mkdir(STORAGE_DIR, 0700);
    }
    
    init_sync_primitives();
    if (pthread_create(&scheduler_thread, NULL, round_robin_scheduler, NULL) != 0) {
        perror("Failed to create scheduler thread");
        exit(EXIT_FAILURE);
    }
    
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }
   
    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        perror("Setsockopt failed");
        exit(EXIT_FAILURE);
    }
    
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);
    
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("Bind failed");
        exit(EXIT_FAILURE);
    }
    
    if (listen(server_fd, MAX_CLIENTS) < 0) {
        perror("Listen failed");
        exit(EXIT_FAILURE);
    }
   
    printf("Server started. Listening on port %d...\n", PORT);
    while (1) {
        if ((new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {
            perror("Accept failed");
            continue;
        }
        sem_wait(&server_sem);
       
        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &address.sin_addr, client_ip, INET_ADDRSTRLEN);
        printf("New connection from %s:%d\n", client_ip, ntohs(address.sin_port));
       int *client_sock = malloc(sizeof(int));
        *client_sock = new_socket;
       
        if (pthread_create(&thread_id, NULL, handle_client, (void*)client_sock) < 0) {
            perror("Thread creation failed");
            close(new_socket);
            free(client_sock);
            sem_post(&server_sem);  // Release semaphore if thread creation fails
        } else {
                  pthread_detach(thread_id);
        }
    }   
    return 0;
}
