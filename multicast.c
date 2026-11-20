
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <errno.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <time.h>
#include <sys/poll.h>


#define BUFFER_SIZE 256
#define MAX_PEERS 100


char MULTICAST_GROUP[20] = "239.0.0.1";
int PORT = 12345;
int sock;
struct sockaddr_in group_addr;
char self_id[32];
char filename[36];
char known_peers[MAX_PEERS][64];
int peer_count = 0;
int controller_HB = 3; // last saved set_interval or 3
int client_HB = 5; // default HB interval (only used if client)
int role = 0; // 0 for client, 1 for controller
int controller_detected = 0;

pthread_mutex_t peer_mutex = PTHREAD_MUTEX_INITIALIZER;


void add_peer(const char *id) {
    pthread_mutex_lock(&peer_mutex);
    for (int i = 0; i < peer_count; i++) {
        if (strcmp(known_peers[i], id) == 0) {
            pthread_mutex_unlock(&peer_mutex);
            return;
        }
    }

    if (peer_count < MAX_PEERS) {
        strncpy(known_peers[peer_count++], id, 64);
        printf("[New peer discovered] ID: %s\n", id);
    }
    pthread_mutex_unlock(&peer_mutex);
}

void log_entry(char *buffer, size_t bufferSize, const char *message, const char *fileName) {
    time_t now = time(NULL);
    struct tm *t = localtime(&now);

    // Format the log message into the provided buffer
    strftime(buffer, bufferSize, "[%Y-%m-%d %H:%M:%S] ", t);
    strncat(buffer, message, bufferSize - strlen(buffer) - 1);

    // Append the log message to the file
    FILE *file = fopen(fileName, "a");
    if (file) {
        fprintf(file, "%s\n", buffer);
        fclose(file);
    } else {
        perror("Failed to open log file");
    }
}

void *send_thread(void *arg){
    char message[BUFFER_SIZE];
    while (1) {
       fflush(stdout);
            if (fgets(message, BUFFER_SIZE, stdin) == NULL) {
                break;
            }
    
            size_t len = strlen(message);
            if (len > 0 && message[len - 1] == '\n') {
                message[len - 1] = '\0';
            }

            //check if first term is SET_INTERVAL
            if (strncmp(message, "SET_INTERVAL", 12) == 0) {
                if (role == 0){
                    fprintf(stderr, "You are not a controller. Cannot set interval.\n");
                    continue;
                }
                int new_interval = atoi(message + 13);
                if (new_interval < 0 || new_interval > 30) {
                    fprintf(stderr, "Invalid heartbeat interval. Must be a positive integer.\n");
                    continue;
                }
                controller_HB = new_interval;
            } 

            // if (strncmp(message, "exit", 4) == 0) {
            //     printf("attempting to exit\n");
            //     exit(0);
            // }

            char full_message[BUFFER_SIZE];
            snprintf(full_message, BUFFER_SIZE, "%s %s", self_id, message);
            sendto(sock, full_message, strlen(full_message), 0,
                (struct sockaddr *)&group_addr, sizeof(group_addr));

             if (strncmp(message, "exit", 4) == 0 || strncmp(message, "abort", 5) == 0) {
                printf("attempting to exit\n");
                exit(0);
            }
       
    }
    return NULL;
}

void *receive_thread(void *arg) {
    char buffer[BUFFER_SIZE];
    struct sockaddr_in sender_addr;
    socklen_t addrlen = sizeof(sender_addr);

    while (1) {
        ssize_t recv_len = recvfrom(sock, buffer, BUFFER_SIZE - 1, 0,
                                    (struct sockaddr *)&sender_addr, &addrlen);
        if (recv_len < 0) {
            perror("recvfrom");
            continue;
        }

        buffer[recv_len] = '\0';

        // Ignore our own messages
        if (strstr(buffer, self_id) != NULL) continue;

        char sender[32], command[32], params[16];
        sscanf(buffer, "%15s %63s %[^\n]", sender, command, params);

        
        //printf("\n[%s]: %s %s\n", sender, command, params);
        printf("%s\n"   , buffer);
  
        char logbuffer [BUFFER_SIZE];
        if (role == 0 || role == 2) { // if client or pending
            if (strcmp(command, "SET_INTERVAL") == 0) {
                controller_detected = 1; //controller detected
                int new_interval = atoi(params);
                client_HB = new_interval;
                log_entry(logbuffer, sizeof(logbuffer), buffer, filename);
            }
            
        } else if (role == 1) { // if controller
            if (strcmp(command, "HEARTBEAT") == 0) {
                log_entry(logbuffer, sizeof(logbuffer), buffer, filename);

            } else if (strcmp(command, "exit") == 0) {
                snprintf(buffer, BUFFER_SIZE, "%s %s", sender, "CONTROLLER_DOWN");
                log_entry(logbuffer, sizeof(logbuffer), buffer, filename);
            }
            
        }
        //printf("> ");
        fflush(stdout);
    }

    return NULL;
}

// Internal struct used only inside the implementation
typedef struct {
    int *target;
    int delay_secs;
} _InternalArgs;

// Thread function that does the delayed update (used to change roles)
void* _delayed_update_thread(void* arg) {
    _InternalArgs *args = (_InternalArgs*)arg;
    sleep(args->delay_secs);
    if (controller_detected == 0) {// if no controller detected
        printf("No controllers detect. Becoming new controller.\n");
        *(args->target) = 1; // then become new controller
        controller_detected = 1;
    } else {
        printf("Controller detected. Becoming client.\n");
        *(args->target) = 0;
    }
    
    free(args);
    return NULL;
}

// Public function that launches the thread with clear arguments
void delayed_update(int *target, int delay_secs) {
    _InternalArgs *args = malloc(sizeof(_InternalArgs));
    args->target = target;
    args->delay_secs = delay_secs;

    pthread_t thread_id;
    if (pthread_create(&thread_id, NULL, _delayed_update_thread, args) != 0) {
        perror("Failed to create thread");
        free(args);
        return;
    }

    pthread_detach(thread_id);  // Detach so we don't have to join later
}

int main(int argc, char *argv[]) {

    if (argc < 2) { // check for userID
        fprintf(stderr, "Missing userID\n");
        exit(EXIT_FAILURE);
    } else if (strlen(argv[1]) > 32) { // check userID length
        fprintf(stderr, "userID too long (max 32 bytes)\n");
        exit(EXIT_FAILURE);
    }
    //assign argv[1] to self_id
    strncpy(self_id, argv[1], 32);
    snprintf(filename, 36, "%s %s", self_id, ".txt");


    if (argc > 2) { // if flag detected
        if (strcmp(argv[2], "-c") == 0) { 
            // sender role
            printf("setting custom heartbeat interval\n");
            role = 2; // pending state (not controller or client)
            delayed_update(&role, 6);

            if (argc > 3) { // if user inputted custom HB interval
                //check if argv[3] is an int
                int custom_HB = atoi(argv[3]);
                if (custom_HB < 0 || custom_HB > 30) {
                    controller_HB = custom_HB;
                    fprintf(stderr, "Invalid heartbeat interval. Must be a positive integer.\n");
                } else {
                    controller_HB = custom_HB;
                }
            }
        } else if (strcmp(argv[2], "-p") == 0) {
            // receiver role
            //write arg into PORT
            if (argc > 3) { // if user inputted custom PORT
                int custom_port = atoi(argv[3]);
                if (custom_port < 0 || custom_port > 65535) {
                    fprintf(stderr, "Invalid port number. Must be between 0 and 65535.\n");
                    exit(EXIT_FAILURE);
                } else {
                    PORT = custom_port;
                }
            }

        } else {
            fprintf(stderr, "Invalid option. Has to be -c or -p\n");
            exit(EXIT_FAILURE);
        }
    }

    struct sockaddr_in local_addr;
    struct ip_mreq mreq;
    char message[BUFFER_SIZE];


    // Create UDP socket
    sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        perror("socket creation failed");
        exit(EXIT_FAILURE);
    }

    int reuse = 1;
    if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (char *)&reuse, sizeof(reuse)) < 0) {
        perror("setsockopt SO_REUSEADDR");
        close(sock);
        exit(EXIT_FAILURE);
    }

    memset(&local_addr, 0, sizeof(local_addr));
    local_addr.sin_family = AF_INET;
    local_addr.sin_port = htons(PORT);
    local_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(sock, (struct sockaddr *)&local_addr, sizeof(local_addr)) < 0) {
        perror("bind failed");
        close(sock);
        exit(EXIT_FAILURE);
    }

    mreq.imr_multiaddr.s_addr = inet_addr(MULTICAST_GROUP);
    mreq.imr_interface.s_addr = htonl(INADDR_ANY);

    if (setsockopt(sock, IPPROTO_IP, IP_ADD_MEMBERSHIP, (char *)&mreq, sizeof(mreq)) < 0) {
        perror("setsockopt - IP_ADD_MEMBERSHIP");
        close(sock);
        exit(EXIT_FAILURE);
    }

    memset(&group_addr, 0, sizeof(group_addr));
    group_addr.sin_family = AF_INET;
    group_addr.sin_addr.s_addr = inet_addr(MULTICAST_GROUP);
    group_addr.sin_port = htons(PORT);

    // Launch receive thread
    pthread_t recv_tid, send_tid;
    pthread_create(&recv_tid, NULL, receive_thread, NULL);
    pthread_create(&send_tid, NULL, send_thread, NULL);

    // Send HELLO
    // snprintf(message, BUFFER_SIZE, "HELLO %s", self_id);
    // sendto(sock, message, strlen(message), 0, (struct sockaddr *)&group_addr, sizeof(group_addr));

    printf("Multicast peer started with ID: %s\n", self_id);

    //random wait time for clients
    if (role == 0){
        int delay_us = rand() % 900001; // random delay between 0 and 900000 microseconds
        usleep(delay_us); //random sleep between 0 and 0.9 secds  
    }
    
    while (1) {
        sleep(1);
        fflush(stdout);

        if (role == 0) { // if client
            sprintf(message, "HEARTBEAT");
            sleep(client_HB);
        } else if (role == 1) {
            sprintf(message, "SET_INTERVAL %d", controller_HB);
            sleep(5);
        }

        if (role != 2) { // if not pending
            size_t len = strlen(message);
            if (len > 0 && message[len - 1] == '\n') {
                message[len - 1] = '\0';
            }

            char full_message[BUFFER_SIZE];
            snprintf(full_message, BUFFER_SIZE, "%s %s", self_id, message);
            sendto(sock, full_message, strlen(full_message), 0,
                (struct sockaddr *)&group_addr, sizeof(group_addr));
        }
        
    }

    pthread_cancel(send_tid);
    pthread_cancel(recv_tid);
    pthread_join(recv_tid, NULL);
    pthread_join(send_tid, NULL);
    close(sock);
    return 0;
}
