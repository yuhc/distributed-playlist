#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <cstring>
#include <stdbool.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/queue.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <pthread.h>
#include <sys/time.h>
#include <stdint.h>
#include <math.h>

#define TOTAL_PROCESSES 5
#define TOTAL_TIME 100
#define BASE_PORT 3456
#define MAX_MSG_LEN 256

void usage() {
  printf("Usage: ./process <process_id>\n");
}

typedef struct command{
  int delay[TOTAL_PROCESSES];
} command_t;

struct message {
  int vec_clk[TOTAL_PROCESSES];
  char msg[MAX_MSG_LEN];
};

struct action {
  int time;
  int to_process[TOTAL_PROCESSES];
  bool first;
  struct message msg;
  TAILQ_ENTRY(action) actions;
};

struct pending_msg {
  struct message msg;
  TAILQ_ENTRY(pending_msg) pending_msgs;
};

TAILQ_HEAD(action_head, action) action_list_head;
TAILQ_HEAD(pending_head, pending_msg) pending_list_head;

pthread_mutex_t mutex;

int vec_clk[TOTAL_PROCESSES] = {0};
command_t command = {0};
int D[TOTAL_PROCESSES] = {0};

struct thread_info {
  int process_id;
  FILE* log_fp;
  uint64_t start_time;
};

#define MAX_PATH_NAME 256

FILE* log_init(int process_id) {
  char filename[MAX_PATH_NAME];
  snprintf(filename, MAX_PATH_NAME, "log_%d.txt", process_id);
  FILE* fp = fopen(filename, "a+");
  return fp;
}

void parse_config(int process_id, command_t* command) {
  int row_count = 0, column_count = 0;
  char* line = NULL;
  size_t len = 0;
  ssize_t read;
  FILE* config_fp = fopen("config.txt", "r");
  if (config_fp == NULL) {
    perror("fopen");
    exit(EXIT_FAILURE);
  }
  while ((read = getline(&line, &len, config_fp)) != -1) {
    if (row_count < TOTAL_PROCESSES) {
      char* num = strtok(line, " ");
      int delay_num = atoi(num);
      if (column_count > row_count) {
        if (row_count == process_id) {
          command->delay[column_count] = delay_num;
        } else if (column_count == process_id) {
          command->delay[row_count] = delay_num;
        }
      }
      for (column_count = 1; column_count < TOTAL_PROCESSES; column_count++) {
        num = strtok(NULL, " ");
        delay_num = atoi(num);
        if (column_count > row_count) {
          if (row_count == process_id) {
            command->delay[column_count] = delay_num;
          } else if (column_count == process_id) {
            command->delay[row_count] = delay_num;
          }
        }
      }
    } else {
      int req_pid = 0;
      int action_time = 0;
      char* num = strtok(line, " ");
      req_pid = atoi(num);
      if (req_pid == process_id) {
        while (num != NULL) {
          num = strtok(NULL, " ");
          if (num == NULL) {
            break;
          }
          action_time = atoi(num);
          struct action* entry = (struct action*)malloc(sizeof(struct action));
          entry->time = action_time;
          memset(entry->to_process, 0, sizeof(entry->to_process));
          entry->first = true;
          TAILQ_INSERT_TAIL(&action_list_head, entry, actions);
        }
      }
    }
    row_count ++;
  }
  free(line);
  fclose(config_fp);
}

void lookup_name(const char* name, int port, struct sockaddr_storage *ret_addr,
                 size_t *ret_addrlen) {
  struct addrinfo hints, *results;
  int retval;

  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  if ((retval = getaddrinfo(name, NULL, &hints, &results)) != 0) {
    fprintf(stderr, "getaddrinfo failed in lookup: %s\n", gai_strerror(retval));
    exit(EXIT_FAILURE);
  }

  if (results->ai_family == AF_INET) {
    struct sockaddr_in *v4addr = (struct sockaddr_in *) results->ai_addr;
    v4addr->sin_port = htons(port);
  } else if (results->ai_family == AF_INET6) {
    struct sockaddr_in6 *v6addr = (struct sockaddr_in6 *) results->ai_addr;
    v6addr->sin6_port = htons(port);
  } else {
    fprintf(stderr, "Fail to provide an IPv4 or IPv6 address\n");
    freeaddrinfo(results);
  }

  memcpy(ret_addr, results->ai_addr, results->ai_addrlen);
  *ret_addrlen = results->ai_addrlen;

  freeaddrinfo(results);
}

void connect_server(const struct sockaddr_storage addr,
                    const size_t addrlen, int* ret_fd) {
  int socket_fd = socket(addr.ss_family, SOCK_STREAM, 0);
  if (socket_fd == -1) {
    perror("socket");
    exit(EXIT_FAILURE);
  }
  int optval = 1;
  //fcntl(socket_fd, F_SETFL, O_NONBLOCK);
  setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));

  int res = connect(socket_fd, (const struct sockaddr *)&addr, addrlen);

  if (res == -1) {
    perror("connect");
    exit(EXIT_FAILURE);
  }

  *ret_fd = socket_fd;
}

int server_listen(const char* portnum, int* sock_family) {
  struct addrinfo hints;
  memset(&hints, 0, sizeof(struct addrinfo));
  hints.ai_family = AF_INET6;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;
  hints.ai_flags |= AI_V4MAPPED;
  hints.ai_protocol = IPPROTO_TCP;
  hints.ai_canonname = NULL;
  hints.ai_addr = NULL;
  hints.ai_next = NULL;

  struct addrinfo *results;
  int res = getaddrinfo(NULL, portnum, &hints, &results);
  if (res != 0) {
    fprintf(stderr, "getaddrinfo failed in server listen: %s\n", gai_strerror(res));
    exit(EXIT_FAILURE);
  }

  int listen_fd = -1;

  for (struct addrinfo* rp = results; rp != NULL; rp = rp->ai_next) {
    listen_fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
    if (listen_fd == -1) {
      perror("socket() failed: ");
      listen_fd = -1;
      continue;
    }

    int optval = 1;
    //fcntl(listen_fd, F_SETFL, O_NONBLOCK);
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
    if (bind(listen_fd, rp->ai_addr, rp->ai_addrlen) == 0) {
      *sock_family = rp->ai_family;
      break;
    }

    close(listen_fd);
    listen_fd = -1;
  }

  freeaddrinfo(results);

  if (listen_fd == -1) {
    fprintf(stderr, "Could not bind any addr!\n");
    return -1;
  }

  if (listen(listen_fd, SOMAXCONN) != 0) {
    perror("listen");
    close(listen_fd);
    return -1;
  }

  return listen_fd;
}

int get_send_process(char* msg) {
  char * save_ptr, *save_ptr2;
  char copy_msg[MAX_MSG_LEN];
  memcpy(&copy_msg, msg, MAX_MSG_LEN);
  char* first = strtok_r(copy_msg, ":", &save_ptr);
  char* second = strtok_r(first, "_", &save_ptr2);
  char* final = strtok_r(NULL, "_", &save_ptr2);
  return atoi(final);
}

bool check_deliver(int send_process, int* vec_clk, int size) {
  if (D[send_process] != (vec_clk[send_process] - 1))
    return false;
  for (int i = 0; i < size; i++) {
    if (i != send_process) {
      if (D[i] < vec_clk[i])
        return false;
    }
  }
  return true;
}

void update_vec_clk(int* ts_vec_clk, int send_process, int process_id) {
  D[send_process] = ts_vec_clk[send_process];
  for (int i = 0; i < TOTAL_PROCESSES; i++) {
    if (i != process_id) {
      if (ts_vec_clk[i] > vec_clk[i]) {
        pthread_mutex_lock(&mutex);
        vec_clk[i] = ts_vec_clk[i];
        pthread_mutex_unlock(&mutex);
      }
    }
  }
}

void* receive(void* arg) {
  struct thread_info* tinfo = (struct thread_info*) arg;
  int sock_family;
  int port_num = BASE_PORT + tinfo->process_id;
  int listen_fd = server_listen(std::to_string(port_num).c_str(), &sock_family);
  if (listen_fd <= 0) {
    fprintf(stderr, "Could not bind to any address!\n");
    exit(EXIT_FAILURE);
  }
  while(1) {
    struct message msg;
    memset(&msg, 0, sizeof(struct message));
    struct sockaddr_storage caddr;
    socklen_t caddr_len = sizeof(caddr);
    int client_fd = accept(listen_fd, (struct sockaddr*)&caddr, &caddr_len);
    if (client_fd < 0) {
      if ((errno == EAGAIN) || (errno == EINTR)) {
        continue;
      }
      perror("accept");
      break;
    }

    while (1) {


      int res = read(client_fd, (char*)&msg, sizeof(struct message));
      if (res == 0) {
        break;
      }

      struct timeval cur;
      gettimeofday(&cur, NULL);
      uint64_t cur_time = cur.tv_sec*1000000 + cur.tv_usec;
      double time_passed = (cur_time - tinfo->start_time)/1000000.0;
      if (res != 0) {
        fprintf(tinfo->log_fp, "%f\tp_%d REC %s\n", time_passed, tinfo->process_id, msg.msg);
        fflush(tinfo->log_fp);
      }


      int send_process = get_send_process(msg.msg);
      if (check_deliver(send_process, msg.vec_clk, TOTAL_PROCESSES)) {
        fprintf(tinfo->log_fp, "%f\tp_%d DLR %s\n", time_passed, tinfo->process_id, msg.msg);
        fflush(tinfo->log_fp);
        update_vec_clk(msg.vec_clk, send_process, tinfo->process_id);
      } else {
        struct pending_msg* p_msg = (struct pending_msg*) malloc(sizeof(struct pending_msg));
        memcpy(&p_msg->msg, &msg, sizeof(struct message));
        TAILQ_INSERT_TAIL(&pending_list_head, p_msg, pending_msgs);
      }
      struct pending_msg* entry, *tmp_entry;
      bool updated = true;
      bool changed = false;
      while (updated) {
        for (entry = TAILQ_FIRST(&pending_list_head); entry != NULL; entry = tmp_entry) {
          tmp_entry = TAILQ_NEXT(entry, pending_msgs);
          int send_process_ = get_send_process(entry->msg.msg);
          if (check_deliver(send_process_, entry->msg.vec_clk, TOTAL_PROCESSES)) {
            fprintf(tinfo->log_fp, "%f\tp_%d DLR %s\n", time_passed, tinfo->process_id, entry->msg.msg);
            fflush(tinfo->log_fp);
            update_vec_clk(entry->msg.vec_clk, send_process_, tinfo->process_id);
            TAILQ_REMOVE(&pending_list_head, entry, pending_msgs);
            free(entry);
            changed = true;
          }
        }
        if (TAILQ_EMPTY(&pending_list_head)) {
          break;
        }
        if (changed) {
          updated = true;
          changed = false;
        } else
          updated = false;
      }
    }
    close(client_fd);
  }
  close(listen_fd);
}

bool check_finished(int* arr, int size) {
  int i = 0;
  for (i = 0; i < size; i++) {
    if (arr[i] == 0)
      return false;
  }
  return true;
}

void set_init_msg(int* arr, int size, int num) {
  int i = 0;
  for (i = 0; i < size; i++) {
    arr[i] = num;
  }
}

void* send_to_server(void* arg) {
  struct thread_info* tinfo = (struct thread_info*) arg;
  int sockfd[TOTAL_PROCESSES];
  struct timeval cur;
  int msg_content[TOTAL_PROCESSES];
  set_init_msg(msg_content, TOTAL_PROCESSES, 1);
  while(1) {
    gettimeofday(&cur, NULL);
    uint64_t cur_time = cur.tv_usec + cur.tv_sec * 1000000;
    double time_passed = (cur_time - tinfo->start_time)/1000000.0;
    struct action* entry, *tmp_entry;
    for (int i = 0; i < TOTAL_PROCESSES; i++) {
      for (entry= TAILQ_FIRST(&action_list_head); entry != NULL;  entry=tmp_entry) {
        tmp_entry = TAILQ_NEXT(entry, actions);
        if ((time_passed - entry->time) >= 0 && (entry->first)) {
          struct message msg;
          memset(&msg, 0, sizeof(struct message));
          pthread_mutex_lock(&mutex);
          vec_clk[tinfo->process_id]++;
          pthread_mutex_unlock(&mutex);
          memcpy(msg.vec_clk, vec_clk, sizeof(vec_clk));
          snprintf(msg.msg, sizeof(msg.msg), "p_%d:%d", tinfo->process_id, msg_content[i]);
          fprintf(tinfo->log_fp, "%f\tp_%d BRC %s\n", time_passed, tinfo->process_id, msg.msg);
          fflush(tinfo->log_fp);
          memcpy(&(entry->msg), &msg, sizeof(struct message));
          entry->first = false;
        }
        if (time_passed - (entry->time + command.delay[i]) >= 0) {

          struct sockaddr_storage addr;
          size_t addrlen;
          lookup_name("127.0.0.1", BASE_PORT+i, &addr, &addrlen);
          connect_server(addr, addrlen, &sockfd[i]);
          int wres = 0;
          if (entry->to_process[i] == 0) {
            wres = write(sockfd[i], (char*)&entry->msg, sizeof(struct message));
          }
          if (wres != 0) {
            entry->to_process[i] = 1;
            msg_content[i]++;
          }
          close(sockfd[i]);
          if (check_finished(entry->to_process, TOTAL_PROCESSES)) {
            TAILQ_REMOVE(&action_list_head, entry, actions);
            free(entry);
          }
        }
      }
    }
    if (TAILQ_EMPTY(&action_list_head)){
      printf("process %d list is empty\n", tinfo->process_id);
      break;
    }
    sleep(1);
  }
}

int main(int argc, char** argv) {
  if (argc < 2) {
    usage();
    exit(EXIT_FAILURE);
   }
  int process_id = atoi(argv[1]);
  TAILQ_INIT(&action_list_head);
  TAILQ_INIT(&pending_list_head);
  parse_config(process_id, &command);
  FILE* log_fp = log_init(process_id);
  struct timeval start;
  gettimeofday(&start, NULL);
  uint64_t start_time = start.tv_usec + start.tv_sec*1000000;
  pthread_t p1, p2;
  struct thread_info tinfo1, tinfo2;
  tinfo1.process_id = process_id;
  tinfo1.log_fp = log_fp;
  tinfo1.start_time = start_time;
  tinfo2.process_id = process_id;
  tinfo2.log_fp = log_fp;
  tinfo2.start_time = start_time;
  pthread_create(&p2, NULL, &receive, &tinfo2);
  pthread_create(&p1, NULL, &send_to_server, &tinfo1);
  pthread_join(p1, NULL);
  pthread_join(p2, NULL);
  fclose(log_fp);
}
