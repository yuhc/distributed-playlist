#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

extern "C" void lookup_name(const char* name, int port, struct sockaddr_storage *ret_addr,
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

extern "C" void connect_server(const struct sockaddr_storage addr,
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

extern "C" int server_listen(const char* portnum, int* sock_family) {
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
