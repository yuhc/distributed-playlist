#ifndef __LIBNET_H__
#define __LIBNET_H__ 1

#include <sys/types.h>
#include <sys/socket.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

void lookup_name(const char* name, int port, struct sockaddr_storage *ret_addr,
                 size_t *ret_addrlen);
void connect_server(const struct sockaddr_storage addr,
                    const size_t addrlen, int* ret_fd);
int server_listen(const char* portnum, int* sock_family);

#ifdef __cplusplus
}
#endif

#endif
