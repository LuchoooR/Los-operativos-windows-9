#ifndef THREAD_IO_H
#define THREAD_IO_H

#include <pthread.h>
#include <commons/log.h>

typedef struct{
    int pid;
    int tiempo_io;
    int socket_kernel;
    char* dispositivo;
    t_log* logger;
}t_args_io;

void* atender_io(void* args);

#endif