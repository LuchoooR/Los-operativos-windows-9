#ifndef THREAD_KERNEL_H
#define THREAD_KERNEL_H

#include <pthread.h>
#include <commons/log.h>

typedef struct{
    int socket_io;
    int socket_dispatch;
    int socket_interrupt;

    t_log* logger;

    pthread_mutex_t lock_io;
    pthread_mutex_t lock_dispatch;
    pthread_mutex_t lock_interrupt;
} t_contexto_kernel;

typedef struct {
    int puerto;
    const char* nombre;
    int* socket_destino; 
    pthread_mutex_t* lock;
    t_log* logger;
} t_args_escucha;

typedef struct {
    char* id_cpu;
    int socket_dispatch;
    int socket_interrupt;
    bool libre;
} t_cpu_conectada;


void* thread_escucha_modulo(void* args);

void iniciar_hilos_escucha(int puerto_io, int puerto_dispatch, int puerto_interrupt);

void* thread_escucha_dispatch(void* args);

void* thread_escucha_interrupt(void* args);

void* thread_escucha_io(void* args);

void* atender_cpu_dispatch(void* arg);

void* atender_cpu_interrupt(void* arg);

void* atender_dispositivo_io(void* arg);

#endif