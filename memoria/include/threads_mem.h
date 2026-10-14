#ifndef THREADS_MEM_H
#define THREADS_MEM_H

#include <pthread.h>
#include <commons/log.h>
#include <commons/config.h>
#include <socket_utils.h>

// === Estructura usada para pasar args al hilo ===
typedef struct {
    int socket_cliente;
    t_log* logger;
} t_args_server;

// === Funciones principales ===
void iniciar_servidor_memoria(t_log* logger, int puerto);
void* atender_cliente(void* args);

#endif