#include "threads_mem.h"
#include <operaciones_m.h>
#include <socket_utils.h>
#include <commons/log.h>
#include <stdlib.h>
#include <string.h>
#include <protocolo.h>

//acepta conexiones, inicia un hilo por cliente
extern t_log* logger;
extern t_config* config;
extern int tam_pagina;
extern int entradas_por_tabla;
extern int cantidad_niveles;

void iniciar_servidor_memoria(t_log* logger, int puerto) {
    int socket_servidor = iniciar_servidor(puerto, logger);
    log_info(logger, "Servidor de Memoria escuchando en el puerto %d", puerto);

    while (1) {
        int socket_cliente = esperar_cliente(socket_servidor, logger);

        if (socket_cliente == -1) {
            log_error(logger, "Error aceptando cliente");
            continue;
        }

        t_args_server* args = malloc(sizeof(t_args_server));
        args->socket_cliente = socket_cliente;
        args->logger = logger;

        pthread_t hilo_cliente;
        pthread_create(&hilo_cliente, NULL, atender_cliente, (void*)args);
        pthread_detach(hilo_cliente); 
    }
}

void* atender_cliente(void* args) {
    t_args_server* datos = (t_args_server*) args;
    int socket_cliente = datos->socket_cliente;
    t_log* logger = datos->logger;
    free(datos); // ya copie los datos, puedo liberar

    while (1) {
        
        int cod_op = recibir_operacion(socket_cliente, logger);

        switch (cod_op) {
            case HANDSHAKE_CPU: {
                handshake_memoria(logger, socket_cliente, tam_pagina, entradas_por_tabla, cantidad_niveles);
                break;
            }

            case INICIAR_PROCESO: {
                log_info(logger, " ## Kernel Conectado - FD del socket: %d", socket_cliente);
                manejar_iniciar_proceso(socket_cliente, logger);
                break;
            }

            case FETCH: {
                manejar_fetch(socket_cliente, logger);
                break;
            }

            case FINALIZAR_PROCESO: {
                log_info(logger, " ## Kernel Conectado - FD del socket: %d", socket_cliente);
                finalizar_proceso(socket_cliente, logger);
                break;
            }

            case DUMP_MEMORY: {
                log_info(logger, " ## Kernel Conectado - FD del socket: %d", socket_cliente);
                dump_memoria(socket_cliente, logger);
                break;
            }

            case SUSPENDER_PROCESO: {
                log_info(logger, " ## Kernel Conectado - FD del socket: %d", socket_cliente);
                suspender_proceso_protegido(socket_cliente, logger);
                break;
            }

            case DESUSPENDER_PROCESO: {
                log_info(logger, " ## Kernel Conectado - FD del socket: %d", socket_cliente);
                desuspender_proceso(socket_cliente, logger);
                break;
            }

            case WRITE: {
                manejar_write(socket_cliente, logger);
                break;
            }

            case READ: {
                manejar_read(socket_cliente, logger);
                break;
            }

            case LEER_PAGINA_COMPLETA: {
                manejar_leer_pagina_completa(socket_cliente, logger);
                break;
            }

            case ESCRIBIR_PAGINA_COMPLETA: {
                manejar_escribir_pagina_completa(socket_cliente, logger);
                break;
            }

            case OBTENER_MARCO_FINAL:{
                manejar_obtener_marco_final(socket_cliente, logger);
            break;
            }
            
            default:{
                log_warning(logger, "Código no manejado: %d", cod_op);
                break;
            }
            
            case -1: {
                log_info(logger, "Cliente desconectado");
                close(socket_cliente);
                return NULL;
            }
        }

    }
    return NULL;
}