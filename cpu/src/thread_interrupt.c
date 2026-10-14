#include "thread_interrupt.h"
#include <commons/log.h>
#include <protocolo.h>
#include <unistd.h>
#include <stdio.h>
#include <netdb.h>

extern int socket_interrupt;
extern t_log* logger;

volatile bool interrupcion = false;

void* thread_interrupt(void* args){

    log_info(logger, "Interrupt: HIlo Iniciado");

    while (1) { 
        
        int op_code = -1;

        //bloqueante
        int recibidos = recv(socket_interrupt, &op_code, sizeof(int), MSG_WAITALL);

        if (recibidos <= 0) {
            log_error(logger, "Interrupt: error al recibir código o conexión cerrada");
            break;
        }

        if (op_code == DESALOJO){
            log_info(logger, "## Llega interrupción al puerto Interrupt");
            interrupcion = true;
            log_info(logger, "Interrupt: se recibió DESALOJO, activando interrupción");
        } else {
            log_warning(logger, "Interrupt: código desconocido recibido: %d", op_code);
        }

    }

    log_info(logger, "Interrupt: Hilo Cerrado");
    
    return NULL;
}
