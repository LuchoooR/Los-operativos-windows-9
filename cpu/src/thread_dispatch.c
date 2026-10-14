#include <cpu.h>
#include "thread_interrupt.h"
#include <protocolo.h>
#include <commons/log.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

extern int socket_memoria;
extern int socket_dispatch;
extern t_log* logger;
extern volatile bool interrupcion;

void* thread_dispatch(void* args){

    log_info(logger, "Dispatch: Hilo Iniciado");

    while(1){
        log_info(logger, "Dispatch: esperando PCB...");

        t_pcb pcb = recibir_pcb(socket_dispatch, logger);

        if (pcb.pid == -1) {
            log_warning(logger, "PCB inválido recibido, cerrando hilo dispatch");
            break;
        }

        log_info(logger, "Dispatch: inicio ciclo instrucciones, interrupcion = %d", interrupcion);


        while (1){

            int cod_op = fetch(socket_memoria, logger, &pcb);

            if (cod_op == -1) {
                log_info(logger, "UUU LOS CABLES WACHO JAJAJA");
                break;
            }

            decode_and_execute(cod_op, socket_memoria, socket_dispatch, logger, &pcb);

            if (interrupcion){
                log_info(logger, "Interrupcion detectada. Finalizando ejecucion de pid %d", pcb.pid);
                pcb.estado = ESTADO_EXEC;
                break;
            }             
        
            if (pcb.estado == ESTADO_BLOCKED){
                log_info(logger, "Se ejecutó IO. Finalizando ciclo de instrucciones para PID=%d", pcb.pid);
                break;
            }

            if (cod_op == EXIT || pcb.estado == ESTADO_EXIT) {
                log_info(logger, "Se ejecutó EXIT. Finalizando ciclo de instrucciones para PID=%d", pcb.pid);
                break;
            }
        
        }

        if(interrupcion == true){
            log_info(logger, "Dispatch: se terminó el ciclo por interrupción");
            
        }

        if(pcb.estado != ESTADO_EXIT && pcb.estado != ESTADO_BLOCKED && pcb.estado != ESTADO_EXEC){
            pcb.estado = ESTADO_READY;
        }
        
        if (pcb.estado != ESTADO_EXIT && pcb.estado != ESTADO_BLOCKED){
            limpiar_tlb_de_proceso(pcb.pid, logger);
            limpiar_cache_de_proceso(pcb.pid, logger);

            log_info(logger, "Devolviendo PCB a Kernel...");
            enviar_pcb(&pcb, socket_dispatch, logger);
            
            int cod_op = DESALOJO;
            send(socket_dispatch,&cod_op, sizeof(int),0);

            interrupcion = false;
        }
    }

    log_info(logger, "Dispatch: Hilo Cerrado");   

    return NULL;
}