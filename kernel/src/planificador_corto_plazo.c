#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include <commons/collections/queue.h>
#include <commons/log.h>
#include <commons/collections/list.h>
#include <commons/collections/dictionary.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <stdbool.h>
#include <threads_kernel.h>
#include <planificador.h>
#include <protocolo.h>
#include <socket_utils.h>
#include <op_code.h>
#include "planificador_corto_plazo.h"

// Globals
extern t_log* logger;
extern int tiempo_suspension;
extern int socket_memoria;
extern char* ingreso_ready;
extern char* algoritmo_planificacion;
extern float alfa;
extern int estimacion_inicial;

// Colas
extern t_list* cola_new;
extern t_list* lista_ready;
extern t_queue* cola_susp_ready;
extern t_queue* cola_susp_blocked;

// Mutex
extern pthread_mutex_t mutex_new;
extern pthread_mutex_t mutex_ready;
extern pthread_mutex_t mutex_susp_ready;
extern pthread_mutex_t mutex_susp_blocked;

// Semáforos
extern sem_t sem_procesos_new;
extern sem_t sem_procesos_ready;
extern sem_t sem_procesos_susp_ready;
extern sem_t sem_desalojo;
extern sem_t sem_cpu_libre;

extern pthread_mutex_t mutex_desalojos_esperados;
extern t_list* desalojos_esperados;


void* planificador_corto_plazo(void* args) {
    while (1) {

        // CORRECTO
        if (strcmp(algoritmo_planificacion, "FIFO") == 0 || 
            strcmp(algoritmo_planificacion, "SJF") == 0) {
            sem_wait(&sem_cpu_libre);  // FIFO y SJF (sin desalojo)
        }
        // SRT NO espera porque puede desalojar

        sem_wait(&sem_procesos_ready);
        
        pthread_mutex_lock(&mutex_ready);
        // Ordenar lista según algoritmo
        if (strcmp(algoritmo_planificacion, "SJF") == 0) {
            list_sort(lista_ready, comparar_estimaciones);
            log_debug(logger, "READY ordenado por estimación (SJF)");
        } else if (strcmp(algoritmo_planificacion, "SRT") == 0) {
            list_sort(lista_ready, comparar_estimaciones);
            log_debug(logger, "READY ordenado por estimación (SRT)");
        } else if (strcmp(algoritmo_planificacion, "FIFO") == 0) {
            log_debug(logger, "READY sin ordenar (FIFO)");
        }
        
        t_pcb* pcb = list_remove(lista_ready, 0);
        pthread_mutex_unlock(&mutex_ready);
        
        t_cpu_conectada* cpu_destino = seleccionar_cpu_libre();
        
        if (cpu_destino == NULL) {
            if (strcmp(algoritmo_planificacion, "SRT") == 0) {
                t_pcb* pcb_en_cpu_mas_lenta = obtener_pcb_con_mayor_estimacion();
                    
                if (pcb_en_cpu_mas_lenta == NULL) {
                    log_info(logger, "## (%d) - No hay procesos ejecutando ni CPUs libres, esperando...", pcb->pid);
                    pthread_mutex_lock(&mutex_ready);
                    list_add(lista_ready, pcb);
                    pthread_mutex_unlock(&mutex_ready);
                    sem_wait(&sem_cpu_libre);
                    sem_post(&sem_procesos_ready);
                    continue;
                } else if (pcb->estimacion_rafaga < pcb_en_cpu_mas_lenta->estimacion_rafaga) {
                    log_info(logger, "## (%d) - Desalojando proceso por SRT (estimación: %.2f < %.2f)", pcb_en_cpu_mas_lenta->pid, pcb->estimacion_rafaga, pcb_en_cpu_mas_lenta->estimacion_rafaga);
                    
                    registrar_desalojo_esperado(pcb_en_cpu_mas_lenta->pid);
                    enviar_interrupcion_a_cpu_de_pid(pcb_en_cpu_mas_lenta->pid);
                    
                    if (esperar_desalojo_con_timeout(pcb_en_cpu_mas_lenta->pid, 500)) { // 500ms timeout
                        log_info(logger, "## (%d) Desalojado por SRT", pcb_en_cpu_mas_lenta->pid);
                        cpu_destino = seleccionar_cpu_libre();
                    } else {
                        log_warning(logger, "## (%d) Timeout esperando desalojo - proceso probablemente en syscall", pcb_en_cpu_mas_lenta->pid);
                        pthread_mutex_lock(&mutex_ready);
                        list_add(lista_ready, pcb);
                        pthread_mutex_unlock(&mutex_ready);
                        sem_wait(&sem_cpu_libre);
                        sem_post(&sem_procesos_ready);
                        continue;
                    }
                } else {
                    // No conviene desalojar → devolver a READY y esperar CPU libre
                    pthread_mutex_lock(&mutex_ready);
                    list_add(lista_ready, pcb);
                    pthread_mutex_unlock(&mutex_ready);
                    sem_wait(&sem_cpu_libre);
                    sem_post(&sem_procesos_ready);
                    continue;
                }
            } else {
                // Si no es SRT, se devuelve a ready y se espera CPU libre
                pthread_mutex_lock(&mutex_ready);
                list_add(lista_ready, pcb);
                pthread_mutex_unlock(&mutex_ready);
                sem_wait(&sem_cpu_libre);
                sem_post(&sem_procesos_ready);
                continue;
            }
        }
        
        // IMPORTANTE: Marcar tiempo de inicio de ejecución ANTES del cambio de estado
        pcb->tiempo_inicio_exec = timestamp_milisegundos();
        
        // Cambiar estado a EXEC
        cambiar_estado_con_metricas(pcb, ESTADO_EXEC);
        log_info(logger, "## (%d) Pasa del estado READY al estado EXEC", pcb->pid);
        log_info(logger, "## (%d) - Pasa de READY a EXEC (CPU: %s, Estimación: %.2f)", 
                pcb->pid, cpu_destino->id_cpu, pcb->estimacion_rafaga);
        
        // Registrar la ejecución
        registrar_ejecucion(pcb, cpu_destino->socket_dispatch, cpu_destino->socket_interrupt);
        
        // Enviar PCB al dispatch de la CPU seleccionada
        enviar_pcb(pcb, cpu_destino->socket_dispatch, logger);
    }
    return NULL;
}