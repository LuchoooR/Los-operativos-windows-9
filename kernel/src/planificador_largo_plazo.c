#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include <commons/collections/queue.h>
#include <commons/log.h>
#include <commons/collections/list.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <stdbool.h>
#include <threads_kernel.h>
#include <planificador.h>
#include <protocolo.h>
#include <socket_utils.h>
#include <op_code.h>
#include "planificador_largo_plazo.h"


extern t_log* logger;
extern int tiempo_suspension;
extern int puerto_memoria;
extern char* ip_memoria;
extern char* ingreso_ready;
extern char* algoritmo_planificacion;
extern float alfa;
extern int estimacion_inicial;

// Colas
extern t_list* lista_new;
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

void* planificador_largo_plazo(void* args) {
    // 1. Inicio bloqueado hasta que el usuario presione ENTER
    log_info(logger, "Presione ENTER para iniciar planificación...");
    getchar();

    while (1) {
        // 2. Prioridad a procesos en cola SUSP_READY antes que NEW
        if (!queue_is_empty(cola_susp_ready)) {
            log_info(logger, "Se prioriza antes el que esta en SUSP READY");
            sem_wait(&sem_procesos_susp_ready);
            pthread_mutex_lock(&mutex_susp_ready);
            t_pcb* pcb = queue_pop(cola_susp_ready);
            pthread_mutex_unlock(&mutex_susp_ready);
            
            int socket_memoria = crear_conexion(ip_memoria, puerto_memoria);
            avisar_desuspension_a_memoria(pcb, socket_memoria);
            int cod_op = recibir_operacion(socket_memoria, logger);
            
            if (cod_op == RESPUESTA_OK) {
                cambiar_estado_con_metricas(pcb, ESTADO_READY);
                log_info(logger, "## (%d) Pasa del estado SUSP_READY al estado READY", pcb->pid);
                pthread_mutex_lock(&mutex_ready);
                list_add(lista_ready, pcb);
                pthread_mutex_unlock(&mutex_ready);
                sem_post(&sem_procesos_ready);
            } else {
                // CAMBIO: Devolver el proceso a SUSP_READY
                log_info(logger, "## (%d) No hay espacio para desuspender. Vuelve a SUSP_READY", pcb->pid);
                pthread_mutex_lock(&mutex_susp_ready);
                queue_push(cola_susp_ready, pcb);
                pthread_mutex_unlock(&mutex_susp_ready);
                sem_post(&sem_procesos_susp_ready); // Importante: restaurar el semáforo
            }
            
            liberar_conexion(socket_memoria);
            continue;
        }
    
    // ... resto del código
        // 3. Esperar un proceso nuevo en la cola NEW
        if (!list_is_empty(lista_new) && queue_is_empty(cola_susp_ready)){    
            sem_wait(&sem_procesos_new);
            pthread_mutex_lock(&mutex_new);
            t_pcb* pcb = NULL;

            if (strcmp(ingreso_ready, "FIFO") == 0) {
                pcb = list_remove(lista_new, 0);
            } 
            else if (strcmp(ingreso_ready, "PMCP") == 0) {
                list_sort(lista_new, comparar_por_tamanio);
                pcb = list_remove(lista_new, 0);
            }

            pthread_mutex_unlock(&mutex_new);

            // 4. Intentar iniciar el proceso en Memoria
            t_proceso_arch* proceso = malloc(sizeof(t_proceso_arch));
            proceso->pid = pcb->pid;
            proceso->tamanio = pcb->tamanio;
            proceso->arch = strdup(pcb->path);

            int socket_memoria = crear_conexion(ip_memoria, puerto_memoria);

            enviar_proceso(proceso, socket_memoria, INICIAR_PROCESO);
            int cod_op = recibir_operacion(socket_memoria, logger);

            if (cod_op == RESPUESTA_OK) {
                // 5. Memoria aceptó: pasa a READY
                cambiar_estado_con_metricas(pcb, ESTADO_READY);
                log_info(logger, "## (%d) Pasa del estado NEW al estado READY", pcb->pid);
                
                pthread_mutex_lock(&mutex_ready);
                list_add(lista_ready, pcb);
                pthread_mutex_unlock(&mutex_ready);

                sem_post(&sem_procesos_ready);
            } else {
                // 6. Memoria rechazó: vuelve a NEW y se intentará luego
                pthread_mutex_lock(&mutex_new);
                list_add_in_index(lista_new,0, pcb);  // ANTES: queue_push(cola_new, pcb);
                pthread_mutex_unlock(&mutex_new);

                log_warning(logger, "## (%d) Memoria rechazó el encolar en READY. Se mantiene en NEW", pcb->pid);
                sleep(5); // Para evitar bucles de CPU alta
            }

            liberar_conexion(socket_memoria);
            free(proceso->arch);
            free(proceso);
        }
    }
    return NULL;
}
