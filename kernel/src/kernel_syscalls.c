// kernel_syscalls.c
#include "kernel_syscalls.h"
#include <planificador.h>
#include <threads_kernel.h>
#include <pthread.h>
#include <commons/collections/list.h>
#include <commons/collections/dictionary.h>
#include <commons/collections/queue.h>
#include <commons/log.h>
#include <string.h>
#include <stdlib.h>
#include <protocolo.h>
#include <sys/socket.h>
#include <op_code.h>
#include <socket_utils.h>
#include <semaphore.h>

extern t_log* logger;
extern int tiempo_suspension;
extern int puerto_memoria;
extern char* ip_memoria;

extern int next_pid;

extern t_list* procesos_en_ejecucion; 
extern t_dictionary* dispositivos_io;
extern pthread_mutex_t mutex_io;
extern pthread_mutex_t mutex_exit;
extern t_list* lista_global_procesos;
extern pthread_mutex_t mutex_ready;
extern t_list* lista_ready;
extern sem_t sem_procesos_ready;
extern sem_t sem_procesos_new;

void syscall_init_proc(char* path, int tamanio) {
    log_info(logger, "## (%d) - Solicitó syscall: INIT_PROC", next_pid);
    int pid = next_pid++;
    crear_proceso(pid, tamanio, path);
}

void syscall_exit(t_pcb* pcb) {
    log_info(logger, "## (%d) - Solicitó syscall: EXIT", pcb->pid);
    
    // 1. El PCB debe estar en EXEC cuando llega aca
    if (pcb->estado != ESTADO_EXEC) {
        log_warning(logger, "## (%d) PCB vino con estado %d, debería ser EXEC (3)", pcb->pid, pcb->estado);
        pcb->estado = ESTADO_EXEC; // Corregir el estado
    }
    
    // 2. Usar cambiar_estado_con_metricas para pasar a EXIT
    cambiar_estado_con_metricas(pcb, ESTADO_EXIT);
    log_info(logger, "## (%d) - Pasa del estado EXEC al estado EXIT", pcb->pid);
    
    // 3. Mostrar métricas finales
    loggear_metricas_estado(pcb);
    
    // 4. Comunicación con memoria
    int socket_memoria = crear_conexion(ip_memoria, puerto_memoria);
    
    t_paquete* paquete = malloc(sizeof(t_paquete));
    paquete->codigo_operacion = FINALIZAR_PROCESO;
    paquete->buffer = malloc(sizeof(t_buffer));
    paquete->buffer->size = sizeof(int);
    paquete->buffer->stream = malloc(sizeof(int));
    memcpy(paquete->buffer->stream, &(pcb->pid), sizeof(int));

    void* stream = serializar_paquete(paquete, paquete->buffer->size + 2 * sizeof(int));
    send(socket_memoria, stream, paquete->buffer->size + 2 * sizeof(int), 0);

    int respuesta = recibir_operacion(socket_memoria, logger);
    if (respuesta == RESPUESTA_OK) {
        log_info(logger, "[MEMORIA → KERNEL] Confirmación de finalización PID=%d", pcb->pid);
        log_info(logger, "## (%d) - Finaliza el proceso", pcb->pid);
        
        sem_post(&sem_procesos_new); // avisar al PLP que hay espacio
    } else {
        log_error(logger, "Error al finalizar PID=%d en Memoria", pcb->pid);
    }

    liberar_conexion(socket_memoria);
    free(stream);
    eliminar_paquete(paquete);
}

void syscall_dump_memory(t_pcb* pcb) {
    log_info(logger, "## (%d) - Solicitó syscall: DUMP_MEMORY", pcb->pid);
    cambiar_estado_con_metricas(pcb, ESTADO_BLOCKED);
    log_info(logger, "## (%d) Pasa del estado EXEC al estado BLOCKED", pcb->pid);

    int socket_memoria = crear_conexion(ip_memoria, puerto_memoria);

    t_paquete* paquete = malloc(sizeof(t_paquete));
    paquete->codigo_operacion = DUMP_MEMORY;
    paquete->buffer = malloc(sizeof(t_buffer));
    paquete->buffer->size = sizeof(int);
    paquete->buffer->stream = malloc(sizeof(int));
    memcpy(paquete->buffer->stream, &(pcb->pid), sizeof(int));

    void* stream = serializar_paquete(paquete, paquete->buffer->size + 2 * sizeof(int));
    send(socket_memoria, stream, paquete->buffer->size + 2 * sizeof(int), 0);
    log_info(logger, "[KERNEL → MEMORIA] DUMP_MEMORY PID=%d", pcb->pid);

    free(stream);
    eliminar_paquete(paquete);

    int cod_op = recibir_operacion(socket_memoria, logger);
    if(cod_op == DUMP_FINALIZADO_OK){
        
        cambiar_estado_con_metricas(pcb, ESTADO_READY);
        log_info(logger, "## (%d) Pasa del estado BLOCKED al estado READY", pcb->pid);

        pthread_mutex_lock(&mutex_ready);
        list_add(lista_ready, pcb);
        pthread_mutex_unlock(&mutex_ready);
        sem_post(&sem_procesos_ready);

        log_info(logger, "## (%d) Dump exitoso, proceso desbloqueado y en READY", pcb->pid);
    } if (cod_op == DUMP_ERROR){
        finalizar_proceso(pcb);
    }
    
} 

void syscall_io(t_instruc_io instruccion, t_log* logger, t_pcb* pcb) {
    log_info(logger, "## (%d) - Solicitó syscall: IO ", pcb->pid);
    log_info(logger, "Kernel: Recibí syscall IO - Dispositivo: %s, Tiempo: %d", instruccion.dispositivo, instruccion.tiempo);
    
    pthread_mutex_lock(&mutex_io);
    t_dispositivo_io* disp = dictionary_get(dispositivos_io, instruccion.dispositivo);
    
    if (disp == NULL || list_is_empty(disp->instancias)) {
        pthread_mutex_unlock(&mutex_io);
        log_error(logger, "Dispositivo %s no encontrado o sin instancias. PID %d → EXIT", instruccion.dispositivo, pcb->pid);
        finalizar_proceso(pcb);
        return;
    }
    
    pcb->nombre_dispositivo_io = strdup(instruccion.dispositivo);
    
    // Bloquear proceso
    cambiar_estado_con_metricas(pcb, ESTADO_BLOCKED);
    log_info(logger, "## (%d) Pasa del estado EXEC al estado BLOCKED", pcb->pid);
    
    pcb->tiempo_io_pendiente = instruccion.tiempo;
    queue_push(disp->cola_bloqueados, pcb);
    
    // Crear un int* con el PID para pasarlo al thread
    int* pid_ptr = malloc(sizeof(int));
    *pid_ptr = pcb->pid;
    
    pthread_create(&pcb->hilo_suspension, NULL, temporizador_suspension, (void*)pid_ptr);
    //pthread_detach(&pcb->hilo_suspension);
    
    log_info(logger, "## (%d) - Bloqueado por IO: %s", pcb->pid, instruccion.dispositivo);
    
    // Buscar instancia libre
    for (int i = 0; i < list_size(disp->instancias); i++) {
        t_instancia_io* instancia = list_get(disp->instancias, i);
        if (!instancia->ocupado && !queue_is_empty(disp->cola_bloqueados)) {
            t_pcb* siguiente = queue_pop(disp->cola_bloqueados);
            t_peticion_io peticion;
            peticion.pid = siguiente->pid;
            peticion.tiempo_io = siguiente->tiempo_io_pendiente;
            
            enviar_peticion_IO(&peticion, instancia->socket);
            instancia->ocupado = true;
            instancia->pid_actual = siguiente->pid;
            
            log_info(logger, "Kernel → IO %s (instancia %d): PID %d ejecutando IO (%d ms)", 
                    instruccion.dispositivo, i, peticion.pid, peticion.tiempo_io);
        }
    }
    
    pthread_mutex_unlock(&mutex_io);
}

