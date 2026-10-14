#include "threads_kernel.h"
#include <socket_utils.h>
#include <stdlib.h>
#include <op_code.h>
#include <protocolo.h>
#include <kernel_syscalls.h>
#include <string.h>
#include <planificador.h>
#include <commons/collections/dictionary.h>
#include <pthread.h>
#include <protocolo.h>
#include <semaphore.h>

extern t_log* logger;

// variable global 
extern t_list* lista_cpus;
t_dictionary* dispositivos_io = NULL; // clave: nombre, valor: t_dispositivo_io*
pthread_mutex_t mutex_io = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_cpus = PTHREAD_MUTEX_INITIALIZER;
extern pthread_mutex_t mutex_ready;
extern pthread_mutex_t mutex_susp_ready;
extern pthread_mutex_t mutex_susp_blocked;
extern pthread_mutex_t mutex_exit;
extern sem_t sem_procesos_ready;
extern sem_t sem_procesos_susp_ready;
extern sem_t sem_desalojo;
extern t_list* cola_new;
extern t_list* lista_ready;
extern t_queue* cola_susp_ready;
extern t_queue* cola_susp_blocked;
extern char* ingreso_ready;
extern char* algoritmo_planificacion;
extern float alfa;
extern int estimacion_inicial;
extern sem_t sem_cpu_libre;


extern pthread_mutex_t mutex_desalojos_esperados;
extern t_list* desalojos_esperados;


void iniciar_hilos_escucha(int puerto_io, int puerto_dispatch, int puerto_interrupt){
    pthread_t hilo_io, hilo_dispatch, hilo_interrupt;

    int* arg_io = malloc(sizeof(int));
    int* arg_dispatch = malloc(sizeof(int));
    int* arg_interrupt = malloc(sizeof(int));

    *arg_io = puerto_io;
    *arg_dispatch = puerto_dispatch;
    *arg_interrupt = puerto_interrupt;

    pthread_create(&hilo_io, NULL, thread_escucha_io, arg_io);
    pthread_create(&hilo_dispatch, NULL, thread_escucha_dispatch, arg_dispatch);
    pthread_create(&hilo_interrupt, NULL, thread_escucha_interrupt, arg_interrupt);
}

// ----------- Dispatch -----------

void* thread_escucha_dispatch(void* arg){
    int puerto = *(int*)arg;
    free(arg);
    int servidor = iniciar_servidor(puerto, logger);
    log_info(logger, "[DISPATCH] Escuchando en el puerto %d", puerto);

    while (1){
        int cliente = esperar_cliente(servidor, logger);
        log_info(logger, "[DISPATCH] Cliente conectado: socket %d", cliente);

        int* socket_cliente = malloc(sizeof(int));
        *socket_cliente = cliente;
        pthread_t hilo;
        pthread_create(&hilo, NULL, atender_cpu_dispatch, socket_cliente);
        pthread_detach(hilo);
    }
    return NULL;
}

void* atender_cpu_dispatch(void* arg){
    int socket_dispatch = *(int*)arg;
    free(arg);

    while (1){
        int cod_op = recibir_operacion(socket_dispatch, logger);
        if(cod_op == -1) break;

        switch (cod_op){
            case HANDSHAKE_CPU: {
                sem_post(&sem_cpu_libre);
                char* id = recibir_nombre(socket_dispatch, logger);
                log_info(logger, "[DISPATCH] Handshake CPU: %s", id);

                t_cpu_conectada* cpu = malloc(sizeof(t_cpu_conectada));
                cpu->id_cpu = strdup(id);
                cpu->socket_dispatch = socket_dispatch;
                cpu->socket_interrupt = -1;
                cpu->libre = true;

                pthread_mutex_lock(&mutex_cpus);
                list_add(lista_cpus, cpu);
                pthread_mutex_unlock(&mutex_cpus);
                free(id);
                break;
            }

            case DEVOLVER_CONTEXTO: {
                t_pcb* pcb_recibida = recibir_pcb(socket_dispatch, logger);
                t_ejecucion* ejec = obtener_ejecucion_por_pid(pcb_recibida->pid);

                if (ejec == NULL) {
                    log_warning(logger, "[DISPATCH] No encontré la ejecución del PID=%d", pcb_recibida->pid);
                    break;
                }

                t_pcb* pcb_local = buscar_pcb_por_pid(pcb_recibida->pid);
                pcb_local->program_counter = pcb_recibida->program_counter;
                actualizar_pcb_en_lista_global(pcb_local);
                free(pcb_recibida);
                break;
            }

            case DESALOJO:{
                t_ejecucion* ejec = obtener_ejecucion_por_socket_dispatch(socket_dispatch);
                
                if(ejec == NULL){
                    log_error(logger, "[DISPATCH] No hay PCB para desalojar");
                    break;
                }

                t_pcb* pcb_en_ejecucion = buscar_pcb_por_pid(ejec->pcb->pid);


                // MARCAR DESALOJO COMO COMPLETADO ANTES DE PROCESAR
                marcar_desalojo_completado(pcb_en_ejecucion->pid);

                finalizar_ejecucion(pcb_en_ejecucion->pid);
                marcar_cpu_libre_por_socket(socket_dispatch);
                sem_post(&sem_cpu_libre);
                sem_post(&sem_desalojo);

                // Calcular cuánto ejecutó
                int tiempo_fin = timestamp_milisegundos();
                int tiempo_ejecutado = tiempo_fin - pcb_en_ejecucion->tiempo_inicio_exec;

                // Actualizar estimación con lo que le falta por ejecutar
                pcb_en_ejecucion->estimacion_rafaga -= tiempo_ejecutado;

                // Por si acaso, que no quede en negativo
                if (pcb_en_ejecucion->estimacion_rafaga < 0)
                    pcb_en_ejecucion->estimacion_rafaga = 0;

                log_info(logger, "## (%d) - Tiempo ejecutado hasta desalojo: %d ms | Estimación restante: %.2f ms", 
                        pcb_en_ejecucion->pid, tiempo_ejecutado, pcb_en_ejecucion->estimacion_rafaga);

                log_info(logger, "## (%d) - Desalojo por algoritmo SJF/SRT", pcb_en_ejecucion->pid);
                cambiar_estado_con_metricas(pcb_en_ejecucion, ESTADO_READY);
                log_info(logger, "## (%d) pasa del estado EXEC al estado READY", pcb_en_ejecucion->pid);

                pthread_mutex_lock(&mutex_ready);
                list_add(lista_ready, pcb_en_ejecucion);
                pthread_mutex_unlock(&mutex_ready);
                sem_post(&sem_procesos_ready);
                
                break;
            }
            
            case IO: {
                usleep(1000);
                t_ejecucion* ejec = obtener_ejecucion_por_socket_dispatch(socket_dispatch);
                
                if(ejec == NULL){
                    log_error(logger, "[DISPATCH] Se recibió IO pero no hay PCB devuelta");
                    break;
                }

                t_pcb* pcb_en_ejecucion = buscar_pcb_por_pid(ejec->pcb->pid);

                // IMPORTANTE: Marcar desalojo como completado si había uno esperado
                // Esto previene el deadlock cuando IO llega después de interrupción
                marcar_desalojo_completado(pcb_en_ejecucion->pid);

                t_instruc_io inst = recibir_instruct_io(socket_dispatch);

                finalizar_ejecucion(pcb_en_ejecucion->pid);
                marcar_cpu_libre_por_socket(socket_dispatch);
                sem_post(&sem_cpu_libre);

                syscall_io(inst, logger, pcb_en_ejecucion);
                break;
            }

            case EXIT: {
                usleep(1000);
                t_ejecucion* ejec = obtener_ejecucion_por_socket_dispatch(socket_dispatch);

                if(ejec == NULL){
                    log_error(logger, "[DISPATCH] Se recibió EXIT pero no hay PCB en ejecucion");
                    break;
                }
                
                t_pcb* pcb_en_ejecucion = buscar_pcb_por_pid(ejec->pcb->pid);

                // IMPORTANTE: Marcar desalojo como completado si había uno esperado
                marcar_desalojo_completado(pcb_en_ejecucion->pid);

                finalizar_ejecucion(pcb_en_ejecucion->pid);  
                marcar_cpu_libre_por_socket(socket_dispatch);
                sem_post(&sem_cpu_libre);
                syscall_exit(pcb_en_ejecucion);
                break;
            }
            
            case DUMP_MEMORY: {
                usleep(1000);
                t_ejecucion* ejec = obtener_ejecucion_por_socket_dispatch(socket_dispatch);

                if(ejec == NULL){
                    log_error(logger, "[DISPATCH] Se recibió DUMP MEMORY pero no hay PCB en ejecucion");
                    break;
                }
                
                t_pcb* pcb_en_ejecucion = buscar_pcb_por_pid(ejec->pcb->pid);

                // IMPORTANTE: Marcar desalojo como completado si había uno esperado
                marcar_desalojo_completado(pcb_en_ejecucion->pid);

                finalizar_ejecucion(pcb_en_ejecucion->pid);
                marcar_cpu_libre_por_socket(socket_dispatch);
                sem_post(&sem_cpu_libre);

                syscall_dump_memory(pcb_en_ejecucion);
                calcular_y_actualizar_rafaga(pcb_en_ejecucion);
                break;
            }
            case INIT_PROC:{
                t_instruc_init_proc init_proces = recibir_instruc_init_proc(socket_dispatch);
                syscall_init_proc(init_proces.archivo_pseudo, init_proces.tamanio);
                free(init_proces.archivo_pseudo);
                break;
            }
        }
    }
    return NULL;
}

// ----------- Interrupt -----------

void* thread_escucha_interrupt(void* arg){
    int puerto = *(int*)arg;
    free(arg);
    int servidor = iniciar_servidor(puerto, logger);
    log_info(logger, "[INTERRUPT] Escuchando en puerto %d", puerto);

    while (1){
        int cliente = esperar_cliente(servidor, logger);
        log_info(logger, "[INTERRUPT] Cliente conectado: socket %d", cliente);

        int* socket_cliente = malloc(sizeof(int));
        *socket_cliente = cliente;
        pthread_t hilo;
        pthread_create(&hilo, NULL, atender_cpu_interrupt, socket_cliente);
        pthread_detach(hilo);
    }
    return NULL;
}

void* atender_cpu_interrupt(void* arg){
    int socket_interrupt = *(int*)arg;
    free(arg);

    int cod_op = recibir_operacion(socket_interrupt, logger);
    if (cod_op == HANDSHAKE_CPU){
        char* id = recibir_nombre(socket_interrupt, logger);
        log_info(logger, "[INTERRUPT] Handshake CPU: %s", id);

        pthread_mutex_lock(&mutex_cpus);
        for (int i = 0; i < list_size(lista_cpus); i++){
            t_cpu_conectada* cpu = list_get(lista_cpus, i);
            if (strcmp(cpu->id_cpu, id) == 0){
                cpu->socket_interrupt = socket_interrupt;
                break;
            }
        }
        pthread_mutex_unlock(&mutex_cpus);
        free(id);
    }
    return NULL;
}

// ----------- IO -----------

void* thread_escucha_io(void* arg){
    int puerto = *(int*)arg;
    free(arg);
    int servidor = iniciar_servidor(puerto, logger);
    log_info(logger, "[IO] Escuchando en puerto %d", puerto);

    while (1){
        int cliente = esperar_cliente(servidor, logger);
        log_info(logger, "[IO] Cliente IO conectado: socket %d", cliente);

        int* socket_cliente = malloc(sizeof(int));
        *socket_cliente = cliente;
        pthread_t hilo;
        pthread_create(&hilo, NULL, atender_dispositivo_io, socket_cliente);
        pthread_detach(hilo);
    }
    return NULL;
}

void* atender_dispositivo_io(void* arg){
    int cliente = *(int*)arg;
    free(arg);
  
    while (1){
        int cod_op = recibir_operacion(cliente, logger);

        switch (cod_op){
            case HANDSHAKE_IO: {
                char* nombre = recibir_nombre(cliente, logger);
                pthread_mutex_lock(&mutex_io);

                t_dispositivo_io* disp = dictionary_get(dispositivos_io, nombre);
                if (disp == NULL) {
                    disp = malloc(sizeof(t_dispositivo_io));
                    disp->cola_bloqueados = queue_create();
                    disp->instancias = list_create();
                    dictionary_put(dispositivos_io, nombre, disp);
                }

                t_instancia_io* nueva = malloc(sizeof(t_instancia_io));
                nueva->socket = cliente;
                nueva->ocupado = false;

                list_add(disp->instancias, nueva);

                pthread_mutex_unlock(&mutex_io);

                log_info(logger, "[IO] Dispositivo registrado: %s", nombre);
                free(nombre);
                break;
            }
            case FIN_IO: {
                t_fin_io datos = recibir_fin_io(cliente);
                log_info(logger, "[IO] Fin de IO para PID=%d en dispositivo %s", datos.pid, datos.dispositivo);

                pthread_mutex_lock(&mutex_io);
                t_dispositivo_io* disp = dictionary_get(dispositivos_io, datos.dispositivo);
                if (!disp) {
                    pthread_mutex_unlock(&mutex_io);
                    log_error(logger, "[IO] Dispositivo %s no encontrado", datos.dispositivo);
                    break;
                }

                // Buscar la instancia que envió el FIN_IO
                t_instancia_io* instancia = NULL;
                for (int i = 0; i < list_size(disp->instancias); i++) {
                    t_instancia_io* actual = list_get(disp->instancias, i);
                    if (actual->socket == cliente) {
                        instancia = actual;
                        break;
                    }
                }

                if (!instancia) {
                    pthread_mutex_unlock(&mutex_io);
                    log_error(logger, "[IO] No se encontro la instancia correspondiente al socket");
                    break;
                }

                instancia->ocupado = false;
                instancia->pid_actual=-1;
                log_info(logger, "[IO] Instancia de %s liberada", datos.dispositivo);

                if (!queue_is_empty(disp->cola_bloqueados)) {
                    t_pcb* siguiente = queue_pop(disp->cola_bloqueados);

                    t_peticion_io peticion;
                    peticion.pid = siguiente->pid;
                    peticion.tiempo_io = siguiente->tiempo_io_pendiente;

                    enviar_peticion_IO(&peticion, instancia->socket);
                    instancia->ocupado = true;
                    instancia->pid_actual = siguiente->pid;

                    log_info(logger, "[IO %s] Instancia atendiendo a PID=%d (%d ms)", datos.dispositivo, peticion.pid, peticion.tiempo_io);
                }

                pthread_mutex_unlock(&mutex_io);

                // Estado post-IO
                t_pcb* pcb = buscar_pcb_por_pid(datos.pid);
                if (!pcb) {
                    log_error(logger, "PCB con PID=%d no encontrada", datos.pid);
                    free(datos.dispositivo);
                    break;
                }

                pcb->tiempo_io_pendiente = 0;

                calcular_y_actualizar_rafaga(pcb);

                if (pcb->estado == ESTADO_BLOCKED) {
                    
                    log_info(logger,"[LOGGER NUESTRO] - Se cancelo el hilo de suspension del pid %d", pcb->pid);

                    if (pcb->hilo_suspension != 0) {
                pthread_cancel(pcb->hilo_suspension);
                pthread_join(pcb->hilo_suspension, NULL);
                pcb->hilo_suspension = 0;  // Marcar como cancelado
                    }
                    
                    cambiar_estado_con_metricas(pcb,ESTADO_READY);
                    log_info(logger, "## (%d) Pasa del estado BLOCKED al estado READY", pcb->pid);

                    actualizar_pcb_en_lista_global(pcb);

                    pthread_mutex_lock(&mutex_ready);
                    list_add(lista_ready, pcb);
                    pthread_mutex_unlock(&mutex_ready);
                    sem_post(&sem_procesos_ready);

                    log_info(logger, "## (%d) finalizo IO y pasa a READY", pcb->pid);
                } else if (pcb->estado == ESTADO_SUSP_BLOCKED) {
                    //manejar_fin_io(pcb->pid);
                    
                    cambiar_estado_con_metricas(pcb, ESTADO_SUSP_READY);
                    log_info(logger, "## (%d) Pasa del estado SUSP. BLOCKED al estado SUSP. READY", pcb->pid);

                    pthread_mutex_lock(&mutex_susp_ready);
                    queue_push(cola_susp_ready, pcb);
                    pthread_mutex_unlock(&mutex_susp_ready);
                    sem_post(&sem_procesos_susp_ready);
                }

                free(datos.dispositivo);
                break;
            }
            case -1: {
                log_error(logger, "[IO] Se detecto desconexion del socket %d", cliente);
                pthread_mutex_lock(&mutex_io);

                // recorremos todos los dispositivos para encontrar a que instancia pertenece
                bool encontrada = false;

                void eliminar_instancia_y_manejar_procesos(char* nombre_disp, void* valor_disp) { // esto lo declaro aca por se usa solamente para esto 
                    if (encontrada) return;

                    t_dispositivo_io* disp = (t_dispositivo_io*) valor_disp;

                    for (int i = 0; i < list_size(disp->instancias); i++) {
                        t_instancia_io* instancia = list_get(disp->instancias, i);
                        if (instancia->socket == cliente) {
                            // eliminar la instancia
                            list_remove(disp->instancias, i);
                            encontrada = true;

                            log_warning(logger, "[IO] Instancia desconectada (socket %d) eliminada del dispositivo %s", cliente, nombre_disp);

                            // si esta instancia estaba ejecutando IO para algun proceso, tambien finaliza todo
                            if (instancia->ocupado && instancia->pid_actual != -1) {
                                int pid_ejecutando = instancia->pid_actual;
                                t_pcb* pcb = buscar_pcb_por_pid(pid_ejecutando);
                                if (pcb) {
                                    log_warning(logger, "[IO] Instancia %s ejecutaba IO para PID=%d finalizando proceso...", nombre_disp, pid_ejecutando);

                                    if (pcb->estado == ESTADO_BLOCKED || pcb->estado == ESTADO_SUSP_BLOCKED) {
                                        log_info(logger, "[DEBUG] Cancelando hilo de suspensión de PID=%d", pcb->pid);
                                        pthread_cancel(pcb->hilo_suspension);
                                        pthread_join(pcb->hilo_suspension, NULL);
                                        log_info(logger, "[DEBUG] Hilo suspendido join finalizado PID=%d", pcb->pid);
                                    }

                                    finalizar_proceso(pcb);
                                } else {
                                    log_warning(logger, "[IO] No se encontro PCB para PID=%d ejecutando en %s", pid_ejecutando, nombre_disp);
                                }
                            }

                            free(instancia);

                            // Si no quedan mas instancias ,finalizo los procesos
                            if (list_is_empty(disp->instancias)) {
                            log_warning(logger, "[IO] No quedan instancias activas de %s finalizando procesos bloqueados...", nombre_disp);
                            finalizar_todos_los_procesos_bloqueados_por(nombre_disp);
                            }


                            break;
                        }
                    }
                }

                dictionary_iterator(dispositivos_io, eliminar_instancia_y_manejar_procesos);

                if (!encontrada) {
                    log_warning(logger, "[IO] Instancia desconectada (socket %d) no encontrada en ninguna lista", cliente);
                }

                pthread_mutex_unlock(&mutex_io);
                return NULL;
            }
        }
    }
    return NULL;
}