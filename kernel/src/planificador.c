#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include <commons/collections/queue.h>
#include <commons/collections/dictionary.h>
#include <commons/log.h>
#include <commons/collections/list.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <stdbool.h>
#include <threads_kernel.h>
#include "planificador.h"
#include <protocolo.h>
#include <socket_utils.h>
#include <op_code.h>
#include <planificador_largo_plazo.h>
#include <planificador_corto_plazo.h>

extern t_log* logger;
extern int tiempo_suspension;
extern int puerto_memoria;
extern char* ip_memoria;
extern t_list* lista_cpus;
extern t_dictionary* dispositivos_io;
extern char* ingreso_ready;
extern char* algoritmo_planificacion;
extern float alfa;
extern int estimacion_inicial;

// Colas
t_list* lista_new;
t_list* lista_ready;
t_queue* cola_susp_ready;
t_queue* cola_susp_blocked;

t_list* lista_global_procesos;
extern t_list* procesos_en_ejecucion; // clave: string con el pid


// Mutex y Condition Variables
pthread_mutex_t mutex_new = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_ready = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_susp_ready = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_susp_blocked = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t mutex_procesos = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t mutex_plp = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond_plp = PTHREAD_COND_INITIALIZER;
pthread_cond_t cond_desalojos = PTHREAD_COND_INITIALIZER;

pthread_mutex_t mutex_desalojos_esperados = PTHREAD_MUTEX_INITIALIZER;
extern t_list* desalojos_esperados;

void notificar_plp() {
    pthread_mutex_lock(&mutex_plp);
    pthread_cond_signal(&cond_plp);
    pthread_mutex_unlock(&mutex_plp);
}

// Semáforos
sem_t sem_procesos_new;
sem_t sem_procesos_ready;
sem_t sem_procesos_susp_ready;
sem_t sem_desalojo;
sem_t sem_cpu_libre;

extern pthread_mutex_t mutex_cpus;

int next_pid = 0;


void finalizar_proceso(t_pcb* pcb) {
     if (pcb == NULL) return;
    
    log_info(logger, "## (%d) Finalizando proceso...", pcb->pid);

    // Actualizar tiempo final del estado actual
    actualizar_tiempo_estado(pcb, pcb->estado);
    
    // Cambiar a EXIT

    cambiar_estado_con_metricas(pcb,ESTADO_EXIT);
    
    // Mostrar métricas finales
    loggear_metricas_estado(pcb);
    
    // Continuar con la lógica original de finalización...
    // (comunicación con memoria, etc.)
    
    // 1. Avisar a memoria
    int socket_mem = crear_conexion(ip_memoria, puerto_memoria);
    if (socket_mem == -1) {
        log_error(logger, "No se pudo conectar con Memoria para finalizar PID=%d", pcb->pid);
        return;
    }

    t_paquete* paquete = malloc(sizeof(t_paquete));
    paquete->codigo_operacion = FINALIZAR_PROCESO;
    paquete->buffer = malloc(sizeof(t_buffer));
    paquete->buffer->size = sizeof(int);
    paquete->buffer->stream = malloc(sizeof(int));
    memcpy(paquete->buffer->stream, &(pcb->pid), sizeof(int));

    void* stream = serializar_paquete(paquete, paquete->buffer->size + 2 * sizeof(int));
    send(socket_mem, stream, paquete->buffer->size + 2 * sizeof(int), 0);


    // 2. Esperar respuesta de Memoria
    int cod_op = recibir_operacion(socket_mem, logger);
    if (cod_op != RESPUESTA_OK) {
        log_error(logger, "Memoria falló al liberar PID=%d", pcb->pid);
        liberar_conexion(socket_mem);
        eliminar_paquete(paquete);
        free(stream);
        return;
    }

    log_info(logger, "## (%d) - Finaliza el proceso", pcb->pid);

    liberar_conexion(socket_mem);
    eliminar_paquete(paquete);
    free(stream);

    // 3. Llamar al planificador largo plazo
    sem_post(&sem_procesos_new);
    notificar_plp();
}

void manejar_fin_io(int pid_finalizado) {

    pthread_mutex_lock(&mutex_susp_blocked);
    t_queue* temporal = queue_create();
    t_pcb* pcb_encontrado = NULL;

    while (!queue_is_empty(cola_susp_blocked)) {
        t_pcb* pcb = queue_pop(cola_susp_blocked);
        if (pcb->pid == pid_finalizado && !pcb_encontrado) {
            pcb_encontrado = pcb;
        } else {
            queue_push(temporal, pcb);
        }
    }
    while (!queue_is_empty(temporal)) {
        queue_push(cola_susp_blocked, queue_pop(temporal));
    }
    queue_destroy(temporal);
    pthread_mutex_unlock(&mutex_susp_blocked);

    if (pcb_encontrado) {
        
        cambiar_estado_con_metricas(pcb_encontrado, ESTADO_SUSP_READY);


        pthread_mutex_lock(&mutex_susp_ready);
        queue_push(cola_susp_ready, pcb_encontrado);
        pthread_mutex_unlock(&mutex_susp_ready);
        sem_post(&sem_procesos_susp_ready);

        log_info(logger, "## (%d) finalizó IO y pasa a SUSP_READY", pcb_encontrado->pid);
        notificar_plp();
    } else {
        log_error(logger, "[KERNEL] No se encontró el proceso PID=%d en SUSP_BLOCKED", pid_finalizado);
    }
}



void iniciar_planificadores() {
    lista_new = list_create();
    lista_ready = list_create();
    cola_susp_ready = queue_create();
    cola_susp_blocked = queue_create();

    sem_init(&sem_procesos_new, 0, 0);
    sem_init(&sem_procesos_ready, 0, 0);
    sem_init(&sem_procesos_susp_ready, 0, 0);
    sem_init(&sem_cpu_libre, 0, 0);

    pthread_t hilo_plp, hilo_pcp;
    pthread_create(&hilo_plp, NULL, planificador_largo_plazo, NULL);
    pthread_detach(hilo_plp);
    
    pthread_create(&hilo_pcp, NULL, planificador_corto_plazo, NULL);
    pthread_detach(hilo_pcp);
}

void crear_proceso(int pid, int tamanio, char* ruta_archivo) {

    t_pcb* pcb = malloc(sizeof(t_pcb));
    pcb->pid = pid;
    pcb->tamanio = tamanio;
    pcb->program_counter = 0;
    pcb->estimacion_rafaga = estimacion_inicial;
    pcb->tiempo_inicio_exec = 0;
    pcb->path = strdup(ruta_archivo); 

    inicializar_metricas_pcb(pcb);

    cambiar_estado_con_metricas(pcb,ESTADO_NEW);
    log_info(logger, "## (%d) Se crea el proceso - Estado: NEW", pid);
    
    pthread_mutex_lock(&mutex_new);
    list_add(lista_new, pcb);
    pthread_mutex_unlock(&mutex_new);

    pthread_mutex_lock(&mutex_procesos);
    list_add(lista_global_procesos, pcb);
    pthread_mutex_unlock(&mutex_procesos);


    sem_post(&sem_procesos_new);
    notificar_plp();

}

void crear_proceso_con_pseudocodigo(char* path, int tamanio) {
    int pid = next_pid++;
    crear_proceso(pid, tamanio, path);
}

void* serializar_pcb(t_pcb* pcb, int* size) {
    int tam = sizeof(int) * 3 + sizeof(estado_proceso_t);
    void* buffer = malloc(tam);
    int offset = 0;

    memcpy(buffer + offset, &(pcb->pid), sizeof(int)); 
    offset += sizeof(int);
    memcpy(buffer + offset, &(pcb->tamanio), sizeof(int)); 
    offset += sizeof(int);
    memcpy(buffer + offset, &(pcb->program_counter), sizeof(int)); 
    offset += sizeof(int);
    memcpy(buffer + offset, &(pcb->estado), sizeof(estado_proceso_t));

    *size = tam;
    return buffer;
}

t_pcb deserializar_pcb(void* buffer) {
    t_pcb pcb;
    int offset = 0;

    memcpy(&(pcb.pid), buffer + offset, sizeof(int)); offset += sizeof(int);
    memcpy(&(pcb.tamanio), buffer + offset, sizeof(int)); offset += sizeof(int);
    memcpy(&(pcb.program_counter), buffer + offset, sizeof(int)); offset += sizeof(int);
    memcpy(&(pcb.estado), buffer + offset, sizeof(estado_proceso_t));

    return pcb;
}

t_pcb* recibir_pcb(int socket_cpu, t_log* logger) {
    int size_buffer;
    void* buffer = recibir_buffer(&size_buffer, socket_cpu);
    if (!buffer) return NULL;

    t_pcb* pcb = malloc(sizeof(t_pcb));
    int offset = 0;
    memcpy(&(pcb->pid), buffer + offset, sizeof(int)); offset += sizeof(int);
    memcpy(&(pcb->tamanio), buffer + offset, sizeof(int)); offset += sizeof(int);
    memcpy(&(pcb->program_counter), buffer + offset, sizeof(int)); offset += sizeof(int);
    memcpy(&(pcb->estado), buffer + offset, sizeof(estado_proceso_t));

    log_info(logger, "Se recibió PCB PID=%d, TAM=%d, PC=%d, ESTADO=%d", pcb->pid, pcb->tamanio, pcb->program_counter, pcb->estado);

    free(buffer);
    return pcb;
}

void enviar_pcb(t_pcb* pcb, int socket, t_log* logger) {
    int size;
    void* buffer = serializar_pcb(pcb, &size);

    t_paquete* paquete = malloc(sizeof(t_paquete));
    paquete->codigo_operacion = EJECUTAR_CONTEXTO;
    paquete->buffer = malloc(sizeof(t_buffer));
    paquete->buffer->size = size;
    paquete->buffer->stream = buffer;

    void* a_enviar = serializar_paquete(paquete, size + 2 * sizeof(int));
    send(socket, a_enviar, size + 2 * sizeof(int), 0);

    log_info(logger, "KERNEL → CPU: Enviado PCB PID=%d, Estado = %d", pcb->pid, pcb->estado);

    free(a_enviar);
    eliminar_paquete(paquete);
}

t_pcb* buscar_pcb_por_pid(int pid_buscado) {
    pthread_mutex_lock(&mutex_procesos);
    for (int i = 0; i < list_size(lista_global_procesos); i++) {
        t_pcb* pcb = list_get(lista_global_procesos, i);
        if (pcb->pid == pid_buscado) {
            pthread_mutex_unlock(&mutex_procesos);
            return pcb;
        }
    }
    pthread_mutex_unlock(&mutex_procesos);
    log_info(logger, "No encontre PCB");
    return NULL; // no encontrado
}

int timestamp_milisegundos(){
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)(tv.tv_sec * 1000 + tv.tv_usec / 1000);
}

bool comparar_por_tamanio(void* a, void* b) {
    t_pcb* pcb_a = (t_pcb*)a;
    t_pcb* pcb_b = (t_pcb*)b;
    return pcb_a->tamanio < pcb_b->tamanio;
}

bool comparar_estimaciones(void* a, void* b) {
    t_pcb* pcb_a = (t_pcb*)a;
    t_pcb* pcb_b = (t_pcb*)b;
    return pcb_a->estimacion_rafaga < pcb_b->estimacion_rafaga;
}

void* temporizador_suspension(void* arg) {
    // 1. COPIAR EL PID ANTES DE LIBERAR ARG
    int* pid_ptr = (int*)arg;
    int pid = *pid_ptr;
    free(arg);  // Ahora es seguro liberar
    
    log_info(logger, "Tengo sueño y me voy a mimir %d", tiempo_suspension);
    
    int64_t TIEMPO_REAL_IO = (tiempo_suspension / 1000);
    sleep(TIEMPO_REAL_IO);
    
    // 2. BUSCAR PCB CON PROTECCIÓN MUTEX
    pthread_mutex_lock(&mutex_procesos);
    t_pcb* pcb = NULL;
    
    // Buscar el PCB en la lista global
    for (int i = 0; i < list_size(lista_global_procesos); i++) {
        t_pcb* pcb_actual = list_get(lista_global_procesos, i);
        if (pcb_actual->pid == pid) {
            pcb = pcb_actual;
            break;
        }
    }
    
    // 3. VERIFICAR SI EL PCB AÚN EXISTE Y ESTÁ EN BLOCKED
    if (pcb == NULL) {
        pthread_mutex_unlock(&mutex_procesos);
        log_info(logger, "## (%d) PCB ya no existe, no se puede suspender", pid);
        return NULL;
    }
    
    // 4. VERIFICAR ESTADO CON MUTEX
    if (pcb->estado != ESTADO_BLOCKED) {
        pthread_mutex_unlock(&mutex_procesos);
        log_info(logger, "## (%d) No se suspende porque ya cambió de estado (actual: %d)", 
                 pid, pcb->estado);
        return NULL;
    }
    
    // 5. CAMBIAR ESTADO Y MOVER A COLA (TODO DENTRO DEL MUTEX)
    cambiar_estado_con_metricas(pcb, ESTADO_SUSP_BLOCKED);
    
    pthread_mutex_lock(&mutex_susp_blocked);
    queue_push(cola_susp_blocked, pcb);
    pthread_mutex_unlock(&mutex_susp_blocked);
    
    pthread_mutex_unlock(&mutex_procesos);
    
    log_info(logger, "## (%d) Pasa de BLOCKED a SUSP_BLOCKED", pid);
    
    // 6. AVISAR A MEMORIA
    avisar_suspension_a_memoria(pcb);
    
    // 7. AVISAR AL PLP
    sem_post(&sem_procesos_new);
    notificar_plp();
    
    return NULL;
}

void avisar_suspension_a_memoria(t_pcb* pcb) {
    int socket_memoria = crear_conexion(ip_memoria, puerto_memoria);

    t_paquete* paquete = malloc(sizeof(t_paquete));
    paquete->codigo_operacion = SUSPENDER_PROCESO;
    paquete->buffer = malloc(sizeof(t_buffer));
    paquete->buffer->size = sizeof(int);
    paquete->buffer->stream = malloc(sizeof(int));
    memcpy(paquete->buffer->stream, &(pcb->pid), sizeof(int));

    void* a_enviar = serializar_paquete(paquete, paquete->buffer->size + 2 * sizeof(int));
    send(socket_memoria, a_enviar, paquete->buffer->size + 2 * sizeof(int), 0);

    log_info(logger, "[KERNEL → MEMORIA] SUSPENDER_PROCESO PID=%d", pcb->pid);

    free(a_enviar);
    eliminar_paquete(paquete);
    liberar_conexion(socket_memoria);
}

void avisar_desuspension_a_memoria(t_pcb* pcb, int socket_memoria){
    t_paquete* paquete = malloc(sizeof(t_paquete));
    paquete->codigo_operacion = DESUSPENDER_PROCESO;
    paquete->buffer = malloc(sizeof(t_buffer));
    paquete->buffer->size = sizeof(int);
    paquete->buffer->stream = malloc(sizeof(int));
    memcpy(paquete->buffer->stream, &(pcb->pid), sizeof(int));

    void* a_enviar = serializar_paquete(paquete, paquete->buffer->size + 2 * sizeof(int));
    send(socket_memoria, a_enviar, paquete->buffer->size + 2 * sizeof(int), 0);

    log_info(logger, "[KERNEL → MEMORIA] DESUSPENDER_PROCESO PID=%d", pcb->pid);

    free(a_enviar);
    eliminar_paquete(paquete);
}

t_cpu_conectada* seleccionar_cpu_libre() {
    pthread_mutex_lock(&mutex_cpus);
    for (int i = 0; i < list_size(lista_cpus); i++) {
        t_cpu_conectada* cpu = list_get(lista_cpus, i);
        //log_info(logger, "CPU %s -> libre: %d, dispatch: %d, interrupt: %d", cpu->id_cpu, cpu->libre, cpu->socket_dispatch, cpu->socket_interrupt);
        if (cpu->libre && cpu->socket_dispatch != -1 && cpu->socket_interrupt != -1) {
            cpu->libre = false;
            pthread_mutex_unlock(&mutex_cpus);
            return cpu;
        }
    }
    pthread_mutex_unlock(&mutex_cpus);
    return NULL;
}


void marcar_cpu_libre(char* id_cpu) {
    pthread_mutex_lock(&mutex_cpus);
    for (int i = 0; i < list_size(lista_cpus); i++) {
        t_cpu_conectada* cpu = list_get(lista_cpus, i);
        if (strcmp(cpu->id_cpu, id_cpu) == 0) {
            cpu->libre = true;
            break;
        }
    }
    pthread_mutex_unlock(&mutex_cpus);
}

void marcar_cpu_libre_por_socket(int socket_dispatch) {
    pthread_mutex_lock(&mutex_cpus);
    for (int i = 0; i < list_size(lista_cpus); i++) {
        t_cpu_conectada* cpu = list_get(lista_cpus, i);
        if (cpu->socket_dispatch == socket_dispatch) {
            cpu->libre = true;
            break;
        }
    }
    pthread_mutex_unlock(&mutex_cpus);
}

void destruir_pcb(t_pcb* pcb) {
    if (pcb == NULL) return;

    if (pcb->path != NULL) {
        free(pcb->path);
    }

    free(pcb);
}

void actualizar_pcb_en_lista_global(t_pcb* pcb_actualizado) {
    for (int i = 0; i < list_size(lista_global_procesos); i++) {
        t_pcb* pcb_original = list_get(lista_global_procesos, i);

        if (pcb_original->pid == pcb_actualizado->pid) {
            // Actualizamos los campos uno por uno
            pcb_original->tamanio = pcb_actualizado->tamanio;
            pcb_original->program_counter = pcb_actualizado->program_counter;
            pcb_original->estado = pcb_actualizado->estado;

            for (int j = 0; j < 7; j++) {
                pcb_original->cantidad_veces_en_estado[j] = pcb_actualizado->cantidad_veces_en_estado[j];
                pcb_original->tiempo_en_milisegundos[j] = pcb_actualizado->tiempo_en_milisegundos[j];
            }

            pcb_original->estimacion_rafaga = pcb_actualizado->estimacion_rafaga;
            pcb_original->tiempo_inicio_exec = pcb_actualizado->tiempo_inicio_exec;
            pcb_original->tiempo_io_pendiente = pcb_actualizado->tiempo_io_pendiente;

            return; // Terminamos cuando lo actualizamos
        }
    }

    // Si llegamos acá, no se encontró el PCB
    log_warning(logger, "No se encontró el PCB con pid=%d para actualizarlo.", pcb_actualizado->pid);
}




// Función para loggear las métricas de estado de un proceso
void loggear_metricas_estado(t_pcb* pcb) {
    if (pcb == NULL) {
        log_error(logger, "PCB es NULL, no se pueden mostrar métricas");
        return;
    }

        // Actualizar tiempo del estado actual antes de mostrar métricas
    if (pcb->estado != ESTADO_EXIT) {
        actualizar_tiempo_estado(pcb, pcb->estado);
    }

    // Nombres de los estados para el log
    char* nombres_estados[] = {
        "NEW",           // 0
        "READY",         // 1
        "SUSP_READY",    // 2
        "EXEC",          // 3
        "BLOCKED",       // 4
        "SUSP_BLOCKED",  // 5
        "EXIT"           // 6
    };

    // Buffer para construir el mensaje
    char mensaje[1024];
    char temp[128];
    
    // Empezar el mensaje
    snprintf(mensaje, sizeof(mensaje), "## (%d) - Métricas de estado: ", pcb->pid);
    
    // Recorrer todos los estados y agregar solo los que tienen datos
    bool primero = true;
    for (int i = 0; i < 7; i++) {
        if (pcb->cantidad_veces_en_estado[i] > 0) {
            if (!primero) {
                strcat(mensaje, ", ");
            }
            
            snprintf(temp, sizeof(temp), "%s (%d) (%d)", 
                     nombres_estados[i], 
                     pcb->cantidad_veces_en_estado[i], 
                     pcb->tiempo_en_milisegundos[i]);
            
            strcat(mensaje, temp);
            primero = false;
        }
    }
    
    log_info(logger, "%s", mensaje);
}

// Función auxiliar para actualizar el tiempo en un estado cuando cambia
void actualizar_tiempo_estado(t_pcb* pcb, estado_proceso_t estado_actual) {
    if (pcb == NULL) return;
    
    int timestamp_actual = timestamp_milisegundos();
    
    // Si tenemos un timestamp de entrada para este estado, calculamos el tiempo transcurrido
    if (pcb->timestamp_entrada_estado[estado_actual] != 0) {
        int tiempo_transcurrido = timestamp_actual - pcb->timestamp_entrada_estado[estado_actual];
         
         if (tiempo_transcurrido > 0) {
            pcb->tiempo_en_milisegundos[estado_actual] += tiempo_transcurrido;
        }
        
        // Resetear el timestamp de entrada ya que terminó este período
        pcb->timestamp_entrada_estado[estado_actual] = 0;
        
        log_debug(logger, "## (%d) Tiempo en estado %d: +%d ms (total: %d ms)", 
                 pcb->pid, estado_actual, tiempo_transcurrido, 
                 pcb->tiempo_en_milisegundos[estado_actual]);
    }

        // Resetear el timestamp de entrada ya que terminó este período
        pcb->timestamp_entrada_estado[estado_actual] = 0;
    
    pcb->timestamp_ultima_actualizacion = timestamp_actual;
}

// Función para cambiar de estado y registrar métricas
void cambiar_estado_con_metricas(t_pcb* pcb, estado_proceso_t nuevo_estado) {
    if (pcb == NULL) return;
    
    estado_proceso_t estado_anterior = pcb->estado;
    int timestamp_actual = timestamp_milisegundos();
    
    // Actualizar tiempo en el estado anterior (finalizar el período)
        if (estado_anterior >= 0 && estado_anterior < 7) {
        actualizar_tiempo_estado(pcb, estado_anterior);
    }
    
    // Cambiar al nuevo estado
    pcb->estado = nuevo_estado;
    pcb->cantidad_veces_en_estado[nuevo_estado]++;
    
    // Marcar el timestamp de entrada al nuevo estado
    pcb->timestamp_entrada_estado[nuevo_estado] = timestamp_actual;
    
    // Actualizar en la lista global
    actualizar_pcb_en_lista_global(pcb);
    
    log_info(logger, "## (%d) cambió de estado %d a %d", pcb->pid, estado_anterior, nuevo_estado);
}

// Función para inicializar métricas cuando creas un PCB
void inicializar_metricas_pcb(t_pcb* pcb) {
    if (pcb == NULL) return;
    
    // Limpiar arrays
    memset(pcb->cantidad_veces_en_estado, 0, sizeof(pcb->cantidad_veces_en_estado));
    memset(pcb->tiempo_en_milisegundos, 0, sizeof(pcb->tiempo_en_milisegundos));
    memset(pcb->timestamp_entrada_estado, 0, sizeof(pcb->timestamp_entrada_estado));
    
    // Inicializar timestamps
    int timestamp_actual = timestamp_milisegundos();
    pcb->timestamp_ultima_actualizacion = timestamp_actual;
    
    // Marcar entrada al estado NEW
    pcb->timestamp_entrada_estado[ESTADO_NEW] = timestamp_actual;
    // pcb->cantidad_veces_en_estado[ESTADO_NEW] = 1;
}

void registrar_ejecucion(t_pcb* pcb, int socket_dispatch, int socket_interrupt) {
    t_ejecucion* ejecucion = malloc(sizeof(t_ejecucion));
    ejecucion->pcb = pcb;
    ejecucion->socket_dispatch = socket_dispatch;
    ejecucion->socket_interrupt = socket_interrupt;

    list_add(procesos_en_ejecucion, ejecucion);
}

t_ejecucion* obtener_ejecucion_por_pid(int pid) {
    bool _match(void* elem) {
        t_ejecucion* ejec = (t_ejecucion*)elem;
        return ejec->pcb->pid == pid;
    }
    return list_find(procesos_en_ejecucion, _match);
}

void finalizar_ejecucion(int pid) {
    bool _match(void* elem) {
        t_ejecucion* ejec = (t_ejecucion*)elem;
        return ejec->pcb->pid == pid;
    }

    t_ejecucion* ejec = list_remove_by_condition(procesos_en_ejecucion, _match);
    if (ejec != NULL) {
        // OJO: no liberar la PCB si se sigue usando
        free(ejec);
    }
}

void destruir_procesos_en_ejecucion() {
    void _destroy(void* elem) {
        t_ejecucion* ejec = (t_ejecucion*)elem;
        free(ejec);
    }
    list_destroy_and_destroy_elements(procesos_en_ejecucion, _destroy);
}

t_ejecucion* obtener_ejecucion_por_socket_dispatch(int socket_dispatch) {
    bool _match(void* elem) {
        t_ejecucion* ejec = (t_ejecucion*)elem;
        return ejec->socket_dispatch == socket_dispatch;
    }
    return list_find(procesos_en_ejecucion, _match);
}

t_pcb* obtener_pcb_con_mayor_estimacion() {
    if (list_is_empty(procesos_en_ejecucion)) return NULL;

    t_ejecucion* max = list_get(procesos_en_ejecucion, 0);

    for (int i = 1; i < list_size(procesos_en_ejecucion); i++) {
        t_ejecucion* actual = list_get(procesos_en_ejecucion, i);
        if (actual->pcb->estimacion_rafaga > max->pcb->estimacion_rafaga) {
            max = actual;
        }
    }

    return max->pcb;
}

void enviar_interrupcion_a_cpu_de_pid(int pid) {
    t_ejecucion* ejec = obtener_ejecucion_por_pid(pid);

    if (ejec == NULL) {
        log_error(logger, "[INTERRUPT] No se encontró ejecución activa para el PID=%d", pid);
        return;
    }

    int op = DESALOJO;
    int socket_interrupt = ejec->socket_interrupt;

    log_info(logger, "[INTERRUPT] Enviando señal de interrupción al PID=%d (socket_interrupt=%d)", pid, socket_interrupt);
    send(socket_interrupt, &op, sizeof(int), 0);
}

void finalizar_todos_los_procesos_bloqueados_por(char* dispositivo) {
    for (int i = 0; i < list_size(lista_global_procesos); i++) {
        t_pcb* pcb = list_get(lista_global_procesos, i);
        if (
            (pcb->estado == ESTADO_BLOCKED || pcb->estado == ESTADO_SUSP_BLOCKED)
            && strcmp(pcb->nombre_dispositivo_io, dispositivo) == 0
            && pcb->nombre_dispositivo_io != NULL
        ) {
            log_info(logger, "[IO] Finalizando PID=%d bloqueado/suspendido en %s", pcb->pid, dispositivo);
            pthread_cancel(pcb->hilo_suspension);
            pthread_join(pcb->hilo_suspension, NULL);
            finalizar_proceso(pcb);
        }
    }
}

void calcular_y_actualizar_rafaga(t_pcb* pcb) {
    if (pcb == NULL) {
        log_error(logger, "Error: PCB NULL en cálculo de ráfaga");
        return;
    }

    // Calcular tiempo de ráfaga real
    int fin = timestamp_milisegundos();
    int rafaga_real = fin - pcb->tiempo_inicio_exec;

    // Aplicar fórmula SJF/SRT: nueva_estimacion = alfa * rafaga_real + (1 - alfa) * estimacion_anterior
    float nueva_estimacion = alfa * rafaga_real + (1 - alfa) * pcb->estimacion_rafaga;

    // Actualizar estimación
    pcb->estimacion_rafaga = nueva_estimacion;

    // Log de la actualización
    log_info(logger, "## (%d) Ráfaga real: %d ms | Nueva estimación: %.2f ms", pcb->pid, rafaga_real, nueva_estimacion);

    // Actualizar en la lista global
    actualizar_pcb_en_lista_global(pcb);
}

void inicializar_sistema_desalojos() {
    desalojos_esperados = list_create();
}

void registrar_desalojo_esperado(int pid) {
    pthread_mutex_lock(&mutex_desalojos_esperados);
    
    t_desalojo_esperado* desalojo = malloc(sizeof(t_desalojo_esperado));
    desalojo->pid = pid;
    desalojo->timestamp_solicitud = timestamp_milisegundos();
    desalojo->completado = false;
    
    list_add(desalojos_esperados, desalojo);
    
    pthread_mutex_unlock(&mutex_desalojos_esperados);
    //log_debug(logger, "[DESALOJO] Registrado desalojo esperado para PID=%d", pid);
}

bool esperar_desalojo_con_timeout(int pid, int timeout_ms) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_nsec += (timeout_ms % 1000) * 1000000;
    ts.tv_sec += timeout_ms / 1000 + ts.tv_nsec / 1000000000;
    ts.tv_nsec %= 1000000000;

    pthread_mutex_lock(&mutex_desalojos_esperados);
    
    while (1) {
        bool encontrado = false;
        for (int i = 0; i < list_size(desalojos_esperados); i++) {
            t_desalojo_esperado* desalojo = list_get(desalojos_esperados, i);
            if (desalojo->pid == pid) {
                if (desalojo->completado) {
                    list_remove_and_destroy_element(desalojos_esperados, i, free);
                    pthread_mutex_unlock(&mutex_desalojos_esperados);
                    return true;
                }
                encontrado = true;
                break;
            }
        }
        
        if (!encontrado) {
            pthread_mutex_unlock(&mutex_desalojos_esperados);
            return true;
        }

        int res = pthread_cond_timedwait(&cond_desalojos, &mutex_desalojos_esperados, &ts);
        if (res != 0) {
            break;
        }
    }

    // Timeout alcanzado, limpiar entrada
    for (int i = 0; i < list_size(desalojos_esperados); i++) {
        t_desalojo_esperado* desalojo = list_get(desalojos_esperados, i);
        if (desalojo->pid == pid) {
            list_remove_and_destroy_element(desalojos_esperados, i, free);
            break;
        }
    }
    pthread_mutex_unlock(&mutex_desalojos_esperados);
    
    return false;
}

void marcar_desalojo_completado(int pid) {
    pthread_mutex_lock(&mutex_desalojos_esperados);
    
    for (int i = 0; i < list_size(desalojos_esperados); i++) {
        t_desalojo_esperado* desalojo = list_get(desalojos_esperados, i);
        if (desalojo->pid == pid) {
            desalojo->completado = true;
            pthread_cond_broadcast(&cond_desalojos);
            pthread_mutex_unlock(&mutex_desalojos_esperados);
            return;
        }
    }
    
    pthread_mutex_unlock(&mutex_desalojos_esperados);
}
