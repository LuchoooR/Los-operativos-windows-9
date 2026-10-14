#ifndef PLANIFICADOR_H
#define PLANIFICADOR_H

#include <commons/collections/queue.h>
#include <commons/log.h>
#include <stdint.h>
#include <pthread.h>
#include <stdbool.h>
#include <threads_kernel.h>


// Estados posibles del proceso
typedef enum {
    ESTADO_NEW,
    ESTADO_READY,
    ESTADO_SUSP_READY,
    ESTADO_EXEC,
    ESTADO_BLOCKED,
    ESTADO_SUSP_BLOCKED,
    ESTADO_EXIT
} estado_proceso_t;

// Estructura PCB
typedef struct {
    int pid;
    int tamanio;
    int program_counter;
    estado_proceso_t estado;
    char* path;
    int cantidad_veces_en_estado[7];
    int tiempo_en_milisegundos[7];
    int timestamp_entrada_estado[7];  // Cuando entró a cada estado
    int timestamp_ultima_actualizacion; // Última vez que se actualizó
    float estimacion_rafaga;
    pthread_t hilo_suspension;
    int tiempo_inicio_exec;
    int tiempo_io_pendiente;
    char* nombre_dispositivo_io;
} t_pcb;

typedef struct {
    t_pcb* pcb;
    int socket_dispatch;
    int socket_interrupt;
} t_ejecucion;

typedef struct {
    int pid;
    int timestamp_solicitud;
    bool completado;
} t_desalojo_esperado;

// === FUNCIONES ===
// Funciones PCB
t_pcb deserializar_pcb(void* buffer);
void* serializar_pcb(t_pcb* pcb, int* size);
void enviar_pcb(t_pcb* pcb, int socket, t_log* logger);
t_pcb* recibir_pcb(int socket, t_log* logger);
t_pcb* buscar_pcb_por_pid(int pid_buscado);
void actualizar_pcb_en_lista_global(t_pcb* pcb_actualizado);
void destruir_pcb(t_pcb* pcb);
void loggear_metricas_estado(t_pcb* pcb);
void actualizar_tiempo_estado(t_pcb* pcb, estado_proceso_t estado_actual);
void cambiar_estado_con_metricas(t_pcb* pcb, estado_proceso_t nuevo_estado);
void inicializar_metricas_pcb(t_pcb* pcb);

// Iniciar Planificadores
void iniciar_planificadores();

// Algoritmos
bool comparar_por_tamanio(void* a, void* b);
bool comparar_estimaciones(void* a, void* b);
void* temporizador_suspension(void* arg);
int timestamp_milisegundos();
void calcular_y_actualizar_rafaga(t_pcb* pcb_local);
void agregar_a_ready_con_prioridad(t_pcb* pcb, bool es_desalojo);

// Crear Procesos 
void crear_proceso(int pid, int tamanio, char* ruta_archivo);
void crear_proceso_con_pseudocodigo(char* path, int tamanio);

// Conexión con otro módulo
void avisar_suspension_a_memoria(t_pcb* pcb);
void avisar_desuspension_a_memoria(t_pcb* pcb, int socket_memoria);
void finalizar_proceso(t_pcb* pcb);
void manejar_fin_io(int pid_finalizado);

// Buscas CPU Libre
t_cpu_conectada* seleccionar_cpu_libre();
void marcar_cpu_libre(char* id_cpu);
void marcar_cpu_libre_por_socket(int socket_dispatch);

// Que grande Fran
void destruir_procesos_en_ejecucion();
void finalizar_ejecucion(int pid);
t_ejecucion* obtener_ejecucion_por_pid(int pid);
void registrar_ejecucion(t_pcb* pcb, int socket_dispatch, int socket_interrupt);
t_ejecucion* obtener_ejecucion_por_socket_dispatch(int socket_dispatch);
void enviar_interrupcion_a_cpu_de_pid(int pid);
t_pcb* obtener_pcb_con_mayor_estimacion();

void finalizar_todos_los_procesos_bloqueados_por(char* dispositivo);



void inicializar_sistema_desalojos();
void registrar_desalojo_esperado(int pid);
bool esperar_desalojo_con_timeout(int pid, int timeout_ms);
void marcar_desalojo_completado(int pid);

#endif // PLANIFICADOR_H
