#ifndef CPU_H
#define CPU_H

#include <stdbool.h>
#include <commons/log.h>
#include <op_code.h>

// === ESTRUCTURAS ===
// Estados de proceso
typedef enum {
    ESTADO_NEW,
    ESTADO_READY,
    ESTADO_SUSP_READY,
    ESTADO_EXEC,
    ESTADO_BLOCKED,
    ESTADO_SUSP_BLOCKED,
    ESTADO_EXIT
} estado_proceso_t;

// PCB
typedef struct {
    int pid;
    int tamanio;
    int program_counter;
    estado_proceso_t estado;
} t_pcb;

// === FUNCIONES ===
// PCB
t_pcb deserializar_pcb(void* buffer);
t_pcb recibir_pcb(int socket_dispatch, t_log* logger);
void* serializar_pcb(t_pcb* pcb, int* size);
void enviar_pcb(t_pcb* pcb, int socket_dispatch, t_log* logger);

// Ciclo CPU
int fetch(int socket_memoria, t_log* logger, t_pcb* pcb);
void decode_and_execute(int cod_op, int socket_memoria,int socket_kernel , t_log* logger, t_pcb* pcb);
int mmu_traduccion(int direccion_logica, t_log* logger, t_pcb* pcb, int socket_memoria);
void enviar_mmu_solicitud(int pid, int direccion_logica, int socket_memoria);
int recibir_marco(int socket_memoria);

// Handshakes
void handshake_de_cpu_memoria(int socket_memoria, t_log* logger); 
void handshake_de_cpu_kernel(int socket_cliente, t_log* logger, char* identificador);

// Funciones para Instrucciones y Syscalls
void ejecutar_read(t_instruc_read* readin,t_log* logger, t_pcb* pcb, int socket_memoria);
void ejecutar_write(t_instruc_write* writin, t_log* logger, t_pcb* pcb , int socket_memoria);
void ejecutar_goto(int pc_nuevo,int cod_op ,t_log* logger,t_pcb* pcb);

void ejecutar_exit (t_log* logger,int cantidad, t_pcb* pcb, int socket_kernel, int socket_memoria);
void ejecutar_dump_memory(t_log* logger, int cantidad, t_pcb* pcb, int socket_kernel);
void ejecutar_init_proc(t_instruc_init_proc proc, int socket_kernel, t_log* logger, t_pcb* pcb);

// Funcion en duda
void escribir_pagina_completa(int pid, int pagina, void* datos, int socket_memoria, t_log* logger);

void* solicitar_pagina_completa(int pid,int dirreccion_fisica_base,int socket_memoria,t_log* logger);


#endif