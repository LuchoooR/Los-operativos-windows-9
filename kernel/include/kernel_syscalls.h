#ifndef KERNEL_SYSCALLS_H
#define KERNEL_SYSCALLS_H

#include <planificador.h>
#include <op_code.h>

typedef struct {
    int socket;
    bool ocupado;
    int pid_actual;
} t_instancia_io;

typedef struct {
    t_queue* cola_bloqueados;
    t_list* instancias; // lista de t_instancia_io*
} t_dispositivo_io;

// Prototipos de funciones para las syscalls de procesos

// Crea un nuevo proceso y lo encola en NEW (no bloquea el proceso llamador)
void syscall_init_proc(char* path, int tamanio);

// Finaliza el proceso llamador, lo pasa a EXIT y avisa a Memoria
void syscall_exit(t_pcb* pcb);

// Solicita un DUMP a Memoria, bloquea el proceso hasta recibir respuesta
void syscall_dump_memory(t_pcb* pcb);

void syscall_io(t_instruc_io instruccion, t_log* logger, t_pcb* pcb);

#endif // KERNEL_SYSCALLS_H
