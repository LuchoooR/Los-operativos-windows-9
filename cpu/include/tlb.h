#ifndef TLB_H
#define TLB_H

#include <stdbool.h>
#include <commons/log.h>


// === ESTRUCTURAS ===
// Entrada de TLB
typedef struct{
    int pid;
    int pagina;
    int marco;
    int timestamp;
} t_entrada_tlb;

// === FUNCIONES ===
// Inicio TLB
void iniciar_tlb(int cantidad_entradas, char* algoritmo_config);

// Funciones de TLB
int consultar_tlb(int pid, int pagina, t_log* logger, bool* hit);
void agregar_a_tlb(int pid, int pagina, int marco, t_log* logger);

// Limpiar TLB
bool misma_pid(void* elem, void* pid_ptr);
void limpiar_tlb_de_proceso(int pid, t_log* logger);
void destruir_tlb();

#endif