//ALRIGHT ITS TLBIN TIME!!

#include "tlb.h"
#include <commons/log.h>
#include <commons/collections/list.h>
#include <time.h>
#include <string.h>
#include <stdlib.h>

static t_list* tlb;
static int entradas_max;
static char* algoritmo;
static int contador_global = 0;

void iniciar_tlb(int cantidad_entradas, char* algoritmo_config){
    tlb = list_create();
    entradas_max = cantidad_entradas;
    algoritmo = strdup(algoritmo_config);
}

int consultar_tlb(int pid, int pagina, t_log* logger, bool* hit) {
    for (int i = 0; i < list_size(tlb); i++) {
        t_entrada_tlb* entrada = list_get(tlb, i);
        if (entrada->pid == pid && entrada->pagina == pagina) {
            *hit = true;
            entrada->timestamp = contador_global++; // Para LRU
            log_info(logger, "PID: %d - TLB HIT - Pagina: %d", pid, pagina);
            return entrada->marco;
        }
    }

    *hit = false;

    log_info(logger, "PID: %d - TLB MISS - Pagina: %d", pid, pagina);
    
    return -1;
}

void agregar_a_tlb(int pid, int pagina, int marco, t_log* logger) {
    t_entrada_tlb* nueva = malloc(sizeof(t_entrada_tlb));
    nueva->pid = pid;
    nueva->pagina = pagina;
    nueva->marco = marco;
    nueva->timestamp = contador_global++;

    if (list_size(tlb) >= entradas_max) {
        // Reemplazo
        int indice_reemplazo = 0;
        if (strcmp(algoritmo, "LRU") == 0) {
            int min_ts = ((t_entrada_tlb*)list_get(tlb, 0))->timestamp;
            for (int i = 1; i < list_size(tlb); i++) {
                t_entrada_tlb* e = list_get(tlb, i);
                if (e->timestamp < min_ts) {
                    min_ts = e->timestamp;
                    indice_reemplazo = i;
                }
            }
        } else if (strcmp(algoritmo, "FIFO") == 0) {
            indice_reemplazo = 0;
        }

        t_entrada_tlb* viejo = list_remove(tlb, indice_reemplazo);
        free(viejo);
    }

    list_add(tlb, nueva);
    log_info(logger, "PID: %d - Agregado a TLB - Página: %d → Marco: %d", pid, pagina, marco);
}

bool misma_pid(void* elem, void* pid_ptr) {
    t_entrada_tlb* e = (t_entrada_tlb*)elem;
    int pid = *(int*)pid_ptr;
    return e->pid == pid;
}

void limpiar_tlb_de_proceso(int pid, t_log* logger) {
    list_remove_and_destroy_all_by_condition(tlb, misma_pid, &pid);
    log_info(logger, "PID: %d - TLB limpiada por desalojo", pid);
}


void destruir_tlb() {
    list_destroy_and_destroy_elements(tlb, free);
}