#include "cache.h"
#include <commons/log.h>
#include <commons/collections/list.h>
#include <time.h>
#include <cpu.h>
#include <string.h>
#include <stdlib.h>

static t_list* cache;
static int entradas_max_cache;
static char* algoritmo_cache;
static int puntero_clock = 0;
extern bool cache_activada;
extern t_log* logger;

extern int retardo_cache;
extern int TAM_PAGINA;

void iniciar_cache(int cantidad_entradas, char* algoritmo){
    cache = list_create();
    entradas_max_cache = cantidad_entradas;
    algoritmo_cache = strdup(algoritmo);
}

t_entrada_cache* buscar_entrada_cache(int pid, int pagina) {
    usleep(retardo_cache*1000);
    for (int i = 0; i < list_size(cache); i++) {
        t_entrada_cache* e = list_get(cache, i);
        if (e->pid == pid && e->pagina == pagina){
            log_info(logger, "PID: %d - Cache Hit - Pagina: %d", pid, pagina);
            return e;
        }
    }

    log_info(logger, "PID: %d - Cache Miss - Pagina: %d", pid, pagina);
    
    return NULL;
}

void cargar_en_cache(int pid, int pagina, void* datos, t_log* logger, int socket_memoria) {
    // Crear nueva entrada (solo lectura)
    t_entrada_cache* nueva = malloc(sizeof(t_entrada_cache));
    nueva->pid = pid;
    nueva->pagina = pagina;
    nueva->contenido = malloc(TAM_PAGINA);
    memcpy(nueva->contenido, datos, TAM_PAGINA);
    nueva->uso = true;
    nueva->modificado = false;  // Solo lectura al cargar

    if (list_size(cache) >= entradas_max_cache) {
        int victima = seleccionar_victima();
        t_entrada_cache* a_reemplazar = list_get(cache, victima);

        if (a_reemplazar->modificado) {
            escribir_pagina_completa(a_reemplazar->pid, a_reemplazar->pagina, 
                                   a_reemplazar->contenido, socket_memoria, logger);
        }

        free(a_reemplazar->contenido);
        list_replace_and_destroy_element(cache, victima, nueva, free);
    } else {
        list_add(cache, nueva);
    }

    if (logger) {
        log_info(logger, "PID: %d - Cache Add - Pagina: %d", pid, pagina);
    }
}

void* leer_de_cache(int pid, int pagina, bool* hit, int socket_memoria, t_log* logger) {
    t_entrada_cache* entrada = buscar_entrada_cache(pid, pagina);
    
    if (entrada) {
        // Cache HIT
        entrada->uso = true;
        *hit = true;
        return entrada->contenido;
    }
    
    // Cache MISS - traer página completa desde memoria
    *hit = false;
    
    log_info(logger, "PID: %d - Cache Miss - Cargando página %d", pid, pagina);
    
    // Calcular dirección física base de la página
    int direccion_logica = pagina * TAM_PAGINA;
    int direccion_fisica_base = mmu_traduccion(direccion_logica, logger, &(t_pcb){ .pid = pid }, socket_memoria);
    
    // Ajustar a base de página (asegurar que sea múltiplo de TAM_PAGINA)
    direccion_fisica_base = (direccion_fisica_base / TAM_PAGINA) * TAM_PAGINA;
    
    // Solicitar página completa desde memoria
    void* contenido_pagina = solicitar_pagina_completa(pid, direccion_fisica_base, socket_memoria, logger);
    
    if (contenido_pagina != NULL) {
        // Agregar al cache (sin marcar como modificado)
        cargar_en_cache(pid, pagina, contenido_pagina, logger, socket_memoria);
        
        // Buscar la entrada que acabamos de crear
        entrada = buscar_entrada_cache(pid, pagina);
        if(entrada) {
            free(contenido_pagina); // Liberar la copia temporal
            return entrada->contenido;
        }
        
        // Si por alguna razón no se pudo agregar al cache, devolver la copia temporal
        log_warning(logger, "No se pudo agregar página al cache, devolviendo copia temporal");
        return contenido_pagina;
    }
    
    log_error(logger, "Error: No se pudo obtener página desde memoria");
    return NULL;
}

void escribir_en_cache(int pid, int pagina, void* datos, t_log* logger, int socket_memoria) {
    t_entrada_cache* entrada = buscar_entrada_cache(pid, pagina);
    
    if (entrada) {
        memcpy(entrada->contenido, datos, TAM_PAGINA);
        entrada->uso = true;
        entrada->modificado = true;
        return;
    }

    // Crear nueva entrada
    t_entrada_cache* nueva = malloc(sizeof(t_entrada_cache));
    nueva->pid = pid;
    nueva->pagina = pagina;
    nueva->contenido = malloc(TAM_PAGINA);
    memcpy(nueva->contenido, datos, TAM_PAGINA);
    nueva->uso = true;
    nueva->modificado = true;

    if (list_size(cache) >= entradas_max_cache) {
        int victima = seleccionar_victima();
        t_entrada_cache* a_reemplazar = list_get(cache, victima);

        if (a_reemplazar->modificado) {
            escribir_pagina_completa(a_reemplazar->pid, a_reemplazar->pagina, 
                                   a_reemplazar->contenido, socket_memoria, logger);
        }

        free(a_reemplazar->contenido);
        list_replace_and_destroy_element(cache, victima, nueva, free);
    } else {
        list_add(cache, nueva);
    }

    if (logger) {
        log_info(logger, "PID: %d - Cache Add - Pagina: %d", pid, pagina);
    }
}

int seleccionar_victima() {
    int size = list_size(cache);

    if (strcmp(algoritmo_cache, "CLOCK") == 0) {
        while (1) {
            t_entrada_cache* e = list_get(cache, puntero_clock);

            log_debug(logger, "[CACHE] CLOCK: Revisando entrada %d → PID=%d, Página=%d, Uso=%d",
                     puntero_clock, e->pid, e->pagina, e->uso);

            if (!e->uso) {
                log_debug(logger, "[CACHE] CLOCK: Reemplazando entrada en posición %d → PID=%d, Página=%d",
                         puntero_clock, e->pid, e->pagina);
                int victima = puntero_clock;
                puntero_clock = (puntero_clock + 1) % size;
                return victima;
            }

            e->uso = false;
            log_debug(logger, "[CACHE] CLOCK: Bajando bit de uso en entrada %d (PID=%d, Página=%d)",
                     puntero_clock, e->pid, e->pagina);

            puntero_clock = (puntero_clock + 1) % size;
        }
    }

    if (strcmp(algoritmo_cache, "CLOCK-M") == 0) {
        int vueltas = 0;

        while (1) {
            // PRIMERA VUELTA: uso = 0, modificado = 0
            for (int i = 0; i < size; i++) {
                t_entrada_cache* e = list_get(cache, puntero_clock);

                log_debug(logger, "[CACHE] CLOCK-M [1° Vuelta]: Revisando entrada %d → PID=%d, Página=%d, Uso=%d, Modificado=%d",
                         puntero_clock, e->pid, e->pagina, e->uso, e->modificado);

                if (!e->uso && !e->modificado) {
                    log_debug(logger, "[CACHE] CLOCK-M: Reemplazando entrada (0,0) en posición %d → PID=%d, Página=%d",
                             puntero_clock, e->pid, e->pagina);
                    int victima = puntero_clock;
                    puntero_clock = (puntero_clock + 1) % size;
                    return victima;
                }

                puntero_clock = (puntero_clock + 1) % size;
            }

            // SEGUNDA VUELTA: uso = 0, modificado = 1
            for (int i = 0; i < size; i++) {
                t_entrada_cache* e = list_get(cache, puntero_clock);

                log_debug(logger, "[CACHE] CLOCK-M [2° Vuelta]: Revisando entrada %d → PID=%d, Página=%d, Uso=%d, Modificado=%d",
                         puntero_clock, e->pid, e->pagina, e->uso, e->modificado);

                if (!e->uso && e->modificado) {
                    log_debug(logger, "[CACHE] CLOCK-M: Reemplazando entrada (0,1) en posición %d → PID=%d, Página=%d",
                             puntero_clock, e->pid, e->pagina);
                    int victima = puntero_clock;
                    puntero_clock = (puntero_clock + 1) % size;
                    return victima;
                }

                if (e->uso) {
                    e->uso = false;
                    log_debug(logger, "[CACHE] CLOCK-M: Bajando bit de uso en entrada %d (PID=%d, Página=%d)",
                             puntero_clock, e->pid, e->pagina);
                }

                puntero_clock = (puntero_clock + 1) % size;
            }

            vueltas++;
            if (vueltas > 10) {
                log_warning(logger, "[CACHE] CLOCK-M: No se encontró víctima en 10 vueltas. Forzando reemplazo en posición actual.");
                return puntero_clock;
            }
        }
    }

    // Algoritmo inválido
    log_error(logger, "Algoritmo de caché inválido: %s. Usando posición actual.", algoritmo_cache);
    int victima = puntero_clock;
    puntero_clock = (puntero_clock + 1) % size;
    return victima;
}



void limpiar_cache_de_proceso(int pid, t_log* logger, int socket_memoria){
    int size = list_size(cache);
    for (int i = 0; i < size; i++) {
        t_entrada_cache* e = list_get(cache, i);
        if (e->pid == pid && e->modificado) {
            escribir_pagina_completa(e->pid, e->pagina, e->contenido, socket_memoria, logger);
        }
    }

    list_remove_and_destroy_all_by_condition(cache, misma_pid_cache, &pid);

    log_info(logger, "CACHE: entradas de PID %d eliminadas", pid);
}

bool misma_pid_cache(void* elem, void* pid_ptr) {
    t_entrada_cache* e = (t_entrada_cache*)elem;
    int pid = *((int*)pid_ptr);
    return e->pid == pid;
}

void destruir_entrada_cache(void* entry) {
    t_entrada_cache* e = (t_entrada_cache*)entry;
    free(e->contenido);
    free(e);
}

void destruir_cache() {
    list_destroy_and_destroy_elements(cache, destruir_entrada_cache);
}