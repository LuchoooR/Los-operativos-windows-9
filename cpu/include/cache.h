#ifndef CACHE_H
#define CACHE_H

#include <commons/log.h>
#include <stdbool.h>

// === ESTRUCTURAS ===
// Caché
typedef struct {
    int pid;
    int pagina;
    void* contenido;
    bool modificado;
    bool uso;
} t_entrada_cache;

// === FUNCIONES ===
// Iniciar Cache
void iniciar_cache(int cantidad_entradas, char* algoritmo);
void cargar_en_cache(int pid, int pagina, void* datos, t_log* logger, int socket_memoria);

// Acceso a cache
t_entrada_cache* buscar_entrada_cache(int pid, int pagina);
void* leer_de_cache(int pid, int pagina, bool* hit, int socket_memoria, t_log* logger);
void escribir_en_cache(int pid, int pagina, void* datos, t_log* logger, int socket_memoria);
int seleccionar_victima();

// Limpiar cache del proceso
bool misma_pid_cache(void* elem, void* pid_ptr);
void limpiar_cache_de_proceso(int pid, t_log* logger, int socket_memoria);
void destruir_cache();

#endif