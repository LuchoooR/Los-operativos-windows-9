#include <operaciones_m.h>
#include <threads_mem.h>
#include <commons/log.h>
#include <commons/config.h>
#include <commons/collections/dictionary.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <pthread.h>
#include <string.h>
#include <hello.h>
#include <socket_utils.h>
#include <op_code.h>
#include <protocolo.h>

// === Variables globales ===
t_log* logger;
t_config* config;

int tam_pagina;
int entradas_por_tabla;
int cantidad_niveles;
int tam_memoria;
char* path_swap;
int retardo_swap;
char* dump_path;
char* ruta_base_scripts;
int retardo_memoria;
int cantidad_marcos;
bool* bitmap_marcos;

void* memoria_usuario;
t_dictionary* metricas_por_pid;
t_dictionary* swap_map;
t_dictionary* paginas_por_pid;
t_list* lista_procesos;
t_list* lista_scripts;
t_list* lista_entradas_swap;


// === Prototipos ===
void inicializar_estructuras_globales();
void cargar_configuracion(t_config* config);

// === Main ===
int main(int argc, char* argv[]) {
    if (argc < 2) {
        printf("Uso: ./bin/memoria <archivo_configuracion>\n");
        return EXIT_FAILURE;
    }

    char* config_path = argv[1];

    char* DEFAULT_CONFIG_DIR  = "./configsMEMORIA/";
    char ruta_completa[256];
    snprintf(ruta_completa, sizeof(ruta_completa), "%sconfig%s.cfg", DEFAULT_CONFIG_DIR, config_path);

    logger = log_create("memoria.log", "MEMORIA", true, LOG_LEVEL_INFO);

    t_config* config = leer_config(ruta_completa, logger);
    if (config == NULL) {
        log_error(logger, "No se pudo leer el archivo de configuración: %s", config_path);
        log_destroy(logger);
        return EXIT_FAILURE;
    }

    cargar_configuracion(config);
    inicializar_estructuras_globales();

    log_info(logger, "Memoria principal reservada (%d bytes)", tam_memoria);
    log_info(logger, "Cantidad de marcos: %d", cantidad_marcos);
    log_info(logger, "Memoria inicializada con éxito");

    int puerto_memoria = config_get_int_value(config, "PUERTO_ESCUCHA");
    iniciar_servidor_memoria(logger, puerto_memoria);

    // Cleanup
    config_destroy(config);
    free(memoria_usuario);
    dictionary_destroy(metricas_por_pid);
    dictionary_destroy(swap_map);
    dictionary_destroy(paginas_por_pid);
    free(bitmap_marcos);
    log_destroy(logger);

    return EXIT_SUCCESS;
}

// === Cargar configuración ===
void cargar_configuracion(t_config* config) {

    tam_memoria = config_get_int_value(config, "TAM_MEMORIA");
    tam_pagina = config_get_int_value(config, "TAM_PAGINA");
    entradas_por_tabla = config_get_int_value(config, "ENTRADAS_POR_TABLA");
    cantidad_niveles = config_get_int_value(config, "CANTIDAD_NIVELES");
    path_swap = config_get_string_value(config, "PATH_SWAPFILE");
    retardo_swap = config_get_int_value(config, "RETARDO_SWAP");
    dump_path = config_get_string_value(config, "DUMP_PATH");
    ruta_base_scripts = config_get_string_value(config, "PATH_INSTRUCCIONES");
    retardo_memoria = config_get_int_value(config, "RETARDO_MEMORIA");

    cantidad_marcos = tam_memoria / tam_pagina;
}

// === Inicialización de estructuras ===
void inicializar_estructuras_globales() {
    memoria_usuario = malloc(tam_memoria);
    memset(memoria_usuario, 0, tam_memoria);
    metricas_por_pid = dictionary_create();
    swap_map = dictionary_create();
    paginas_por_pid = dictionary_create();
    bitmap_marcos = calloc(cantidad_marcos, sizeof(bool)); // false = libre
    lista_procesos = list_create();
    lista_scripts = list_create();
    lista_entradas_swap = list_create();

}

