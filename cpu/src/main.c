#include <hello.h>
#include <cpu.h>
#include <thread_dispatch.h>
#include <thread_interrupt.h>
#include <tlb.h>
#include <cache.h>
#include <string.h>
#include <op_code.h>
#include <commons/config.h>
#include <commons/log.h>
#include <protocolo.h>
#include <pthread.h>

bool cache_activada = false;
bool tlb_activada = false;
int retardo_cache;

int TAM_PAGINA;
int ENTRADAS_POR_TABLA;
int CANT_NIVELES;

int socket_memoria;
int socket_dispatch;
int socket_interrupt;

t_log* logger;

int main(int argc, char* argv[]) {
    if (argc < 3) {
        printf("Uso: %s <identificador> <path_config>\n", argv[0]);
        return EXIT_FAILURE;
    }

    char* identificador = argv[1];
    char* path_config = argv[2];

    char* DEFAULT_CONFIG_DIR  = "./configsCPU/";
    char ruta_completa[256];
    snprintf(ruta_completa, sizeof(ruta_completa), "%sconfig%s.cfg", DEFAULT_CONFIG_DIR, path_config);

    // Crear logger
    char log_filename[256];
    sprintf(log_filename, "cpu_%s.log", identificador);
    logger = iniciar_logger(log_filename, "CPU", LOG_LEVEL_INFO);

    // Leer config desde archivo pasado por argumento
    t_config* config = leer_config(ruta_completa, logger);
    if (!config) {
        log_error(logger, "No se pudo abrir el archivo de configuración %s", path_config);
        return EXIT_FAILURE;
    }

    char* ip_memoria = config_get_string_value(config, "IP_MEMORIA");
    int puerto_memoria = config_get_int_value(config, "PUERTO_MEMORIA");
    char* ip_kernel = config_get_string_value(config, "IP_KERNEL");
    int puerto_dispatch = config_get_int_value(config, "PUERTO_KERNEL_DISPATCH");
    int puerto_interrupt = config_get_int_value(config, "PUERTO_KERNEL_INTERRUPT");

    int entradas_tlb = config_get_int_value(config, "ENTRADAS_TLB");
    char* reemplazo_tlb = config_get_string_value(config, "REEMPLAZO_TLB");

    if (entradas_tlb == 0) {
        log_info(logger, "TLB Deshabilitada");
    } else {
        tlb_activada = true;
        iniciar_tlb(entradas_tlb, reemplazo_tlb);
        log_info(logger, "TLB Habilitada");
    }

    int entradas_cache = config_get_int_value(config, "ENTRADAS_CACHE");
    char* reemplazo_cache = config_get_string_value(config, "REEMPLAZO_CACHE");
    int retardo_cache = config_get_int_value(config, "RETARDO_CACHE");

    if (entradas_cache == 0) {
        log_info(logger, "Cache Deshabilitada");
    } else {
        cache_activada = true;
        iniciar_cache(entradas_cache, reemplazo_cache);
        log_info(logger, "Cache Habilitada");
    }

    // Conexiones
    socket_memoria = crear_conexion(ip_memoria, puerto_memoria);
    handshake_de_cpu_memoria(socket_memoria, logger);

    socket_dispatch = crear_conexion(ip_kernel, puerto_dispatch);
    handshake_de_cpu_kernel(socket_dispatch, logger, identificador);

    usleep(1000);

    socket_interrupt = crear_conexion(ip_kernel, puerto_interrupt);
    handshake_de_cpu_kernel(socket_interrupt, logger, identificador);

    log_info(logger, "CPU Conectada a Memoria y Kernel (los dos)");

    pthread_t tid_dispatch, tid_interrupt;
    pthread_create(&tid_dispatch, NULL, thread_dispatch, NULL);
    pthread_create(&tid_interrupt, NULL, thread_interrupt, NULL);

    pthread_join(tid_dispatch, NULL);
    pthread_join(tid_interrupt, NULL);

    config_destroy(config);
    log_destroy(logger);
    destruir_tlb();
    destruir_cache();

    return EXIT_SUCCESS;
}

void cargar_configuracion(t_config* config){}

