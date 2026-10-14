#include <hello.h>
#include <string.h>
#include <op_code.h>
#include <commons/config.h>
#include <commons/log.h>
#include <socket_utils.h>
#include <protocolo.h>
#include <planificador_corto_plazo.h>
#include <planificador_largo_plazo.h>
#include <kernel_syscalls.h>
#include <threads_kernel.h>
#include <planificador.h>
#include <stdlib.h>

// === Variables globales ===
t_log* logger;
int tiempo_suspension;
int puerto_memoria;
char* ip_memoria;
t_list* lista_cpus = NULL;
t_list* procesos_en_ejecucion; // clave: string con el pid
extern t_dictionary* dispositivos_io;
extern t_list* lista_global_procesos;
int puerto_io;
int puerto_interrupt;
int puerto_dispatch;
char* ingreso_ready;
char* algoritmo_planificacion;
float alfa;
int estimacion_inicial;
char* log_level_str;
t_log_level info_log;
t_list* desalojos_esperados;


// === Prototipos ===
void cargar_configuracion(t_config* config);
void inicializar_estructuras_globales();
void destruir_dispositivo_io(void* ptr);

// === Main ===
int main(int argc, char* argv[]){

    if (argc < 4) {
        printf("Uso: ./bin/kernel <archivo_pseudocodigo> <tamanio_proceso> <config_path>\n");
        return EXIT_FAILURE;
    }

    char* archivo_pseudocodigo = argv[1];
    int tamanio_proceso = atoi(argv[2]);
    char* config_path = argv[3];
    

    t_log* logger_inicio = iniciar_logger("kernel.log", "KERNEL", LOG_LEVEL_INFO);

    char* DEFAULT_CONFIG_DIR  = "./configsKERNEL/";
    char ruta_completa[256];
    snprintf(ruta_completa, sizeof(ruta_completa), "%sconfig%s.cfg", DEFAULT_CONFIG_DIR, config_path);

    t_config* config = leer_config(ruta_completa, logger_inicio);
    if (config == NULL) {
        log_error(logger_inicio, "Error al leer el archivo de configuración en ruta: %s", config_path);
        return EXIT_FAILURE;
    }
    log_destroy(logger_inicio);

    // Carga Configuracion y Estructuras Globales
    cargar_configuracion(config);
    inicializar_estructuras_globales();

    logger = iniciar_logger("kernel.log", "KERNEL", info_log);

    iniciar_hilos_escucha(puerto_io, puerto_dispatch, puerto_interrupt);

    // Iniciar planificadores (Largo/Mediano/Corto Plazo)
    iniciar_planificadores();

    crear_proceso_con_pseudocodigo(archivo_pseudocodigo, tamanio_proceso);

    pause();

    config_destroy(config);
    log_destroy(logger);
    dictionary_destroy_and_destroy_elements(dispositivos_io,destruir_dispositivo_io);


    return EXIT_SUCCESS;
}

// === Cargar configuración ===
void cargar_configuracion(t_config* config){
    ip_memoria = config_get_string_value(config, "IP_MEMORIA");
    puerto_memoria = config_get_int_value(config, "PUERTO_MEMORIA");
    puerto_dispatch = config_get_int_value(config, "PUERTO_ESCUCHA_DISPATCH");
    puerto_interrupt = config_get_int_value(config, "PUERTO_ESCUCHA_INTERRUPT");
    puerto_io = config_get_int_value(config, "PUERTO_ESCUCHA_IO");
    algoritmo_planificacion = config_get_string_value(config, "ALGORITMO_CORTO_PLAZO");
    ingreso_ready = config_get_string_value(config, "ALGORITMO_INGRESO_A_READY");
    alfa = config_get_int_value(config, "ALFA");
    estimacion_inicial = config_get_int_value(config, "ESTIMACION_INICIAL");
    tiempo_suspension = config_get_int_value(config, "TIEMPO_SUSPENSION");
    log_level_str = config_get_string_value(config, "LOG_LEVEL");

    info_log = log_level_from_string(log_level_str);
}

// === Inicialización de estructuras ===
void inicializar_estructuras_globales(){
    lista_cpus = list_create();
    dispositivos_io = dictionary_create();
    procesos_en_ejecucion = list_create();
    lista_global_procesos = list_create();
    inicializar_sistema_desalojos();
}

void destruir_dispositivo_io(void* ptr){
    t_dispositivo_io* dispositivo = (t_dispositivo_io*) ptr;
    queue_destroy(dispositivo->cola_bloqueados); 
    free(dispositivo); 
}