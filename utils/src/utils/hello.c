#include "hello.h"
#include <commons/log.h> 
#include <commons/config.h>
#include <stdlib.h>
#include <commons/string.h>
#include <commons/collections/list.h>

void saludar(char* quien) {
    printf("Hola desde %s!!\n", quien);
}

t_log* iniciar_logger(char* archivo_log, char* nombre_modulo, t_log_level nivel) {
    t_log* logger = log_create(archivo_log, nombre_modulo, true, nivel);

    if (logger == NULL) {
        printf("No se pudo crear el logger para %s\n", nombre_modulo);
        exit(EXIT_FAILURE);
    }

    return logger;
}

t_config* leer_config(char* arch_cfg, t_log* logger){
    t_config* config = config_create(arch_cfg);
    if(!config){
        log_error(logger,"No se pudo leer el archivo de configuracion");
        log_destroy(logger);
        return EXIT_FAILURE;
    }
    return config;
}

void leer_consola(t_log* logger){
    char* leido;
    leido = readline("> ");
    while(strcmp(leido,"") != 0){
        free(leido);
        leido = readline("> ");
        log_info(logger,">> %s",leido);
    }
    free(leido);
}