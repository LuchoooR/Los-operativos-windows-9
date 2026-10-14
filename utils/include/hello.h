#ifndef UTILS_HELLO_H_
#define UTILS_HELLO_H_

#include <stdlib.h>
#include <stdio.h>
#include <commons/log.h>
#include <commons/config.h>
#include <commons/string.h>
#include <readline/readline.h>
#include <commons/collections/list.h>

/**
* @brief Imprime un saludo por consola
* @param quien Módulo desde donde se llama a la función
* @return No devuelve nada
*/
void saludar(char* quien);

/**
* @brief inicializa un log
* @param archivo_log nombre del archivo donde se van a guardar los logs 
* @param nombre_modulo etiqueta que identifica el módulo que genera el log. Aparece al inicio de cada línea del log
* @param nivel nivel de detalle máximo del log. Puede ser LOG_LEVEL_INFO, LOG_LEVEL_DEBUG, LOG_LEVEL_TRACE, etc.
* @return puntero al t_log
*/
t_log* iniciar_logger(char* archivo_log, char* nombre_modulo, t_log_level nivel);

/**
* @brief leer un cgf
* @param arch_cfg nombre del archivo cfg a leer
* @param logger logger
* @return la cfg lista
*/
t_config* leer_config(char* arch_cfg, t_log* logger);


void leer_consola(t_log* logger);

#endif
