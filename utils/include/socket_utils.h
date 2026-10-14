#ifndef SOCKET_UTILS_H
#define SOCKET_UTILS_H
#define _GNU_SOURCE
#define PUERTO "4444"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include<commons/log.h>
#include<commons/collections/list.h>
#include<commons/string.h>
#include"op_code.h"

/**
* @brief Inicia un servidor
* @param puerto Puerto donde se va a escuchar
* @param logger Logger
* @return Devuelve el socket servidor
*/
int iniciar_servidor(int puerto, t_log* logger);

int esperar_cliente(int servidor_fd, t_log* logger);

int crear_conexion(char* ip, int puerto);




#endif