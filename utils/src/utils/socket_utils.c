#include "socket_utils.h"


int iniciar_servidor(int puerto, t_log* logger){
    int socket_servidor;

    struct addrinfo hints, *servinfo;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    char puerto_str[6]; // 65535 + null = 6
    sprintf(puerto_str, "%d", puerto); // paso el entero del puerto a un char* por los parametro q necesita getaddrinfo

    getaddrinfo(NULL, puerto_str, &hints, &servinfo); 

    socket_servidor = socket(servinfo->ai_family,
                             servinfo->ai_socktype,
                             servinfo->ai_protocol);

    bind(socket_servidor, servinfo->ai_addr, servinfo->ai_addrlen);

    listen(socket_servidor, SOMAXCONN);
    
    freeaddrinfo(servinfo);
    log_trace(logger, "Listo para escuchar");

    return socket_servidor;
}


int esperar_cliente(int socket_servidor, t_log* logger){
	int socket_cliente = accept(socket_servidor,NULL,NULL);
	log_info(logger,"Se conecto un cliente");

	return socket_cliente;
}

int crear_conexion(char* ip, int puerto){
    struct addrinfo hints, *server_info;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    char puerto_str[6]; // máximo "65535" + null
    sprintf(puerto_str, "%d", puerto); // paso el entero del puerto a un char* por los parametro q necesita getaddrinfo

    getaddrinfo(ip, puerto_str, &hints, &server_info);

    int socket_cliente = socket(server_info->ai_family,
                                server_info->ai_socktype,
                                server_info->ai_protocol);

    connect(socket_cliente, server_info->ai_addr, server_info->ai_addrlen);

    freeaddrinfo(server_info);
    return socket_cliente;
}
