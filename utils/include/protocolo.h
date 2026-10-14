#ifndef PROTOCOLO_H
#define PROTOCOLO_H
#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <commons/log.h>
#include <commons/collections/list.h>
#include <commons/string.h>
#include "op_code.h"

typedef struct
{
	int size;
	void* stream;
} t_buffer;

typedef struct
{
	op_code codigo_operacion;
	t_buffer* buffer;
} t_paquete;


void enviar_mensaje(char* mensaje, int socket_cliente,t_log* logger);

void recibir_mensaje(int socket_cliente,t_log* logger);

char* recibir_nombre(int socket_cliente,t_log* logger);

void* recibir_buffer(int* size, int socket_cliente);

void crear_buffer(t_paquete* paquete);

void iterator(void* value);

int recibir_operacion(int,t_log* logger);

t_list* recibir_paquete(int);

void* serializar_paquete(t_paquete* paquete, int bytes);

void eliminar_paquete(t_paquete* paquete);

void liberar_conexion(int socket_cliente);

void* serializar_peticion_IO(t_peticion_io* peticion);

void enviar_peticion_IO(t_peticion_io* peticion,int socket_IO);

t_peticion_io recibir_peticion_IO(int socket_cliente);

t_instruc_read recibir_instruc_read(int socket_cliente);

void enviar_instruct_read(t_instruc_read *peticion, int socket_cpu);

void *serializar_instruct_read(t_instruc_read *peticion);

t_instruc_write recibir_instruc_write(int socket_cliente);

void enviar_instruct_write(t_instruc_write *peticion, int socket_cpu);

void *serializar_instruct_write(t_instruc_write *peticion);

void enviar_instruccion_con_int(op_code codigo, int valor, int socket);

int recibir_instruccion_con_int(int socket);

void *serializar_datos_iniciales_cpu(t_hand_inicial_cpu_mem *datos);

void enviar_datos_iniciales_cpu(op_code codigo, t_hand_inicial_cpu_mem datos, int socket);

t_hand_inicial_cpu_mem recibir_datos_iniciales_cpu(int socket);

void* serializar_mmu_solicitud(t_mmu_solicitud* entrada);

void enviar_mmu_solicitud_2(t_mmu_solicitud entrada, int socket, op_code codigo);

t_mmu_solicitud recibir_mmu_solicitud(int socket);

void* serializar_proceso(t_proceso_arch* proceso, int* bytes_totales);

void enviar_proceso(t_proceso_arch* proceso, int socket, op_code codigo_op);

t_proceso_arch recibir_proceso(int socket);

void* serializar_instruc_init_proc(t_instruc_init_proc* instruccion, int* size);

void enviar_instruc_init_proc(t_instruc_init_proc* instruccion, int socket_destino);

t_instruc_init_proc recibir_instruc_init_proc(int socket_origen);

void* serializar_instruct_io(t_instruc_io* instr, int* size);

void enviar_instruct_io(t_instruc_io* instr, int socket);

t_instruc_io recibir_instruct_io(int socket);

void handshake_de_io_kernel(int socket_cliente, t_log* logger, char* nombre);

void* serializar_fin_io(t_fin_io* fin_io, int* size_out);

void enviar_fin_io(t_fin_io* fin_io, int socket);

t_fin_io recibir_fin_io(int socket);

void enviar_pagina_completa(void* pagina, int tam_pagina, int socket_destino);

void enviar_respuesta_tabla_o_marco(int id, int socket_destino);


#endif