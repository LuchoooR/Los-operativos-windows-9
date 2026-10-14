#include "protocolo.h"
#include <commons/log.h>
#include <commons/collections/list.h>
#include <stdio.h>
#include <netdb.h>

void enviar_mensaje(char *mensaje, int socket_cliente, t_log *logger)
{
	t_paquete *paquete = malloc(sizeof(t_paquete));

	paquete->codigo_operacion = MENSAJE;
	paquete->buffer = malloc(sizeof(t_buffer));
	paquete->buffer->size = strlen(mensaje) + 1;
	paquete->buffer->stream = malloc(paquete->buffer->size);
	memcpy(paquete->buffer->stream, mensaje, paquete->buffer->size);

	int bytes = paquete->buffer->size + 2 * sizeof(int);

	void *a_enviar = serializar_paquete(paquete, bytes);

	send(socket_cliente, a_enviar, bytes, 0);

	free(a_enviar);
	eliminar_paquete(paquete);
}

void recibir_mensaje(int socket_cliente, t_log *logger)
{
	int size;
	char *buffer = recibir_buffer(&size, socket_cliente);
	log_info(logger, "Me llego el mensaje %s", buffer);
	free(buffer);
}

char* recibir_nombre(int socket_cliente, t_log *logger)
{
	int size;
	char *buffer = recibir_buffer(&size, socket_cliente);
	return buffer;
}

void *recibir_buffer(int *size, int socket_cliente)
{
	void *buffer;

	recv(socket_cliente, size, sizeof(int), MSG_WAITALL);
	buffer = malloc(*size);
	recv(socket_cliente, buffer, *size, MSG_WAITALL);

	return buffer;
}

void crear_buffer(t_paquete *paquete)
{
	paquete->buffer = malloc(sizeof(t_buffer));
	paquete->buffer->size = 0;
	paquete->buffer->stream = NULL;
}

int recibir_operacion(int socket_cliente, t_log *logger)
{
	int cod_op;
	int bytes_recibidos = recv(socket_cliente, &cod_op, sizeof(int), MSG_WAITALL);
	if (bytes_recibidos > 0)
	{
		return cod_op;
	}
	else
	{
		log_error(logger, "Error al recibir código de operación");
		close(socket_cliente);
		return -1;
	}
}

t_list *recibir_paquete(int socket_cliente)
{
	int size;
	int desplazamiento = 0;
	void *buffer;
	t_list *valores = list_create();
	int tamanio;

	buffer = recibir_buffer(&size, socket_cliente);
	while (desplazamiento < size)
	{
		memcpy(&tamanio, buffer + desplazamiento, sizeof(int));
		desplazamiento += sizeof(int);
		char *valor = malloc(tamanio);
		memcpy(valor, buffer + desplazamiento, tamanio);
		desplazamiento += tamanio;
		list_add(valores, valor);
	}
	free(buffer);
	return valores;
}

void *serializar_paquete(t_paquete *paquete, int bytes)
{
	void *magic = malloc(bytes);
	int desplazamiento = 0;

	memcpy(magic + desplazamiento, &(paquete->codigo_operacion), sizeof(int));
	desplazamiento += sizeof(int);
	memcpy(magic + desplazamiento, &(paquete->buffer->size), sizeof(int));
	desplazamiento += sizeof(int);
	memcpy(magic + desplazamiento, paquete->buffer->stream, paquete->buffer->size);
	desplazamiento += paquete->buffer->size;

	return magic;
}

void eliminar_paquete(t_paquete *paquete)
{
	free(paquete->buffer->stream);
	free(paquete->buffer);
	free(paquete);
}

void liberar_conexion(int socket_cliente)
{
	close(socket_cliente);
}

void *serializar_peticion_IO(t_peticion_io *peticion)
{											// pido el puntero a t_peticion porq solo quiero leer los datos no me importa la estructura en si (son menos bytes)
	void *buffer = malloc(sizeof(int) * 2); // pid(int) + tiempo(int)
	int desplazamiento = 0;

	memcpy(buffer + desplazamiento, &(peticion->pid), sizeof(int));
	desplazamiento += sizeof(int);
	memcpy(buffer + desplazamiento, &(peticion->tiempo_io), sizeof(int));
	desplazamiento += sizeof(int); // copio los valores de los campos en el buffer

	return buffer;
}

void enviar_peticion_IO(t_peticion_io *peticion, int socket_IO)
{
	void *peticion_serializada = serializar_peticion_IO(peticion);

	t_paquete *paquete = malloc(sizeof(t_paquete));
	paquete->codigo_operacion = SOLICITUD_IO;
	paquete->buffer = malloc(sizeof(t_buffer));		// inicializo el buffer
	paquete->buffer->size = sizeof(int) * 2;		// tamanio que necesito para la peticion serializada
	paquete->buffer->stream = peticion_serializada; // cargo la peticion

	int size_total = sizeof(int) * 4; // tengo op_code,size,pid,tiempo (4 ints)
	void *paquete_serializado = serializar_paquete(paquete, size_total);
	send(socket_IO, paquete_serializado, size_total, 0);

	free(paquete_serializado);
	eliminar_paquete(paquete);
}

t_peticion_io recibir_peticion_IO(int socket_cliente)
{
	int tamanio;
	void *buffer = recibir_buffer(&tamanio, socket_cliente);

	t_peticion_io peticion;
	int desplazamiento = 0;

	memcpy(&(peticion.pid), buffer + desplazamiento, sizeof(int));
	desplazamiento += sizeof(int);
	memcpy(&(peticion.tiempo_io), buffer + desplazamiento, sizeof(int)); // copio los valores que trae el buffer al nuevo t_peticion_io

	free(buffer);
	return peticion;
}

t_instruc_read recibir_instruc_read(int socket_cliente)
{
	int tamanio;
	void *buffer = recibir_buffer(&tamanio, socket_cliente);

	t_instruc_read peticion;
	int desplazamiento = 0;

	memcpy(&(peticion.direccion), buffer + desplazamiento, sizeof(int));
	desplazamiento += sizeof(int);
	memcpy(&(peticion.tamanio), buffer + desplazamiento, sizeof(int)); // copio los valores que trae el buffer al nuevo t_instruc_read

	free(buffer);
	return peticion;
}

void enviar_instruct_read(t_instruc_read *peticion, int socket_cpu)
{
	void *instruct_serializada = serializar_instruct_read(peticion);

	t_paquete *paquete = malloc(sizeof(t_paquete));
	paquete->codigo_operacion = READ;
	paquete->buffer = malloc(sizeof(t_buffer));		// inicializo el buffer
	paquete->buffer->size = sizeof(int) * 2;		// tamanio que necesito para la peticion serializada
	paquete->buffer->stream = instruct_serializada; // cargo la peticion

	int size_total = sizeof(int) * 4; // tengo op_code,size,pid,tiempo (4 ints)
	void *paquete_serializado = serializar_paquete(paquete, size_total);
	send(socket_cpu, paquete_serializado, size_total, 0);

	free(paquete_serializado);
	free(instruct_serializada);
	eliminar_paquete(paquete);
} // si no funca, comenta free(paquete_serializado);

void *serializar_instruct_read(t_instruc_read *peticion)
{											
	void *buffer = malloc(sizeof(int) * 2); // direc(int) + tamanio(int)
	int desplazamiento = 0;

	memcpy(buffer + desplazamiento, &(peticion->direccion), sizeof(int));
	desplazamiento += sizeof(int);
	memcpy(buffer + desplazamiento, &(peticion->tamanio), sizeof(int));
	desplazamiento += sizeof(int); // copio los valores de los campos en el buffer

	return buffer;
}

t_instruc_write recibir_instruc_write(int socket_cliente) {
    int tamanio;
    void* buffer = recibir_buffer(&tamanio, socket_cliente);

    t_instruc_write peticion;
    int desplazamiento = 0;
	int longitud = tamanio - sizeof(int);

    memcpy(&(peticion.direccion), buffer + desplazamiento, sizeof(int));
    desplazamiento += sizeof(int);

    peticion.datos = malloc(longitud);
    memcpy(peticion.datos, buffer + desplazamiento, longitud);

    free(buffer);
    return peticion;
}

void enviar_instruct_write(t_instruc_write* peticion, int socket_cpu) {
    void* instruct_serializada = serializar_instruct_write(peticion);
    int longitud = strlen(peticion->datos) + 1;
    int size_total = sizeof(int) + longitud;

    t_paquete* paquete = malloc(sizeof(t_paquete));
    paquete->codigo_operacion = WRITE;
    paquete->buffer = malloc(sizeof(t_buffer));
    paquete->buffer->size = size_total;
    paquete->buffer->stream = instruct_serializada;

    int paquete_size = sizeof(int) * 2 + size_total; // op_code + buffer.size + payload
    void* paquete_serializado = serializar_paquete(paquete, paquete_size);
    send(socket_cpu, paquete_serializado, paquete_size, 0);

    free(paquete_serializado);
    eliminar_paquete(paquete);
} // si no funca, comenta free(paquete_serializado);


void* serializar_instruct_write(t_instruc_write* peticion) {
    int longitud = strlen(peticion->datos) + 1; 
    int size_total = sizeof(int) + longitud;

    void* buffer = malloc(size_total);
    int desplazamiento = 0;

    memcpy(buffer + desplazamiento, &(peticion->direccion), sizeof(int));
    desplazamiento += sizeof(int);
    memcpy(buffer + desplazamiento, peticion->datos, longitud);

    return buffer;
}


void enviar_instruccion_con_int(op_code codigo, int valor, int socket) 
{
    void* stream = malloc(sizeof(int));
    memcpy(stream, &valor, sizeof(int));

    t_paquete* paquete = malloc(sizeof(t_paquete));
    paquete->codigo_operacion = codigo;
    paquete->buffer = malloc(sizeof(t_buffer));
    paquete->buffer->size = sizeof(int);
    paquete->buffer->stream = stream;

    int size_total = sizeof(op_code) + sizeof(int) + sizeof(int); // op_code + buffer.size + int
    void* paquete_serializado = serializar_paquete(paquete, size_total);

    send(socket, paquete_serializado, size_total, 0);

    free(paquete_serializado);
    eliminar_paquete(paquete);
} // si no funca, comenta free(paquete_serializado);

int recibir_instruccion_con_int(int socket) 
{
    int size;
    void* buffer = recibir_buffer(&size, socket);

    int valor;
    memcpy(&valor, buffer, sizeof(int));

    free(buffer);
    return valor;
}



void *serializar_datos_iniciales_cpu(t_hand_inicial_cpu_mem *datos) {
    void *buffer = malloc(sizeof(int) * 3);
    int offset = 0;

    memcpy(buffer + offset, &(datos->tamanio_pag), sizeof(int));
    offset += sizeof(int);
    memcpy(buffer + offset, &(datos->entradas_por_tabla), sizeof(int));
    offset += sizeof(int);
    memcpy(buffer + offset, &(datos->cant_niveles), sizeof(int));

    return buffer;
}


void enviar_datos_iniciales_cpu(op_code codigo, t_hand_inicial_cpu_mem datos, int socket) {
    void* datos_serializados = serializar_datos_iniciales_cpu(&datos);

    t_paquete* paquete = malloc(sizeof(t_paquete));
    paquete->codigo_operacion = codigo;
    paquete->buffer = malloc(sizeof(t_buffer));
    paquete->buffer->size = sizeof(int) * 3;
    paquete->buffer->stream = datos_serializados;

    int total_size = sizeof(int) * 5; // op_code + buffer.size + 3 ints
    void* a_enviar = serializar_paquete(paquete, total_size);
    send(socket, a_enviar, total_size, 0);

    free(a_enviar);
    //free(datos_serializados);
    eliminar_paquete(paquete);
}

t_hand_inicial_cpu_mem recibir_datos_iniciales_cpu(int socket) {
    int size;
    void* buffer = recibir_buffer(&size, socket);

    t_hand_inicial_cpu_mem datos;
    int offset = 0;

    memcpy(&datos.tamanio_pag, buffer + offset, sizeof(int));
    offset += sizeof(int);
    memcpy(&datos.entradas_por_tabla, buffer + offset, sizeof(int));
    offset += sizeof(int);
    memcpy(&datos.cant_niveles, buffer + offset, sizeof(int));

    free(buffer);

    return datos; 
}


void* serializar_mmu_solicitud(t_mmu_solicitud* entrada) {
    void* buffer = malloc(sizeof(int) * 2);
    int offset = 0;

    memcpy(buffer + offset, &(entrada->pid), sizeof(int));
    offset += sizeof(int);
    memcpy(buffer + offset, &(entrada->direccion_logica), sizeof(int));

    return buffer;
}

void enviar_mmu_solicitud_2(t_mmu_solicitud entrada, int socket, op_code codigo) {
    void* serializado = serializar_mmu_solicitud(&entrada);

    t_paquete* paquete = malloc(sizeof(t_paquete));
    paquete->codigo_operacion = codigo;
    paquete->buffer = malloc(sizeof(t_buffer));
    paquete->buffer->size = sizeof(int) * 2;
    paquete->buffer->stream = serializado;

    int total = sizeof(int) * 4; // cod_op + size + dos ints
    void* a_enviar = serializar_paquete(paquete, total);
    send(socket, a_enviar, total, 0);

    free(a_enviar);
    free(serializado);
    eliminar_paquete(paquete);
}

t_mmu_solicitud recibir_mmu_solicitud(int socket) {
    int size;
    void* buffer = recibir_buffer(&size, socket);

    t_mmu_solicitud entrada;
    int offset = 0;

    memcpy(&(entrada.pid), buffer + offset, sizeof(int));  // PRIMERO PID o tabla
    offset += sizeof(int);
    memcpy(&(entrada.direccion_logica), buffer + offset, sizeof(int));         // DESPUÉS la entrada

    free(buffer);
    return entrada;
}

void* serializar_proceso(t_proceso_arch* proceso, int* bytes_totales){
    int longitud_arch = strlen(proceso->arch) + 1;
    *bytes_totales = sizeof(int) * 2 + sizeof(int) + longitud_arch;

    void* buffer = malloc(*bytes_totales);
    int offset = 0;

    memcpy(buffer + offset, &(proceso->pid), sizeof(int));
    offset += sizeof(int);

    memcpy(buffer + offset, &(proceso->tamanio), sizeof(int));
    offset += sizeof(int);

    memcpy(buffer + offset, &longitud_arch, sizeof(int));
    offset += sizeof(int);

    memcpy(buffer + offset, proceso->arch, longitud_arch);

    return buffer;
}

void enviar_proceso(t_proceso_arch* proceso, int socket, op_code codigo_op){
    int size;
    void* serializado = serializar_proceso(proceso, &size);

    t_paquete* paquete = malloc(sizeof(t_paquete));
    paquete->codigo_operacion = codigo_op;
    paquete->buffer = malloc(sizeof(t_buffer));
    paquete->buffer->size = size;
    paquete->buffer->stream = serializado;

    int total_size = size + sizeof(int) * 2;
    void* a_enviar = serializar_paquete(paquete, total_size);

    send(socket, a_enviar, total_size, 0);

    free(a_enviar);
    eliminar_paquete(paquete);
}

t_proceso_arch recibir_proceso(int socket){
    int size;
    void* buffer = recibir_buffer(&size, socket);

    t_proceso_arch proceso;
    int offset = 0;
    memcpy(&(proceso.pid), buffer + offset, sizeof(int));
    offset += sizeof(int);
    memcpy(&(proceso.tamanio), buffer + offset, sizeof(int));
    offset += sizeof(int);
    int longitud_arch;
    memcpy(&longitud_arch, buffer + offset, sizeof(int));
    offset += sizeof(int);
    proceso.arch = malloc(longitud_arch);
    memcpy(proceso.arch, buffer + offset, longitud_arch);

    free(buffer);
    return proceso;
}

void* serializar_instruc_init_proc(t_instruc_init_proc* instruccion, int* size){
    int longitud_archivo = strlen(instruccion->archivo_pseudo) + 1;
    *size = sizeof(int) + longitud_archivo + sizeof(int);

    void* buffer = malloc(*size);
    int offset = 0;

    memcpy(buffer + offset, &longitud_archivo, sizeof(int));
    offset += sizeof(int);

    memcpy(buffer + offset, instruccion->archivo_pseudo, longitud_archivo);
    offset += longitud_archivo;

    memcpy(buffer + offset, &(instruccion->tamanio), sizeof(int));

    return buffer;
}

void enviar_instruc_init_proc(t_instruc_init_proc* instruccion, int socket_destino){
    int size;
    void* stream = serializar_instruc_init_proc(instruccion, &size);

    t_paquete* paquete = malloc(sizeof(t_paquete));
    paquete->codigo_operacion = INIT_PROC;
    paquete->buffer = malloc(sizeof(t_buffer));
    paquete->buffer->size = size;
    paquete->buffer->stream = stream;

    int total_bytes = size + sizeof(int) * 2;
    void* a_enviar = serializar_paquete(paquete, total_bytes);
    send(socket_destino, a_enviar, total_bytes, 0);

    free(a_enviar);
    eliminar_paquete(paquete);
}

t_instruc_init_proc recibir_instruc_init_proc(int socket_origen){
    int size;
    void* buffer = recibir_buffer(&size, socket_origen);

    t_instruc_init_proc instruccion;
    int offset = 0;

    int longitud_archivo;
    memcpy(&longitud_archivo, buffer + offset, sizeof(int));
    offset += sizeof(int);

    instruccion.archivo_pseudo = malloc(longitud_archivo);
    memcpy(instruccion.archivo_pseudo, buffer + offset, longitud_archivo);
    offset += longitud_archivo;

    memcpy(&(instruccion.tamanio), buffer + offset, sizeof(int));

    free(buffer);
    return instruccion;
}


void* serializar_instruct_io(t_instruc_io* instr, int* size) {
    int longitud_dispositivo = strlen(instr->dispositivo) + 1;
    *size = sizeof(int) + longitud_dispositivo + sizeof(int);

    void* buffer = malloc(*size);
    int offset = 0;

    memcpy(buffer + offset, &longitud_dispositivo, sizeof(int));
    offset += sizeof(int);

    memcpy(buffer + offset, instr->dispositivo, longitud_dispositivo);
    offset += longitud_dispositivo;

    memcpy(buffer + offset, &(instr->tiempo), sizeof(int));

    return buffer;
}

void enviar_instruct_io(t_instruc_io* instruccion, int socket_destino) {
    int size;
    void* buffer_serializado = serializar_instruct_io(instruccion, &size);

    t_paquete* paquete = malloc(sizeof(t_paquete));
    paquete->codigo_operacion = IO;
    paquete->buffer = malloc(sizeof(t_buffer));
    paquete->buffer->size = size;
    paquete->buffer->stream = buffer_serializado;

    void* a_enviar = serializar_paquete(paquete, size + sizeof(int) * 2);
    send(socket_destino, a_enviar, size + sizeof(int) * 2, 0);

    free(a_enviar);
    eliminar_paquete(paquete);
}

t_instruc_io recibir_instruct_io(int socket_cliente) {
    int size;
    void* buffer = recibir_buffer(&size, socket_cliente);

    t_instruc_io instruccion;
    int offset = 0;

    // 1. Recibir longitud del dispositivo
    int longitud_dispositivo;
    memcpy(&longitud_dispositivo, buffer + offset, sizeof(int));
    offset += sizeof(int);

    // 2. Recibir string del dispositivo
    instruccion.dispositivo = malloc(longitud_dispositivo);
    memcpy(instruccion.dispositivo, buffer + offset, longitud_dispositivo);
    offset += longitud_dispositivo;

    // 3. Recibir tiempo
    memcpy(&(instruccion.tiempo), buffer + offset, sizeof(int));

    free(buffer);
    return instruccion;
}

void handshake_de_io_kernel(int socket_cliente, t_log* logger, char* nombre){
    t_paquete *paquete = malloc(sizeof(t_paquete));

	paquete->codigo_operacion = HANDSHAKE_IO;
	paquete->buffer = malloc(sizeof(t_buffer));
	paquete->buffer->size = strlen(nombre) + 1;
	paquete->buffer->stream = malloc(paquete->buffer->size);
	memcpy(paquete->buffer->stream, nombre, paquete->buffer->size);

	int bytes = paquete->buffer->size + 2 * sizeof(int);

	void *a_enviar = serializar_paquete(paquete, bytes);

	send(socket_cliente, a_enviar, bytes, 0);

	free(a_enviar);
	eliminar_paquete(paquete);
}

void* serializar_fin_io(t_fin_io* fin_io, int* size_out) {
    int len_dispositivo = strlen(fin_io->dispositivo) + 1;
    int size_total = sizeof(int) + len_dispositivo + sizeof(int); // len + string + pid

    void* buffer = malloc(size_total);
    int offset = 0;

    memcpy(buffer + offset, &len_dispositivo, sizeof(int));
    offset += sizeof(int);

    memcpy(buffer + offset, fin_io->dispositivo, len_dispositivo);
    offset += len_dispositivo;

    memcpy(buffer + offset, &(fin_io->pid), sizeof(int));
    offset += sizeof(int);

    *size_out = size_total;
    return buffer;
}

void enviar_fin_io(t_fin_io* fin_io, int socket) {
    int size;
    void* stream = serializar_fin_io(fin_io, &size);

    t_paquete* paquete = malloc(sizeof(t_paquete));
    paquete->codigo_operacion = FIN_IO;
    paquete->buffer = malloc(sizeof(t_buffer));
    paquete->buffer->size = size;
    paquete->buffer->stream = stream;

    int total_size = sizeof(int) * 2 + size;
    void* a_enviar = serializar_paquete(paquete, total_size);

    send(socket, a_enviar, total_size, 0);

    free(a_enviar);
    eliminar_paquete(paquete);
}

t_fin_io recibir_fin_io(int socket) {
    int size;
    void* buffer = recibir_buffer(&size, socket);

    t_fin_io fin_io;
    int offset = 0;
    int len_dispositivo;

    memcpy(&len_dispositivo, buffer + offset, sizeof(int));
    offset += sizeof(int);

    fin_io.dispositivo = malloc(len_dispositivo);
    memcpy(fin_io.dispositivo, buffer + offset, len_dispositivo);
    offset += len_dispositivo;

    memcpy(&(fin_io.pid), buffer + offset, sizeof(int));

    free(buffer);
    return fin_io;
}


void enviar_pagina_completa(void* pagina, int tam_pagina, int socket_destino) {
    t_paquete* paquete = malloc(sizeof(t_paquete));
    paquete->codigo_operacion = LEER_PAGINA_COMPLETA;

    paquete->buffer = malloc(sizeof(t_buffer));
    paquete->buffer->size = tam_pagina;
    paquete->buffer->stream = malloc(tam_pagina);
    memcpy(paquete->buffer->stream, pagina, tam_pagina);

    int total = sizeof(int) + sizeof(int) + tam_pagina; // op_code + size + stream
    void* serializado = serializar_paquete(paquete, total);
    send(socket_destino, serializado, total, 0);

    free(serializado);
    eliminar_paquete(paquete);
}



void enviar_respuesta_tabla_o_marco(int id, int socket) {
    enviar_instruccion_con_int(OBTENER_TABLA_O_MARCO, id, socket);
}

   