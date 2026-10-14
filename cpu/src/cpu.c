#include "cpu.h"
#include <tlb.h>
#include <cache.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <unistd.h>
#include <protocolo.h>
#include <op_code.h>
#include <commons/log.h>
#include <stdio.h>
#include <netdb.h>


extern bool cache_activada;
extern bool tlb_activada;

//todo esto nos lo pasa memoria x hanshake o funcion aparte
extern int TAM_PAGINA;
extern int ENTRADAS_POR_TABLA;
extern int CANT_NIVELES;

void* serializar_pcb(t_pcb* pcb, int* size) {
    int tam = sizeof(int) * 3 + sizeof(estado_proceso_t);
    void* buffer = malloc(tam);
    int offset = 0;

    memcpy(buffer + offset, &(pcb->pid), sizeof(int));
    offset += sizeof(int);
    memcpy(buffer + offset, &(pcb->tamanio), sizeof(int));
    offset += sizeof(int);
    memcpy(buffer + offset, &(pcb->program_counter), sizeof(int));
    offset += sizeof(int);
    memcpy(buffer + offset, &(pcb->estado), sizeof(estado_proceso_t));
    offset += sizeof(estado_proceso_t);

    *size = tam;
    return buffer;
}

t_pcb deserializar_pcb(void* buffer) {
    t_pcb pcb;
    int offset = 0;

    memcpy(&(pcb.pid), buffer + offset, sizeof(int));
    offset += sizeof(int);

    memcpy(&(pcb.tamanio), buffer + offset, sizeof(int));
    offset += sizeof(int);

    memcpy(&(pcb.program_counter), buffer + offset, sizeof(int));
    offset += sizeof(int);

    memcpy(&(pcb.estado), buffer + offset, sizeof(estado_proceso_t));

    return pcb;
}

t_pcb recibir_pcb(int socket_dispatch, t_log* logger){
    int size;

    int cod_op = recibir_operacion(socket_dispatch, logger);

    if (cod_op != EJECUTAR_CONTEXTO) {
        log_error(logger, "Código inesperado recibido en CPU: %d", cod_op);
        t_pcb pcb_error = { .pid = -1, .program_counter = 0, .estado = ESTADO_NEW };
        return pcb_error;
    }

    void* buffer = recibir_buffer(&size, socket_dispatch);

    if (buffer == NULL) {
        log_error(logger, "CPU: Error al recibir buffer del PCB");
        t_pcb pcb_error = { .pid = -1, .program_counter = 0, .estado = ESTADO_NEW };
        return pcb_error;
    }

    t_pcb pcb = deserializar_pcb(buffer);
    free(buffer);

    log_info(logger, "Recibí PCB: PID=%d, PC=%d, ESTADO=%d", pcb.pid, pcb.program_counter, pcb.estado);
    return pcb;
}

void enviar_pcb(t_pcb* pcb, int socket_dispatch, t_log* logger) {
    int size;
    void* buffer = serializar_pcb(pcb, &size); // usamos la función correcta

    t_paquete* paquete = malloc(sizeof(t_paquete));
    paquete->codigo_operacion = DEVOLVER_CONTEXTO;
    paquete->buffer = malloc(sizeof(t_buffer));
    paquete->buffer->size = size;
    paquete->buffer->stream = buffer;

    int total_bytes = size + sizeof(int) * 2;
    void* a_enviar = serializar_paquete(paquete, total_bytes);
    send(socket_dispatch, a_enviar, total_bytes, 0);

    log_info(logger, "CPU → Kernel: PCB PID=%d devuelto con estado %d", pcb->pid, pcb->estado);

    free(a_enviar);
    eliminar_paquete(paquete);
}

int fetch(int socket_memoria, t_log* logger, t_pcb* pcb) {
    log_info(logger, "## PID: %d - FETCH - Program Counter: %d", pcb->pid, pcb->program_counter);

    // Armar el paquete con PID y PC
    t_paquete* paquete = malloc(sizeof(t_paquete));
    paquete->codigo_operacion = FETCH;
    paquete->buffer = malloc(sizeof(t_buffer));
    paquete->buffer->size = sizeof(int) * 2;
    paquete->buffer->stream = malloc(paquete->buffer->size);

    int offset = 0;
    memcpy(paquete->buffer->stream + offset, &(pcb->pid), sizeof(int));
    offset += sizeof(int);
    memcpy(paquete->buffer->stream + offset, &(pcb->program_counter), sizeof(int));

    // Serializar y enviar
    int total_bytes = sizeof(int) * 2 + paquete->buffer->size; // op_code + size + data
    void* a_enviar = serializar_paquete(paquete, total_bytes);
    send(socket_memoria, a_enviar, total_bytes, 0);

    free(a_enviar);
    eliminar_paquete(paquete);

    // Esperar el código de operación de la instrucción que responde Memoria
    int cod_op = recibir_operacion(socket_memoria, logger);
    return cod_op;
}

void decode_and_execute(int cod_op, int socket_mem ,int socket_kernel, t_log* logger, t_pcb* pcb) {
    switch (cod_op) {
        case NOOP: {
            log_info(logger,"1 NOOP");
            
            int pc_nuevo = recibir_instruccion_con_int(socket_mem);
            pcb->program_counter = pcb->program_counter + pc_nuevo;
            usleep(500);
            
            log_info(logger, "## PID: %d - Ejecutando: NOOP ", pcb->pid);
            break;
        }
        case GOTO: {
            log_info(logger,"2 GOTO");
            int pc_nuevo = recibir_instruccion_con_int(socket_mem);
            ejecutar_goto(pc_nuevo,cod_op,logger, pcb);
            break;
        }
        case READ: {
            log_info(logger,"3 READ");
            pcb->program_counter++;
            t_instruc_read readin = recibir_instruc_read(socket_mem);
            ejecutar_read(&readin, logger, pcb, socket_mem);
            break; 
        }
        case WRITE: {
            log_info(logger,"4 WRITE ");
            pcb->program_counter++;
            t_instruc_write writin = recibir_instruc_write(socket_mem);
            ejecutar_write(&writin, logger, pcb, socket_mem);
            break;
        }
        case IO: {
            log_info(logger, "5 IO");

            t_instruc_io instruccion = recibir_instruct_io(socket_mem);

            pcb->estado = ESTADO_BLOCKED;

            // Enviar PCB a Kernel
            pcb->program_counter++;
            enviar_pcb(pcb, socket_kernel, logger);

            // Enviar la instruccion IO tal cual
            enviar_instruct_io(&instruccion, socket_kernel);

            log_info(logger, "## PID: %d - Ejecutando: IO - Dispositivo: %s - Tiempo: %d", pcb->pid, instruccion.dispositivo, instruccion.tiempo);

            free(instruccion.dispositivo); 
            break;
        }
        case INIT_PROC:{
            log_info(logger, "6 INIT_PROC");
            t_instruc_init_proc proc = recibir_instruc_init_proc(socket_mem);
            ejecutar_init_proc(proc, socket_kernel, logger, pcb);
            pcb->program_counter++;
            break;
        }
        case DUMP_MEMORY:{
            log_info(logger, "7 DUMP_MEMORY");       
            int cantidad = recibir_instruccion_con_int(socket_mem); 
            pcb->estado = ESTADO_BLOCKED;    
            pcb->program_counter++;
            ejecutar_dump_memory(logger,cantidad,pcb,socket_kernel);
            break;
        }
        case EXIT:{
            log_info(logger, "8 EXIT");
            int cantidad = recibir_instruccion_con_int(socket_mem);
            ejecutar_exit(logger,cantidad,pcb,socket_kernel, socket_mem);
            break;
        }

    }
}

void ejecutar_dump_memory(t_log* logger, int cantidad, t_pcb* pcb, int socket_kernel) {
    log_info(logger, "## PID: %d - Ejecutando: DUMP_MEMORY", pcb->pid);

    enviar_pcb(pcb,socket_kernel,logger);
    
    int cod_op = DUMP_MEMORY;
    send(socket_kernel, &cod_op, sizeof(int), 0);
}

void ejecutar_exit (t_log* logger,int cantidad, t_pcb* pcb, int socket_kernel, int socket_memoria) {
    log_info(logger, "## PID: %d - Ejecutando: EXIT", pcb->pid );
    
    pcb->estado = ESTADO_EXIT;

    if(cache_activada){
        limpiar_cache_de_proceso(pcb->pid, logger, socket_memoria);
    }

    if(tlb_activada){
        limpiar_tlb_de_proceso(pcb->pid, logger);
    }

    enviar_pcb(pcb,socket_kernel,logger);

    int cod_op = EXIT;
    send(socket_kernel, &cod_op, sizeof(int), 0);
}

void ejecutar_goto(int pc_nuevo,int cod_op ,t_log* logger,t_pcb* pcb){
    pcb->program_counter = pc_nuevo;
    log_info(logger, "## PID: %d - Ejecutando: GOTO - %d ", pcb->pid, pc_nuevo);
    return;
}

void ejecutar_init_proc(t_instruc_init_proc proc, int socket_kernel, t_log* logger, t_pcb* pcb){
    log_info(logger, "## PID: %d - Ejecutando: INIT_PROC - %s - %d", pcb->pid, proc.archivo_pseudo, proc.tamanio);
    enviar_instruc_init_proc(&proc, socket_kernel);
    return;
}

void ejecutar_read(t_instruc_read* readin, t_log* logger, t_pcb* pcb, int socket_memoria){
    
    int direccion_logica = readin->direccion;
    int tamanio = readin->tamanio;

    log_info(logger, "## PID: %d - Ejecutando: READ - %d %d", pcb->pid, direccion_logica, tamanio);

    int numero_pagina = floor(direccion_logica / TAM_PAGINA);
    int desplazamiento = direccion_logica % TAM_PAGINA;

    if(cache_activada){
        // CON CACHE: Solo usar cache, nunca acceso directo a memoria
        bool hit_cache = false;
        void* contenido_cache = leer_de_cache(pcb->pid, numero_pagina, &hit_cache, socket_memoria, logger);

        if(contenido_cache != NULL){
            // Leer desde cache (tanto hit como miss ya cargado)
            char* valor_leido = malloc(tamanio + 1);
            memcpy(valor_leido, (char*)contenido_cache + desplazamiento, tamanio);
            valor_leido[tamanio] = '\0';

            if(hit_cache) {
                log_info(logger, "PID: %d - Valor Leído desde CACHE (HIT): %s", pcb->pid, valor_leido);
            } else {
                log_info(logger, "PID: %d - Valor Leído desde CACHE (MISS->LOADED): %s", pcb->pid, valor_leido);
            }
            
            free(valor_leido);
            return;
        } else {
            log_error(logger, "Error: No se pudo cargar página en cache");
            return;
        }
    } else {
        // SIN CACHE: Acceso directo a memoria
        int direccion_fisica = mmu_traduccion(direccion_logica, logger, pcb, socket_memoria);

        t_paquete* paquete = malloc(sizeof(t_paquete));
        paquete->codigo_operacion = READ;
        paquete->buffer = malloc(sizeof(t_buffer));

        int payload_size = sizeof(int) * 3;
        void* buffer = malloc(payload_size);
        int offset = 0;
        memcpy(buffer + offset, &pcb->pid, sizeof(int));
        offset += sizeof(int);
        memcpy(buffer + offset, &direccion_fisica, sizeof(int));
        offset += sizeof(int);
        memcpy(buffer + offset, &tamanio, sizeof(int));

        paquete->buffer->stream = buffer;
        paquete->buffer->size = payload_size;

        void* a_enviar = serializar_paquete(paquete, payload_size + sizeof(int) * 2);
        send(socket_memoria, a_enviar, payload_size + sizeof(int) * 2, 0);

        log_info(logger, "PID: %d - Acción: LEER - Dirección Física: %d", pcb->pid, direccion_fisica);

        // Recibir dato de memoria
        int cod_op = recibir_operacion(socket_memoria, logger);
        if (cod_op == RESPUESTA_OK){
            int size;
            void* buffer_recibido = recibir_buffer(&size, socket_memoria);
            
            char* valor = malloc(tamanio + 1);
            memcpy(valor, buffer_recibido, tamanio);
            valor[tamanio] = '\0';
            
            log_info(logger, "PID: %d - Valor Leído desde MEMORIA: %s", pcb->pid, valor);
            
            free(buffer_recibido);
            free(valor);
        }
        
        free(a_enviar);
        eliminar_paquete(paquete);
    }
}

void ejecutar_write(t_instruc_write* writin, t_log* logger, t_pcb* pcb, int socket_memoria) {
    int direccion_logica = writin->direccion;
    char* datos = writin->datos;

    log_info(logger, "## PID: %d - Ejecutando: WRITE - Dirección: %d - Datos: %s", pcb->pid, direccion_logica, datos);

    int nro_pagina = direccion_logica / TAM_PAGINA;
    int desplazamiento = direccion_logica % TAM_PAGINA;

    if (cache_activada) {
        // CON CACHE: Solo usar cache, nunca acceso directo a memoria
        bool hit = false;
        void* contenido = leer_de_cache(pcb->pid, nro_pagina, &hit, socket_memoria, logger);

        if (contenido != NULL) {
            // Escribir en el cache
            memcpy((char*)contenido + desplazamiento, datos, strlen(datos));
            
            // Marcar entrada como modificada
            t_entrada_cache* entrada = buscar_entrada_cache(pcb->pid, nro_pagina);
            if(entrada) {
                entrada->modificado = true;
                entrada->uso = true;
            }
            
            if(hit) {
                log_info(logger, "PID: %d - WRITE en caché (HIT) - Página: %d - Offset: %d - Dato: %s", pcb->pid, nro_pagina, desplazamiento, datos);
            } else {
                log_info(logger, "PID: %d - WRITE en caché (MISS->LOADED) - Página: %d - Offset: %d - Dato: %s", pcb->pid, nro_pagina, desplazamiento, datos);
            }
        } else {
            log_error(logger, "Error: No se pudo cargar página en cache para WRITE");
        }
    } else {
        // SIN CACHE: Acceso directo a memoria
        int direccion_fisica = mmu_traduccion(direccion_logica, logger, pcb, socket_memoria);

        int size_datos = strlen(datos);
        int payload_size = sizeof(int) * 2 + size_datos;
        void* buffer = malloc(payload_size);

        int offset = 0;
        memcpy(buffer + offset, &pcb->pid, sizeof(int));
        offset += sizeof(int);
        memcpy(buffer + offset, &direccion_fisica, sizeof(int));
        offset += sizeof(int);
        memcpy(buffer + offset, datos, size_datos);

        t_paquete* paquete = malloc(sizeof(t_paquete));
        paquete->codigo_operacion = WRITE;
        paquete->buffer = malloc(sizeof(t_buffer));
        paquete->buffer->stream = buffer;
        paquete->buffer->size = payload_size;

        void* a_enviar = serializar_paquete(paquete, payload_size + sizeof(int)*2);
        send(socket_memoria, a_enviar, payload_size + sizeof(int)*2, 0);

        log_info(logger, "PID: %d - Acción: ESCRIBIR - Dirección Física: %d - Datos: %s", pcb->pid, direccion_fisica, datos);

        // Recibir confirmación de Memoria
        int cod_op = recibir_operacion(socket_memoria, logger);
        if (cod_op == RESPUESTA_OK){
            log_info(logger, "PID: %d - Respuesta de Memoria: RESPUESTA_OK", pcb->pid);
        } 

        free(a_enviar);
        eliminar_paquete(paquete);
    }
}

int mmu_traduccion(int direccion_logica, t_log* logger, t_pcb* pcb, int socket_memoria) {
    int nro_pagina = floor(direccion_logica / TAM_PAGINA);
    int desplazamiento = direccion_logica % TAM_PAGINA;

    if (tlb_activada) {
        bool hit = false;
        int marco = consultar_tlb(pcb->pid, nro_pagina, logger, &hit);
        if (hit) {
            return marco * TAM_PAGINA + desplazamiento;
        }
    }

    enviar_mmu_solicitud(pcb->pid, direccion_logica, socket_memoria);
    int marco = recibir_marco(socket_memoria);

    if (tlb_activada && marco != -1) {
        agregar_a_tlb(pcb->pid, nro_pagina, marco, logger);
    }

    log_info(logger, "PID: %d - OBTENER MARCO - Página: %d - Marco: %d", pcb->pid, nro_pagina, marco);
    return marco * TAM_PAGINA + desplazamiento;
}

void enviar_mmu_solicitud(int pid, int direccion_logica, int socket_memoria) {
    t_mmu_solicitud solicitud;
    solicitud.pid = pid;
    solicitud.direccion_logica = direccion_logica;

    int size = sizeof(int) * 2;
    void* stream = malloc(size);
    memcpy(stream, &solicitud.pid, sizeof(int));
    memcpy(stream + sizeof(int), &solicitud.direccion_logica, sizeof(int));

    t_paquete* paquete = malloc(sizeof(t_paquete));
    paquete->codigo_operacion = OBTENER_MARCO_FINAL;
    paquete->buffer = malloc(sizeof(t_buffer));
    paquete->buffer->size = size;
    paquete->buffer->stream = stream;

    void* a_enviar = serializar_paquete(paquete, size + sizeof(int) * 2);
    send(socket_memoria, a_enviar, size + sizeof(int) * 2, 0);

    free(a_enviar);
    eliminar_paquete(paquete);
}

int recibir_marco(int socket_memoria) {
    int cod_op = recibir_operacion(socket_memoria, NULL); // puede usar logger
    if (cod_op != MARCO_O_TABLA) return -1;

    return recibir_instruccion_con_int(socket_memoria);
}

void handshake_de_cpu_memoria(int socket_memoria, t_log* logger){
    int op = HANDSHAKE_CPU;
    send(socket_memoria, &op, sizeof(int), 0);

    // Esperar la respuesta
    int cod_op = recibir_operacion(socket_memoria, logger);
    if (cod_op != HANDSHAKE_RESPUESTA) {
        log_error(logger, "No recibí handshake de Memoria");
        exit(EXIT_FAILURE);
    }

    t_hand_inicial_cpu_mem variables = recibir_datos_iniciales_cpu(socket_memoria);

    TAM_PAGINA = variables.tamanio_pag;
    CANT_NIVELES = variables.cant_niveles;
    ENTRADAS_POR_TABLA = variables.entradas_por_tabla;

    log_info(logger, "TAM_PAGINA : %d | CANT_NIVELES : %d | ENTRADAS_POR_TABLA : %d", TAM_PAGINA,CANT_NIVELES,ENTRADAS_POR_TABLA);
}

void handshake_de_cpu_kernel(int socket_cliente, t_log* logger, char* identificador){
    t_paquete *paquete = malloc(sizeof(t_paquete));

	paquete->codigo_operacion = HANDSHAKE_CPU;
	paquete->buffer = malloc(sizeof(t_buffer));
	paquete->buffer->size = strlen(identificador) + 1;
	paquete->buffer->stream = malloc(paquete->buffer->size);
	memcpy(paquete->buffer->stream, identificador, paquete->buffer->size);

	int bytes = paquete->buffer->size + 2 * sizeof(int);

	void *a_enviar = serializar_paquete(paquete, bytes);

	send(socket_cliente, a_enviar, bytes, 0);

	free(a_enviar);
	eliminar_paquete(paquete);
}

void escribir_pagina_completa(int pid, int pagina, void* datos, int socket_memoria, t_log* logger) {
    int direccion_logica = pagina * TAM_PAGINA;
    int desplazamiento = direccion_logica % TAM_PAGINA;

    int direccion_fisica = mmu_traduccion(direccion_logica, logger, &(t_pcb){ .pid = pid }, socket_memoria);

    int marco;
    marco = (direccion_fisica - desplazamiento) / TAM_PAGINA;

    int size_datos = TAM_PAGINA;

    int payload_size = sizeof(int) + sizeof(int) + size_datos;
    void* buffer = malloc(payload_size);

    int offset = 0;
    memcpy(buffer + offset, &pid, sizeof(int));
    offset += sizeof(int);
    memcpy(buffer + offset, &direccion_fisica, sizeof(int));
    offset += sizeof(int);
    memcpy(buffer + offset, datos, size_datos);

    // Armar paquete
    t_paquete* paquete = malloc(sizeof(t_paquete));
    paquete->codigo_operacion = ESCRIBIR_PAGINA_COMPLETA;
    paquete->buffer = malloc(sizeof(t_buffer));
    paquete->buffer->stream = buffer;
    paquete->buffer->size = payload_size;

    void* a_enviar = serializar_paquete(paquete, payload_size + sizeof(int) * 2);
    send(socket_memoria, a_enviar, payload_size + sizeof(int) * 2, 0);

    log_info(logger, "CACHE ➜ MEMORIA - Escritura final - PID: %d, Página: %d, Dirección Física: %d", pid, pagina, direccion_fisica);

    // Esperar confirmación
    int cod_op = recibir_operacion(socket_memoria, logger);
    if (cod_op == RESPUESTA_OK) {
        log_info(logger, "PID: %d - Respuesta de Memoria con ESCRITURA COMPLETA DE PAGINA: RESPUESTA_OK", pid);
        log_info(logger, "PID: %d - Memory Update - Pagina: %d - Frame: %d ", pid, pagina, marco);
        log_info(logger, "PID: %d - Respuesta de Memoria: RESPUESTA_OK", pid);
    }

    free(a_enviar);
    eliminar_paquete(paquete);
}


void* solicitar_pagina_completa(int pid, int direccion_fisica_base, int socket_memoria, t_log* logger) {
    t_paquete* paquete = malloc(sizeof(t_paquete));
    paquete->codigo_operacion = LEER_PAGINA_COMPLETA;
    paquete->buffer = malloc(sizeof(t_buffer));
    
    int payload_size = sizeof(int) * 2;
    void* buffer = malloc(payload_size);
    
    int offset = 0;
    memcpy(buffer + offset, &pid, sizeof(int));
    offset += sizeof(int);
    memcpy(buffer + offset, &direccion_fisica_base, sizeof(int));
    
    paquete->buffer->stream = buffer;
    paquete->buffer->size = payload_size;
    
    void* a_enviar = serializar_paquete(paquete, payload_size + sizeof(int) * 2);
    send(socket_memoria, a_enviar, payload_size + sizeof(int) * 2, 0);
    
    log_info(logger, "Solicitando página completa - PID: %d - Dir. Física Base: %d", pid, direccion_fisica_base);
    
    // Recibir página completa
    int cod_op = recibir_operacion(socket_memoria, logger);
    void* contenido_pagina = NULL;
    
    if (cod_op == LEER_PAGINA_COMPLETA) {
        int size_pagina;
        contenido_pagina = recibir_buffer(&size_pagina, socket_memoria);
        log_info(logger, "Página completa recibida desde Memoria");
    } else {
        log_error(logger, "Error: Se esperaba LEER_PAGINA_COMPLETA");
    }
    
    free(a_enviar);
    eliminar_paquete(paquete);
    
    return contenido_pagina;
}