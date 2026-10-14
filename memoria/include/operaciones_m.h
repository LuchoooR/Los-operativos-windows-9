#ifndef OPERACIONES_M_H
#define OPERACIONES_M_H

#include <pthread.h>
#include <commons/log.h>
#include <commons/config.h>
#include <commons/collections/list.h>
#include <stdint.h>
#include <protocolo.h>
#include <op_code.h>
#include <socket_utils.h>
#include <sys/time.h> // para gettimeofday

// === ESTRUCTURAS ===
typedef struct {
    int acc_tp;
    int inst_fetch;
    int lecturas;
    int escrituras;
    int swap_in;
    int swap_out;
} t_metricas;

typedef struct {
    int marco;
    bool presente;
    bool modificada;
    bool uso;
} t_pagina;

typedef struct {
    t_list* entradas;        
    bool es_nivel_final;
} t_tabla_nivel;

typedef struct {
    int pid;
    int tamanio;
    t_tabla_nivel* tabla_1er_nivel;
    t_metricas* metricas;
} t_proceso;

typedef struct {
    int pid;
    t_list* instrucciones; // lista de t_instruccion*
} t_script;

typedef struct {
    int pid;
    int nro_pagina;
    int offset;
} t_entrada_swap; // Para manejo de paginas en swap

// === FUNCIONES ===
// Inicialización
void manejar_iniciar_proceso(int socket_cliente, t_log* logger);
t_proceso* buscar_proceso(int pid);
t_tabla_nivel* crear_tabla(int nivel_actual, int nivel_final, int entradas_por_tabla);
t_pagina* obtener_pagina(t_proceso* proceso, int nro_pagina);
int buscar_marco_libre();

// Memoria
void manejar_read(int socket_cliente, t_log* logger);
void manejar_write(int socket_cliente, t_log* logger);

// Handshake
void handshake_memoria(t_log* logger, int socket_cliente, int tam_pagina, int entradas_por_tabla, int cant_niveles);

// Procesos
void manejar_fetch(int socket_cliente, t_log* logger);

void suspender_proceso(int socket_cliente, t_log* logger);
void suspender_proceso_protegido(int socket_cliente, t_log* logger);
void desuspender_proceso(int socket_cliente, t_log* logger);
void loggear_metricas_proceso(t_proceso* proceso, t_log* logger);
void finalizar_proceso(int socket_cliente, t_log* logger);
bool proceso_con_pid(void* elem, void* pid_ptr);
void destruir_proceso(void* p);

// Gestion de tablas de pagina mulitinivel 
void dump_paginas(FILE* dump, t_tabla_nivel* tabla, int nivel_actual);
void dump_paginas_protegido(FILE* dump, t_tabla_nivel* tabla, int nivel_actual);
void liberar_tabla_multinivel(t_tabla_nivel* tabla); 
void liberar_marcos_asignados(t_proceso* proceso);


// Dump
void dump_memoria(int socket_cliente, t_log* logger);

// Parsing
t_instruccion* parsear_linea(char* linea, t_log* logger);

// Handlers de operaciones
void manejar_leer_pagina_completa(int socket_cliente, t_log* logger);
void manejar_escribir_pagina_completa(int socket_cliente, t_log* logger);
void manejar_obtener_marco_final(int socket_cliente, t_log* logger);
void manejar_obtener_tabla_o_marco(int socket_cliente, t_log* logger);

// Debuggin
int contar_marcos_libres();

#endif
