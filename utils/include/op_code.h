#ifndef OP_CODE_H
#define OP_CODE_H

typedef enum {
    
    INICIAR_PROCESO,
    FINALIZAR_PROCESO,
    SUSPENDER_PROCESO,
    DESUSPENDER_PROCESO,
    SOLICITUD_IO,
    FIN_IO,
    GOTO,
    NOOP,

    OK,
    ERROR_PROCESO_NO_ENCONTRADO,
    ERROR_DIRECCION_INVALIDA,
    
    ERROR_SIN_ESPACIO,

    HANDSHAKE_IO,
    HANDSHAKE_CPU,
    HANDSHAKE_RESPUESTA,
    OBTENER_TABLA_O_MARCO,

    ESCRIBIR_PAGINA_COMPLETA,
    LEER_PAGINA_COMPLETA,
    DESALOJO,
    EJECUTAR_CONTEXTO,
    DEVOLVER_CONTEXTO,

    READ,
    WRITE,
    OBTENER_MARCO,
    MEMORY_DUMP,

    DUMP_ERROR,
    DUMP_FINALIZADO_OK,


    IO,
    INIT_PROC,
    DUMP_MEMORY,
    EXIT,

    PEDIR_MARCO,
    FETCH,
    MENSAJE,
    PAQUETE,
    HANDSHAKE,
    CONSULTAR_ESPACIO_LIBRE,
    CONFIRMACION,
    
    RESPUESTA_OK,
    RESPUESTA_ERROR,
    MARCO_O_TABLA,
    OBTENER_MARCO_FINAL,
    
} op_code;

typedef struct {
    int pid;
    int tiempo_io;
} t_peticion_io;

typedef struct {
    int direccion;
    char* datos;
} t_instruc_write;

typedef struct {
    int direccion;
    int tamanio;
} t_instruc_read;
typedef struct {
    char* archivo_pseudo;
    int tamanio;
} t_instruc_init_proc;
typedef struct {
    op_code codigo;
    void* instruccion;
} t_instruccion;  // Wrapper para que puedas meter todo en una sola lista

typedef struct {
    int tamanio_pag;
    int entradas_por_tabla;
    int cant_niveles;
} t_hand_inicial_cpu_mem;

typedef struct {
    int pid;
    int direccion_logica;
} t_mmu_solicitud;
typedef struct {
    int pid;
    int tamanio;
    char* arch;
} t_proceso_arch;

typedef struct {
    char* dispositivo;
    int tiempo;
} t_instruc_io;

typedef struct{
    char* dispositivo;
    int pid;
}t_fin_io;

#endif
