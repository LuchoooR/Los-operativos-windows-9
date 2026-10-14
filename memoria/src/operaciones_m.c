#include "operaciones_m.h"
#include <stdlib.h>
#include <protocolo.h>
#include <op_code.h>
#include <commons/log.h>
#include <string.h>
#include <math.h>
#include <commons/string.h>
#include <commons/collections/list.h>
#include <socket_utils.h>
#include <pthread.h>

extern int tam_pagina;
extern int entradas_por_tabla;
extern int cantidad_niveles;
extern int tam_memoria;
extern char* path_swap;
extern int retardo_swap;
extern void* memoria_usuario;
extern bool* bitmap_marcos;
extern int cantidad_marcos;
extern t_list* lista_procesos;
extern t_list* lista_entradas_swap;
extern t_list* lista_scripts; // lista de t_script*
extern t_log* logger;
extern char* ruta_base_scripts;
extern int retardo_memoria;
extern char* dump_path;

extern t_dictionary* metricas_por_pid;
extern t_dictionary* swap_map;

pthread_mutex_t mutex_lista_procesos = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_bitmap_marcos = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_memoria_usuario = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_lista_swap = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_lista_scripts = PTHREAD_MUTEX_INITIALIZER;

void handshake_memoria(t_log* logger, int socket_cliente, int tam_pagina, int entradas_por_tabla, int cant_niveles) {
	log_info(logger, "Recibi handshake desde CPU");

	t_hand_inicial_cpu_mem datos;
	datos.tamanio_pag = tam_pagina;
	datos.entradas_por_tabla = entradas_por_tabla;
	datos.cant_niveles = cant_niveles;

	enviar_datos_iniciales_cpu(HANDSHAKE_RESPUESTA, datos, socket_cliente);

	log_info(logger, "Memoria → CPU: Enviado tamaño de pagina = %d, entradas por tabla = %d, niveles = %d", datos.tamanio_pag, datos.entradas_por_tabla, datos.cant_niveles);
}

int buscar_marco_libre() {
    int marco = -1;

    pthread_mutex_lock(&mutex_bitmap_marcos);
    
    for (int i = 0; i < cantidad_marcos; i++) {
        if (!bitmap_marcos[i]) {
            marco = i;
            break;
        }
    }
    
    pthread_mutex_unlock(&mutex_bitmap_marcos);

    return marco; // devuelve el índice o -1 si no encontró
}

t_tabla_nivel* crear_tabla(int nivel_actual, int nivel_final, int entradas_por_tabla) {
    t_tabla_nivel* tabla = malloc(sizeof(t_tabla_nivel));
    tabla->es_nivel_final = (nivel_actual == nivel_final);
    tabla->entradas = list_create();

    for (int i = 0; i < entradas_por_tabla; i++) {
        if (tabla->es_nivel_final) {
            list_add(tabla->entradas, NULL);
        } else {
            t_tabla_nivel* subtabla = crear_tabla(nivel_actual + 1, nivel_final, entradas_por_tabla);
            list_add(tabla->entradas, subtabla);
        }
    }

    return tabla;
}

void manejar_read(int socket_cliente, t_log* logger) {
    usleep(retardo_memoria*1000);
    int size;
    void* buffer = recibir_buffer(&size, socket_cliente);
    
    int pid, direccion_fisica, tamanio;
    int offset = 0;

    memcpy(&pid, buffer + offset, sizeof(int));
    offset += sizeof(int);
    memcpy(&direccion_fisica, buffer + offset, sizeof(int));
    offset += sizeof(int);
    memcpy(&tamanio, buffer + offset, sizeof(int));
    free(buffer);

    log_info(logger, "## PID: %d - Lectura - Dir. Fisica: %d - Tamaño: %d", pid, direccion_fisica, tamanio);

    // Buscar proceso
 
    t_proceso* proceso = buscar_proceso(pid);

    if (!proceso) {
        log_error(logger, "No se encontró el proceso PID=%d en manejar_read", pid);
        return;
    }

    // Validar dirección
    if (direccion_fisica + tamanio > tam_memoria) {
        log_error(logger, "Dirección fuera de rango en READ: PID=%d", pid);
        return;
    }

    // Leer datos desde memoria
    pthread_mutex_lock(&mutex_memoria_usuario);
    
    void* datos = malloc(tamanio + 1);
    memcpy(datos, memoria_usuario + direccion_fisica, tamanio);
    ((char*)datos)[tamanio] = '\0';  // Null-terminate el string

    pthread_mutex_unlock(&mutex_memoria_usuario);

    pthread_mutex_lock(&mutex_lista_procesos);
    proceso->metricas->lecturas++;
    pthread_mutex_unlock(&mutex_lista_procesos);

    // Armar y enviar paquete
    t_paquete *paquete = malloc(sizeof(t_paquete));
    paquete->codigo_operacion = RESPUESTA_OK;
    paquete->buffer = malloc(sizeof(t_buffer));
    paquete->buffer->size = tamanio + 1;
    paquete->buffer->stream = malloc(paquete->buffer->size);
    memcpy(paquete->buffer->stream, datos, paquete->buffer->size);

    int bytes = paquete->buffer->size + 2 * sizeof(int);  // op_code + size + stream
    void *a_enviar = serializar_paquete(paquete, bytes);

    send(socket_cliente, a_enviar, bytes, 0);

    log_info(logger, "[READ] Valor enviado a CPU: '%s'", (char*)datos);

    // Liberar recursos
    free(a_enviar);
    eliminar_paquete(paquete);
    free(datos);
}

void manejar_write(int socket_cliente, t_log* logger) {
    usleep(retardo_memoria * 1000);
    int size;
    void* buffer = recibir_buffer(&size, socket_cliente);

    int offset = 0;
    int pid;
    int direccion_fisica;

    memcpy(&pid, buffer + offset, sizeof(int));
    offset += sizeof(int);

    memcpy(&direccion_fisica, buffer + offset, sizeof(int));
    offset += sizeof(int);

    int tam_datos = size - sizeof(int) * 2;
    char* datos = malloc(tam_datos);
    memcpy(datos, buffer + offset, tam_datos);
    free(buffer);

    log_info(logger, "[WRITE] PID=%d | Dirección física: %d | Datos: %.20s", pid, direccion_fisica, datos);

    if (direccion_fisica + tam_datos > tam_memoria) {
        log_error(logger, "Dirección fuera de rango en WRITE: PID=%d", pid);
        free(datos);
        return;
    }

    pthread_mutex_lock(&mutex_memoria_usuario);

    // 🔁 COPIAR DATOS
    memcpy(memoria_usuario + direccion_fisica, datos, tam_datos);

    pthread_mutex_unlock(&mutex_memoria_usuario);

    int nro_pagina = direccion_fisica / tam_pagina;
    t_proceso* proceso = buscar_proceso(pid);

    if (proceso) {
        t_pagina* pagina = obtener_pagina(proceso, nro_pagina);
        if (pagina) {
            pagina->modificada = true;
        }
        proceso->metricas->escrituras++;
    } else {
        log_error(logger, "[WRITE] No se encontró el proceso PID=%d para marcar como modificada", pid);
    }

    log_info(logger, "[WRITE] Escritura exitosa en dirección %d", direccion_fisica);

    int op = RESPUESTA_OK;
    send(socket_cliente, &op, sizeof(int), 0);

    free(datos);
}


void manejar_obtener_marco_final(int socket_cliente, t_log* logger) {
    usleep(retardo_memoria*1000);

    t_mmu_solicitud solicitud = recibir_mmu_solicitud(socket_cliente);
    int nro_pagina = solicitud.direccion_logica / tam_pagina;

    log_info(logger, "[LOGGER NUESTRO] - Piden Marco - PID=%d, Dirección lógica=%d, Página=%d", solicitud.pid, solicitud.direccion_logica, nro_pagina);

    t_proceso* proceso = buscar_proceso(solicitud.pid);
    if (!proceso) {
        log_error(logger, "PID=%d no encontrado", solicitud.pid);
        enviar_instruccion_con_int(MARCO_O_TABLA, -1, socket_cliente);
        return;
    }

    t_tabla_nivel* tabla_actual = proceso->tabla_1er_nivel;

    // Navegamos por cada nivel intermedio
    for (int nivel = 1; nivel < cantidad_niveles; nivel++) {
        int indice = (nro_pagina / (int)pow(entradas_por_tabla, cantidad_niveles - nivel)) % entradas_por_tabla;
        log_info(logger, "Navegando nivel %d, índice %d", nivel, indice);

        if (!tabla_actual || list_size(tabla_actual->entradas) <= indice) {
            log_error(logger, "Nivel %d no existe o índice %d fuera de rango", nivel, indice);
            enviar_instruccion_con_int(MARCO_O_TABLA, -1, socket_cliente);
            return;
        }

        tabla_actual = list_get(tabla_actual->entradas, indice);
        if (!tabla_actual) {
            log_error(logger, "Subtabla NULL en nivel %d, índice %d", nivel, indice);
            enviar_instruccion_con_int(MARCO_O_TABLA, -1, socket_cliente);
            return;
        }
    }

    // Nivel final
    int indice_final = nro_pagina % entradas_por_tabla;
    log_info(logger, "Nivel final, índice %d", indice_final);

    if (!tabla_actual || list_size(tabla_actual->entradas) <= indice_final) {
        log_error(logger, "Tabla final NULL o índice %d fuera de rango", indice_final);
        enviar_instruccion_con_int(MARCO_O_TABLA, -1, socket_cliente);
        return;
    }

    t_pagina* pagina = list_get(tabla_actual->entradas, indice_final);

    int marco = -1;

    if (pagina) {
        if (!pagina->presente) {
            marco = buscar_marco_libre();
            if (marco == -1) {
                log_error(logger, "No hay marcos libres para la página %d", nro_pagina);
            } else {
                pagina->marco = marco;
                pagina->presente = true;
                pagina->uso = true;
                pagina->modificada = false;

                log_error(logger, "[MARCO ASIGNADO] Página %d → Marco %d", nro_pagina, marco);
            }
        } else {
            marco = pagina->marco;
        }
    } else {
        log_error(logger, "Página %d no existe (NULL)", nro_pagina);
    }

    log_info(logger, "[LOGGER NUESTRO] - PID=%d - Página=%d - Marco=%d", solicitud.pid, nro_pagina, marco);
    enviar_instruccion_con_int(MARCO_O_TABLA, marco, socket_cliente);
}

void manejar_leer_pagina_completa(int socket_cliente, t_log* logger) {
    usleep(retardo_memoria*1000);
    
    int size;
    void* buffer = recibir_buffer(&size, socket_cliente);

    int pid, direccion_fisica;
    memcpy(&pid, buffer, sizeof(int));
    memcpy(&direccion_fisica, buffer + sizeof(int), sizeof(int));
    free(buffer);

    log_info(logger, "## PID: %d - Lectura - Dir. Fisica: 0 - Tamaño: %d", pid, tam_pagina);

    t_proceso* proceso = buscar_proceso(pid);
    if (!proceso) {
        log_error(logger, "PID %d no encontrado en manejar_leer_pagina_completa", pid);
        uint8_t error = ERROR_PROCESO_NO_ENCONTRADO;
        send(socket_cliente, &error, sizeof(uint8_t), 0);
        return;
    }

    void* datos = memoria_usuario + direccion_fisica;
    enviar_pagina_completa(datos, tam_pagina, socket_cliente);

    log_info(logger, "Página completa enviada - PID: %d - Dirección Física: %d", pid, direccion_fisica);
}

void manejar_escribir_pagina_completa(int socket_cliente, t_log* logger) {
    usleep(retardo_memoria*1000);
    int size;
    void* buffer = recibir_buffer(&size, socket_cliente);

    int direccion_fisica;
    int pid;
    memcpy(&pid, buffer, sizeof(int));
    memcpy(&direccion_fisica, buffer + sizeof(int), sizeof(int));
    void* datos = buffer + sizeof(int) * 2;

    if (direccion_fisica + tam_pagina > tam_memoria) {
        log_error(logger, "WRITE página completa fuera de rango");
        free(buffer);
        return;
    }

    pthread_mutex_lock(&mutex_memoria_usuario);
    memcpy(memoria_usuario + direccion_fisica, datos, tam_pagina);
    pthread_mutex_unlock(&mutex_memoria_usuario);

    log_info(logger, "[WRITE_PAGINA] Dirección: %d", direccion_fisica);

    int op = RESPUESTA_OK;
    send(socket_cliente, &op, sizeof(int), 0);

    free(buffer);
}


// Solo sincronizado swap mutex(memoria) definido en suspender_proceso_protegido
void suspender_proceso(int socket_cliente, t_log* logger) {
    usleep(retardo_swap*1000);
    
    int size;
    void* buffer = recibir_buffer(&size, socket_cliente);
    int pid;
    memcpy(&pid, buffer, sizeof(int));
    free(buffer);

    t_proceso* proceso = buscar_proceso(pid);
    if (!proceso) {
        log_error(logger, "PID %d no encontrado en suspender_proceso", pid);
        return;
    }

    char ruta_completa[256];
    snprintf(ruta_completa, sizeof(ruta_completa), "%sswapfile.bin", path_swap);

    FILE* swap = fopen(ruta_completa, "ab+");   // modo binario append + read
    if (!swap) {
        log_error(logger, "No se puede abrir el archivo SWAP en %s", path_swap);
        //pthread_mutex_unlock(&mutex_swap);
        return;
    }

    int paginas_guardadas = 0;

    
        void guardar_paginas(t_tabla_nivel* tabla, int nivel_actual, int pagina_base) {
        //log_info(logger, "[DEBUG SUSPENDER] Nivel actual: %d, Pagina Base: %d, Es nivel final: %d", nivel_actual, pagina_base, tabla->es_nivel_final);

        int entradas = entradas_por_tabla;
        int paginas_por_nivel = pow(entradas_por_tabla, cantidad_niveles - nivel_actual); 

        for (int i = 0; i < entradas; i++) {
            //log_info(logger, "[DEBUG SUSPENDER] Iterando en Nivel %d, Indice %d", nivel_actual, i);
            if (!tabla->es_nivel_final) {
                t_tabla_nivel* tabla_siguiente = list_get(tabla->entradas, i);
                if (tabla_siguiente != NULL) {
                    //log_info(logger, "[DEBUG SUSPENDER]   --> Entrando a subtabla en indice %d", i);
                    guardar_paginas(tabla_siguiente, nivel_actual + 1, pagina_base + i * paginas_por_nivel);
                } else {
                    //log_warning(logger, "[DEBUG SUSPENDER]   --> Entrada %d en Nivel %d es NULL. Se salta esta rama.", i, nivel_actual);
                }
            } else {
                t_pagina* pagina = list_get(tabla->entradas, i);
                if (pagina != NULL) {

                    if (pagina->presente) {
                        
                        int nro_pagina = pagina_base + i;

                        fwrite(&pid, sizeof(int), 1, swap);
                        fwrite(&nro_pagina, sizeof(int), 1, swap);
                        long offset = ftell(swap);

                        void* datos_pagina = memoria_usuario + pagina->marco * tam_pagina;

                        size_t escritos = fwrite(datos_pagina, tam_pagina, 1, swap);
                        if (escritos != 1) {
                            log_error(logger, "Error al escribir pagina %d del PID %d en swap", nro_pagina, pid);
                            continue;
                        }

                        bitmap_marcos[pagina->marco] = false;
                        pagina->presente = false;
                        pagina->marco = -1;

                        paginas_guardadas++;
                        proceso->metricas->swap_out++;

                        t_entrada_swap* entrada = malloc(sizeof(t_entrada_swap));
                        entrada->pid = pid;
                        entrada->nro_pagina = nro_pagina;
                        entrada->offset = offset;
                        list_add(lista_entradas_swap, entrada);
                        
                    }
                } else {
                    //log_info(logger, "[DEBUG SUSPENDER]   --> En Nivel FINAL, Entrada %d es NULL (no hay página).", pagina_base + i);
                }
            }
        }
    }


    guardar_paginas(proceso->tabla_1er_nivel, 1, 0);

    
    fclose(swap);
    //pthread_mutex_unlock(&mutex_swap);

    log_warning(logger, "## PID: %d - Proceso suspendido - Páginas guardadas: %d", pid, paginas_guardadas);
}


// proteje de accesos a memoria toda la funicon suspender proceso
void suspender_proceso_protegido(int socket_cliente, t_log* logger){
    //pthread_mutex_lock(&mutex_memoria);
    suspender_proceso(socket_cliente, logger);
    //pthread_mutex_unlock(&mutex_memoria);
}

void desuspender_proceso(int socket_cliente, t_log* logger) {
    usleep(retardo_swap*1000);
    
    int size;
    void* buffer = recibir_buffer(&size, socket_cliente);
    int pid;
    memcpy(&pid, buffer, sizeof(int));
    free(buffer);

    t_proceso* proceso = buscar_proceso(pid);
    if (!proceso) {
        log_error(logger, "PID %d no encontrado en desuspender_proceso", pid);
        int cod_op = ERROR_SIN_ESPACIO; // o el código de error que uses
        send(socket_cliente, &cod_op, sizeof(int), 0);
        return;
    }

    // NUEVO: Contar cuántas páginas necesitamos restaurar
    int paginas_a_restaurar = 0;
    for (int i = 0; i < list_size(lista_entradas_swap); i++) {
        t_entrada_swap* entrada = list_get(lista_entradas_swap, i);
        if (entrada->pid == pid) {
            paginas_a_restaurar++;
        }
    }

    // NUEVO: Contar marcos libres
    int marcos_libres = 0;
    for (int i = 0; i < cantidad_marcos; i++) {
        if (!bitmap_marcos[i]) {
            marcos_libres++;
        }
    }

    // NUEVO: Verificar si hay espacio suficiente
    if (marcos_libres < paginas_a_restaurar) {
        log_warning(logger, "No hay espacio suficiente para desuspender PID %d. Necesita %d marcos, disponibles %d", 
                    pid, paginas_a_restaurar, marcos_libres);
        int cod_op = ERROR_SIN_ESPACIO;
        send(socket_cliente, &cod_op, sizeof(int), 0);
        return;
    }

    // Si llegamos acá, hay espacio suficiente - proceder con la desuspensión
    char ruta_completa[256];
    snprintf(ruta_completa, sizeof(ruta_completa), "%sswapfile.bin", path_swap);
    
    FILE* swap = fopen(ruta_completa, "r+b");
    if (!swap) {
        log_error(logger, "No se pudo abrir swapfile.bin para lectura");
        int cod_op = ERROR_SIN_ESPACIO;
        send(socket_cliente, &cod_op, sizeof(int), 0);
        return;
    }

    void* pagina_datos = malloc(tam_pagina);
    int restauradas = 0;
    t_list* entradas_a_eliminar = list_create();

    for (int i = 0; i < list_size(lista_entradas_swap); i++) {
        t_entrada_swap* entrada = list_get(lista_entradas_swap, i);

        if (entrada->pid == pid) {
            fseek(swap, entrada->offset, SEEK_SET);

            if (fread(pagina_datos, tam_pagina, 1, swap) != 1) {
                log_error(logger, "Error al leer datos de swap para PID %d, página %d", pid, entrada->nro_pagina);
                continue;
            }

            // Buscar página en tabla jerárquica
            t_tabla_nivel* tabla_actual = proceso->tabla_1er_nivel;
            int paginas_por_nivel = pow(entradas_por_tabla, cantidad_niveles - 1);
            int pagina_idx = entrada->nro_pagina;

            while (!tabla_actual->es_nivel_final) {
                int indice = pagina_idx / paginas_por_nivel;
                tabla_actual = list_get(tabla_actual->entradas, indice);
                pagina_idx %= paginas_por_nivel;
                paginas_por_nivel /= entradas_por_tabla;
            }

            int indice_final = pagina_idx;
            t_pagina* pagina = list_get(tabla_actual->entradas, indice_final);

            // Buscar marco libre (ya sabemos que hay suficientes)
            int marco = -1;
            for (int j = 0; j < cantidad_marcos; j++) {
                if (!bitmap_marcos[j]) {
                    marco = j;
                    bitmap_marcos[j] = true;
                    break;
                }
            }

            // Como ya verificamos que hay espacio, esto no debería fallar
            if (marco == -1) {
                log_error(logger, "Error interno: no se encontró marco libre cuando debería haber");
                continue;
            }

            memcpy(memoria_usuario + marco * tam_pagina, pagina_datos, tam_pagina);

            pagina->marco = marco;
            pagina->presente = true;
            pagina->uso = false;
            pagina->modificada = false;
            
            log_info(logger, "Creada estructura de página: marco=%d, presente=%d, uso=%d, modif=%d", 
                     pagina->marco, pagina->presente, pagina->uso, pagina->modificada);

            proceso->metricas->swap_in++;
            restauradas++;

            list_add(entradas_a_eliminar, entrada);
        }
    }

    // Eliminar las entradas del swap usadas
    for (int i = 0; i < list_size(entradas_a_eliminar); i++) {
        t_entrada_swap* entrada = list_get(entradas_a_eliminar, i);
        list_remove_element(lista_entradas_swap, entrada);
        free(entrada);
    }

    list_destroy(entradas_a_eliminar);
    free(pagina_datos);
    fclose(swap);

    log_info(logger, "## PID: %d - Proceso des-suspendido - Páginas restauradas: %d", pid, restauradas);

    // Enviar respuesta exitosa
    int cod_op = RESPUESTA_OK;
    send(socket_cliente, &cod_op, sizeof(int), 0);
}

void finalizar_proceso(int socket_cliente, t_log* logger) {
    usleep(retardo_memoria*1000);
    
    int size;
    void* buffer = recibir_buffer(&size, socket_cliente);
    int pid;
    memcpy(&pid, buffer, sizeof(int));
    free(buffer);

    int index = -1;

    // Proteger acceso a lista_procesos para buscar índice
    //pthread_mutex_lock(&mutex_lista_procesos);
    for (int i = 0; i < list_size(lista_procesos); i++) {
        t_proceso* p = list_get(lista_procesos, i);
        if (p->pid == pid) {
            index = i;
            break;
        }
    }

    if (index == -1) {
        //pthread_mutex_unlock(&mutex_lista_procesos);
        log_error(logger, "[FINALIZAR] Proceso PID=%d no encontrado", pid);
        return;
    }

    // Remover proceso de la lista protegida
    t_proceso* proceso = list_remove(lista_procesos, index);
    //pthread_mutex_unlock(&mutex_lista_procesos);

    log_info(logger, "[FINALIZAR] Finalizando proceso PID=%d", pid);
    

    loggear_metricas_proceso(proceso, logger);
    
    liberar_marcos_asignados(proceso);
    //Ya estan sincronizadas en la definicion de las funciones
    liberar_tabla_multinivel(proceso->tabla_1er_nivel);
    

    destruir_proceso(proceso);  // libera memoria del struct

    int cod_op = RESPUESTA_OK;
    send(socket_cliente, &cod_op, sizeof(int), 0);

}

void loggear_metricas_proceso(t_proceso* proceso, t_log* logger) {
    t_metricas* m = proceso->metricas;

    log_info(logger, "## PID: %d - Proceso Destruido - Metricas - Acc.T.Pag: %d; Inst.Sol.: %d; SWAP: %d; Mem.Prin.: %d; Lec.Mem.: %d; Esc.Mem.: %d",
        proceso->pid,
        m->acc_tp,
        m->inst_fetch,
        m->swap_out,
        m->swap_in,      
        m->lecturas,
        m->escrituras
    );
}

void liberar_tabla_multinivel(t_tabla_nivel* tabla) {
        //pthread_mutex_lock(&mutex_memoria);

    for (int i = 0; i < list_size(tabla->entradas); i++) {
        void* entrada = list_get(tabla->entradas, i);
        if (!entrada) continue;

        if (tabla->es_nivel_final) {
            t_pagina* pagina = (t_pagina*)entrada;
            free(pagina);
        } else {
            t_tabla_nivel* subtabla = (t_tabla_nivel*)entrada;
            liberar_tabla_multinivel(subtabla);
        }
    }

    list_destroy(tabla->entradas);
    free(tabla);
    //pthread_mutex_lock(&mutex_memoria);

}

void liberar_marcos_asignados(t_proceso* proceso) {
    void recorrer_y_liberar(t_tabla_nivel* tabla, int nivel_actual) {
        for (int i = 0; i < entradas_por_tabla; i++) {
            if (nivel_actual < cantidad_niveles) {
                t_tabla_nivel* subtabla = list_get(tabla->entradas, i);
                if (subtabla != NULL) {
                    recorrer_y_liberar(subtabla, nivel_actual + 1);
                }
            } else {
                t_pagina* pagina = list_get(tabla->entradas, i);
                if (pagina && pagina->presente) {
                    bitmap_marcos[pagina->marco] = false;
                
                    log_trace(logger, "[FINALIZAR] Marco %d liberado", pagina->marco);

                    pagina->presente = false;
                    pagina->marco = -1;
                }
            }
        }
    }

    recorrer_y_liberar(proceso->tabla_1er_nivel, 1);
}


bool proceso_con_pid(void* elem, void* pid_ptr) {
    //pthread_mutex_lock(&mutex_lista_procesos);
    t_proceso* p = (t_proceso*)elem;
    int pid = *(int*)pid_ptr;
    return p->pid == pid;
    //pthread_mutex_unlock(&mutex_lista_procesos);
}

void destruir_proceso(void* p) {
    //pthread_mutex_lock(&mutex_lista_procesos);
    t_proceso* proceso = (t_proceso*)p;
    free(proceso->metricas);
    free(proceso);
    //pthread_mutex_unlock(&mutex_lista_procesos);
}

void dump_paginas_protegido(FILE* dump, t_tabla_nivel* tabla, int nivel_actual) {
    //pthread_mutex_lock(&mutex_memoria);

    dump_paginas(dump, tabla, 1);

    //pthread_mutex_unlock(&mutex_memoria);

}

void dump_paginas(FILE* dump, t_tabla_nivel* tabla, int nivel_actual) {

    int entradas = entradas_por_tabla;
    int paginas_por_nivel = (int) pow(entradas_por_tabla, cantidad_niveles - nivel_actual);

    for (int i = 0; i < entradas; i++) {
        if (nivel_actual < cantidad_niveles) {
            // Nivel medio: bajar 
            t_tabla_nivel* tabla_siguiente = list_get(tabla->entradas, i);
            if (tabla_siguiente != NULL) {

                dump_paginas(dump, tabla_siguiente, nivel_actual + 1);

            } else {
                // escribir páginas vacías
                char cero[tam_pagina];
                memset(cero, 0, tam_pagina);
                for (int j = 0; j < paginas_por_nivel; j++) {
                    fwrite(cero, tam_pagina, 1, dump);
                }
            }
        } else {
            // Nivel final: páginas
            t_pagina* pagina = list_get(tabla->entradas, i);
            if (pagina != NULL && pagina->presente) {
                fwrite(memoria_usuario + pagina->marco * tam_pagina, tam_pagina, 1, dump);
            } else {
                // Página no presente: escribir página vacía
                char cero[tam_pagina];
                memset(cero, 0, tam_pagina);
                fwrite(cero, tam_pagina, 1, dump);
            }
        }
    }

}
    

void dump_memoria(int socket_cliente, t_log* logger) {
    usleep(retardo_memoria*1000);
    int size;
    void* buffer = recibir_buffer(&size, socket_cliente);
    int pid;
    memcpy(&pid, buffer, sizeof(int));
    free(buffer);
    
    log_info(logger, "## PID: %d - Memory Dump solicitado", pid);
    t_proceso* proceso = buscar_proceso(pid);

    if (!proceso) {
        log_error(logger, "PID %d no encontrado para dump", pid);
        return;
    }

    // Obtener timestamp en microsegundos
    time_t ahora = time(NULL);
    char ruta_completa[128];
    snprintf(ruta_completa, sizeof(ruta_completa), "%s%d-%lld.dmp", dump_path, pid, ahora);

    FILE* dump = fopen(ruta_completa, "wb");
    if (!dump) {
        log_error(logger, "Error al crear archivo de dump");
        int op = DUMP_ERROR;
        send(socket_cliente, &op, sizeof(int), 0);
        return;
    }

    dump_paginas_protegido(dump, proceso->tabla_1er_nivel, 1);

    fclose(dump);
    log_info(logger, "## PID: %d - CREE EL MEMORY DUMP EN: %s", pid, ruta_completa);

    int op = DUMP_FINALIZADO_OK;
    send(socket_cliente, &op, sizeof(int),0);
}


t_proceso* buscar_proceso(int pid) {

    pthread_mutex_lock(&mutex_lista_procesos);
    
    t_proceso* resultado = NULL;

    for (int i = 0; i < list_size(lista_procesos); i++) {
        t_proceso* p = list_get(lista_procesos, i);
        if (p->pid == pid) {
            resultado = p;
            break;
        }
    }

    pthread_mutex_unlock(&mutex_lista_procesos);
    return resultado;
}

t_pagina* obtener_pagina(t_proceso* proceso, int nro_pagina) {
    //pthread_mutex_lock(&mutex_memoria);
    t_tabla_nivel* tabla_actual = proceso->tabla_1er_nivel;
    int nivel = 1;

    proceso->metricas->acc_tp++;

    while (!tabla_actual->es_nivel_final) {
        int indice = (nro_pagina / (int)pow(entradas_por_tabla, cantidad_niveles - nivel)) % entradas_por_tabla;

        tabla_actual = (t_tabla_nivel*) list_get(tabla_actual->entradas, indice);
        proceso->metricas->acc_tp++;

        nivel++;
    }

    int indice_final = nro_pagina % entradas_por_tabla;

    proceso->metricas->acc_tp++;

    t_pagina* pagina = (t_pagina*) list_get(tabla_actual->entradas, indice_final);

    //pthread_mutex_unlock(&mutex_memoria);
    return pagina;
}

void manejar_iniciar_proceso(int socket_cliente, t_log* logger) {
    usleep(retardo_memoria*1000);
    
    t_proceso_arch proceso_archivo;

    proceso_archivo = recibir_proceso(socket_cliente);

    int pid = proceso_archivo.pid;
    int tamanio = proceso_archivo.tamanio;
    char* archivo = strdup(proceso_archivo.arch);

    int paginas_necesarias = ceil((double)tamanio / tam_pagina);
    log_info(logger, "el proceso %d, necesita %d paginas", pid, paginas_necesarias);
    
    pthread_mutex_lock(&mutex_bitmap_marcos);

    int libres = 0;
    for (int i = 0; i < cantidad_marcos; i++) {
        if (!bitmap_marcos[i]) libres++;
    }
    
    log_info(logger, "hay %d marcos libres", libres);

    if (libres < paginas_necesarias) {
        pthread_mutex_unlock(&mutex_bitmap_marcos);
        log_warning(logger, "No hay marcos suficientes para PID %d", pid);

        t_paquete* paquete = malloc(sizeof(t_paquete));
        paquete->codigo_operacion = RESPUESTA_ERROR;
        paquete->buffer = malloc(sizeof(t_buffer));
        paquete->buffer->size = 0;
        paquete->buffer->stream = NULL;

        void* a_enviar = serializar_paquete(paquete, sizeof(int) * 2);
        send(socket_cliente, a_enviar, sizeof(int) * 2, 0);

        free(a_enviar);
        eliminar_paquete(paquete);
        free(archivo);
        return;
    }

    pthread_mutex_unlock(&mutex_bitmap_marcos);

    // Crear proceso
    t_proceso* proceso = malloc(sizeof(t_proceso));
    proceso->pid = pid;
    proceso->tamanio = tamanio;
    proceso->tabla_1er_nivel = crear_tabla(1, cantidad_niveles, entradas_por_tabla);
    proceso->metricas = calloc(1, sizeof(t_metricas));

    
    pthread_mutex_lock(&mutex_memoria_usuario);
    
    
    for (int i = 0; i < paginas_necesarias; i++) {

        int marco = -1;
        
        usleep(500);

        log_info(logger, "Asignando página %d al proceso %d", i, pid);

        for (int j = 0; j < cantidad_marcos; j++) {
            if (!bitmap_marcos[j]) {
                marco = j;
                bitmap_marcos[j] = true;
                break;
            }
        }

        log_info(logger, "→ Página %d del proceso %d asignada al marco %d", i, pid, marco);

        if (marco == -1) {
            log_error(logger, "¡No hay marcos libres para la página %d del proceso %d!", i, pid);
            break;
        }

        t_pagina* pagina = malloc(sizeof(t_pagina));
        pagina->marco = marco;
        pagina->presente = true;
        pagina->uso = false;
        pagina->modificada = false;

        log_info(logger, "Creada estructura de página: marco=%d, presente=%d, uso=%d, modif=%d", pagina->marco, pagina->presente, pagina->uso, pagina->modificada);

        t_tabla_nivel* tabla_actual = proceso->tabla_1er_nivel;
        t_tabla_nivel* tabla_padre = NULL;
        int nivel = 1;
        int indice = 0;

        log_info(logger, "el marco, %d, lo esta usando", pagina->marco);

        while (!tabla_actual->es_nivel_final) {
            tabla_padre = tabla_actual;
            indice = (i / (int)pow(entradas_por_tabla, (cantidad_niveles - nivel))) % entradas_por_tabla;
            tabla_actual = list_get(tabla_padre->entradas, indice);

            log_info(logger, "Navegando tabla de nivel %d para página %d: índice=%d", nivel, i, indice);

            if (!tabla_actual) {
                log_error(logger, "Nivel intermedio %d es NULL (pid %d, página %d), creando subtabla", nivel, pid, i);
                tabla_actual = crear_tabla(nivel + 1, cantidad_niveles, entradas_por_tabla);
                list_replace(tabla_padre->entradas, indice, tabla_actual);
            }

            nivel++;
        }

        int indice_final = i % entradas_por_tabla;
        log_info(logger, "Insertando página en tabla final (nivel %d), índice %d", nivel, indice_final);
        list_replace(tabla_actual->entradas, indice_final, pagina);
        log_info(logger, "agregando andooo");
        
    }
    
    
    pthread_mutex_unlock(&mutex_memoria_usuario);

    pthread_mutex_lock(&mutex_lista_procesos);
    list_add(lista_procesos, proceso);
    pthread_mutex_unlock(&mutex_lista_procesos);


    pthread_mutex_lock(&mutex_lista_scripts);
    char ruta_completa[256];
    snprintf(ruta_completa, sizeof(ruta_completa), "%s%s", ruta_base_scripts, archivo);

    FILE* archivo_pseudo = fopen(ruta_completa, "r");
    if (!archivo_pseudo) {      
        log_error(logger, "No se pudo abrir el archivo de pseudocódigo %s", ruta_completa);

        t_paquete* paquete_fail = malloc(sizeof(t_paquete));
        paquete_fail->codigo_operacion = RESPUESTA_ERROR;   
        paquete_fail->buffer = malloc(sizeof(t_buffer));
        paquete_fail->buffer->size = 0;
        paquete_fail->buffer->stream = NULL;
        usleep(retardo_memoria * 1000);
        void* fail_serializado = serializar_paquete(paquete_fail, sizeof(int) * 2);
        send(socket_cliente, fail_serializado, sizeof(int) * 2, 0);

        free(fail_serializado);
        eliminar_paquete(paquete_fail);
        free(archivo);
        return;
    }
    
    t_script* script = malloc(sizeof(t_script));
    script->pid = pid;
    script->instrucciones = list_create();

    char* linea = NULL;
    size_t len = 0;

    while (getline(&linea, &len, archivo_pseudo) != -1) {

        char* linea_procesada = strdup(linea);
        if (!linea_procesada) continue;
        
        string_trim_right(&linea_procesada);

        t_instruccion* instr = parsear_linea(linea_procesada, logger);
  

        if (instr){
            list_add(script->instrucciones, instr);
        }else{
            log_warning(logger, "Línea inválida: '%s'", linea);
        }
    }

    fclose(archivo_pseudo);
    free(linea);
    free(archivo);

    // Agregar script protegido
    if (!lista_scripts){
        lista_scripts = list_create();
    }
    list_add(lista_scripts, script);

    pthread_mutex_unlock(&mutex_lista_scripts);

    log_info(logger, "## PID: %d - Proceso Creado - Tamaño: %d", pid, tamanio);

    t_paquete* paquete = malloc(sizeof(t_paquete));
    paquete->codigo_operacion = RESPUESTA_OK;
    paquete->buffer = malloc(sizeof(t_buffer));
    paquete->buffer->size = 0;
    paquete->buffer->stream = NULL;

    void* a_enviar = serializar_paquete(paquete, sizeof(int) * 2);
    send(socket_cliente, a_enviar, sizeof(int) * 2, 0);

    free(a_enviar);
    eliminar_paquete(paquete);
}

void manejar_fetch(int socket_cliente, t_log* logger) {
    usleep(retardo_memoria*1000);

    int pid;
    int pc;
    int size;

    // Recibir PID y PC en un mismo buffer
    void* buffer = recibir_buffer(&size, socket_cliente);
    int desplazamiento = 0;

    memcpy(&pid, buffer + desplazamiento, sizeof(int));
    desplazamiento += sizeof(int);
    memcpy(&pc, buffer + desplazamiento, sizeof(int));
    free(buffer);



    log_info(logger, "Memoria: Recibí FETCH con PID = %d y PC = %d", pid, pc);

    //pthread_mutex_lock(&mutex_lista_scripts);

    t_script* script = NULL;
    for (int i = 0; i < list_size(lista_scripts); i++) {
        t_script* s = list_get(lista_scripts, i);
        if (s->pid == pid) {
            script = s;
            break;
        }
    }

    // Chequeo 1: existe el script?
    if (!script) {
        log_error(logger, "No se encontró script para PID %d", pid);
        return;
    }

    // Chequeo 2: pc válido?
    if (pc < 0 || pc >= list_size(script->instrucciones)) {
        log_error(logger, "PC fuera de rango para PID %d. PC=%d, instrucciones=%d", pid, pc, list_size(script->instrucciones));
        return;
    }

    t_proceso* proceso = buscar_proceso(pid);

    if (proceso) {
        proceso->metricas->inst_fetch++;
    }

    t_instruccion* inst = NULL;
    inst = list_get(script->instrucciones, pc);

    if (!inst) {
        log_error(logger, "Instrucción nula en PC=%d", pc);
        return;
    }

    //pthread_mutex_unlock(&mutex_lista_scripts);

    t_paquete* paquete = malloc(sizeof(t_paquete));
    paquete->codigo_operacion = inst->codigo;
    paquete->buffer = malloc(sizeof(t_buffer));

    void* buffer_serializado = NULL;
    int buffer_size = 0;

    switch (inst->codigo) {
        case NOOP:{
            log_info(logger, "NOOP");
            log_info(logger, "## PID: %d - Obtener Instruccion: %d - Instruccion: NOOP", pid, pc);
            buffer_size = sizeof(int);
            buffer_serializado = malloc(buffer_size);
            memcpy(buffer_serializado, inst->instruccion, buffer_size);
            break;
        }
        case GOTO:{
            log_info(logger, "GOTO");
            log_info(logger, "## PID: %d - Obtener Instruccion: %d - Instruccion: GOTO - 0", pid, pc);
            buffer_size = sizeof(int);
            buffer_serializado = malloc(buffer_size);
            memcpy(buffer_serializado, inst->instruccion, buffer_size);
            break;
        }
        case READ: {
            log_info(logger, "READ");
            t_instruc_read* r = (t_instruc_read*)inst->instruccion;
            log_info(logger, "## PID: %d - Obtener Instruccion: %d - Instruccion: READ - %d - %d", pid, pc, r->direccion, r->tamanio);
            buffer_serializado = serializar_instruct_read(r);
            buffer_size = sizeof(int) * 2;
            break;
        }            
        case WRITE: {
            log_info(logger, "WRITE");
            t_instruc_write* w = (t_instruc_write*)inst->instruccion;
            log_info(logger, "## PID: %d - Obtener Instruccion: %d - Instruccion: WRITE - %s - %d", pid, pc, w->datos, w->direccion);
            buffer_size = sizeof(int) + strlen(w->datos) + 1;
            buffer_serializado = serializar_instruct_write(w);
            break;
        }
        case IO: {
            log_info(logger, "IO");
            t_instruc_io* io = (t_instruc_io*)inst->instruccion;
            log_info(logger, "## PID: %d - Obtener Instruccion: %d - Instruccion: IO - %s - %d ", pid, pc, io->dispositivo, io->tiempo);
            buffer_serializado = serializar_instruct_io(io, &buffer_size);
            break;
        }
        case INIT_PROC: {
            log_info(logger, "INIT_PROC");
            t_instruc_init_proc* init = (t_instruc_init_proc*)inst->instruccion;
            log_info(logger, "## PID: %d - Obtener Instruccion: %d - Instruccion: INIT_PROC - %s - %d", pid, pc, init->archivo_pseudo, init->tamanio);
            buffer_serializado = serializar_instruc_init_proc(init, &buffer_size);
            break;
        }
        case EXIT:{
            log_info(logger, "EXIT");
            log_info(logger, "## PID: %d - Obtener Instruccion: %d - Instruccion: EXIT", pid, pc);
            buffer_size = sizeof(int);
            buffer_serializado = malloc(buffer_size);
            memcpy(buffer_serializado, inst->instruccion, buffer_size);
            break;
        }
        case DUMP_MEMORY:{
            log_info(logger, "DUMP_MEMORY");
            log_info(logger, "## PID: %d - Obtener Instruccion: %d - Instruccion: DUMP_MEMORY", pid, pc);
            buffer_size = sizeof(int);
            buffer_serializado = malloc(buffer_size);
            memcpy(buffer_serializado, inst->instruccion, buffer_size);
            break;
        }
        default:{
            log_warning(logger, "Código no manejado: %d", inst->codigo);
            break;
        }
    }

    paquete->buffer->size = buffer_size;
    paquete->buffer->stream = buffer_serializado;

    int total_size = sizeof(int) * 2 + buffer_size;
    void* a_enviar = serializar_paquete(paquete, total_size);
    send(socket_cliente, a_enviar, total_size, 0);

    log_info(logger, "Memoria: Envie instruccion tipo %d en respuesta al FETCH para PID %d", inst->codigo, pid);

    free(a_enviar);
    eliminar_paquete(paquete);
}

t_instruccion* parsear_linea(char* linea, t_log* logger) {
    
    char* linea_trabajo = strdup(linea);
    if (!linea_trabajo) return NULL;

    t_instruccion* instruccio = malloc(sizeof(t_instruccion));
    if (!instruccio) {
        log_error(logger, "Error al asignar memoria para instrucción");
        return NULL;
    }
    
    // Hacer una copia de la línea antes de usar strtok
    char* linea_copia = strdup(linea);
    if (!linea_copia) {
        free(instruccio);
        return NULL;
    }
    
    char* codigo_str = strtok(linea_copia, " \n\r");
    if (!codigo_str) {
        free(linea_copia);
        free(instruccio);
        return NULL;
    }
    
    if (strcmp(codigo_str, "NOOP") == 0) {
        log_info(logger, "INSTRUCCION NOOP");
        instruccio->codigo = NOOP;
        int* pc = malloc(sizeof(int));
        if (!pc) {
            free(linea_copia);
            free(instruccio);
            return NULL;
        }
        *pc = 1;
        instruccio->instruccion = pc;
    }
    else if (strcmp(codigo_str, "READ") == 0) {
        log_info(logger, "INSTRUCCION READ");
        instruccio->codigo = READ;
        t_instruc_read* r = malloc(sizeof(t_instruc_read));
        if (!r) {
            free(linea_copia);
            free(instruccio);
            return NULL;
        }
        char* dir_str = strtok(NULL, " \n\r");
        char* tam_str = strtok(NULL, " \n\r");
        if (!dir_str || !tam_str) {
            free(r);
            free(linea_copia);
            free(instruccio);
            return NULL;
        }
        r->direccion = atoi(dir_str);
        r->tamanio = atoi(tam_str);
        instruccio->instruccion = r;
    }
    else if (strcmp(codigo_str, "WRITE") == 0) {
        log_info(logger, "INSTRUCCION WRITE");
        instruccio->codigo = WRITE;
        t_instruc_write* w = malloc(sizeof(t_instruc_write));
        if (!w) {
            free(linea_copia);
            free(instruccio);
            return NULL;
        }
        char* dir_str = strtok(NULL, " \n\r");
        char* texto = strtok(NULL, "\n\r");  // resto de la línea
        if (!dir_str || !texto) {
            free(w);
            free(linea_copia);
            free(instruccio);
            return NULL;
        }
        w->direccion = atoi(dir_str);
        w->datos = strdup(texto);
        if (!w->datos) {
            free(w);
            free(linea_copia);
            free(instruccio);
            return NULL;
        }
        instruccio->instruccion = w;
    }
    else if (strcmp(codigo_str, "GOTO") == 0) {
        log_info(logger, "INSTRUCCION GOTO");
        instruccio->codigo = GOTO;
        char* numero_str = strtok(NULL, " \n\r");
        if (!numero_str) {
            log_error(logger, "GOTO mal definido, no tiene número");
            free(linea_copia);
            free(instruccio);
            return NULL;
        }
        int* pc = malloc(sizeof(int));
        if (!pc) {
            free(linea_copia);
            free(instruccio);
            return NULL;
        }
        *pc = atoi(numero_str);
        instruccio->instruccion = pc;
    }
    else if (strcmp(codigo_str, "IO") == 0) {
        log_info(logger, "INSTRUCCION IO");
        instruccio->codigo = IO;

        t_instruc_io* io = malloc(sizeof(t_instruc_io));
        if (!io) {
            free(linea_copia);
            free(instruccio);
            return NULL;
        }
        
        char* dispositivo = strtok(NULL, " \n\r");
        char* tiempo_str = strtok(NULL, " \n\r");
        
        if (!dispositivo || !tiempo_str) {
            free(io);
            free(linea_copia);
            free(instruccio);
            return NULL;
        }
        
        log_info(logger, "Buenas soy el tiempo de la IO, %s", tiempo_str);

        io->dispositivo = strdup(dispositivo);
        if (!io->dispositivo) {
            free(io);
            free(linea_copia);
            free(instruccio);
            return NULL;
        }
        io->tiempo = atoi(tiempo_str);

        instruccio->instruccion = io;
    }
    else if (strcmp(codigo_str, "INIT_PROC") == 0) {
        log_info(logger, "INSTRUCCION INIT_PROC");
        instruccio->codigo = INIT_PROC;  
        
        char* arch = strtok(NULL, " \n\r");
        char* tamanio_str = strtok(NULL, " \n\r");

        if (!arch || !tamanio_str) {
            log_error(logger, "INIT_PROC mal definida. Línea: '%s'", linea);
            free(linea_copia);
            free(instruccio);
            return NULL;
        }

        t_instruc_init_proc* init = malloc(sizeof(t_instruc_init_proc));
        if (!init) {
            free(linea_copia);
            free(instruccio);
            return NULL;
        }
        
        init->archivo_pseudo = strdup(arch);
        if (!init->archivo_pseudo) {
            free(init);
            free(linea_copia);
            free(instruccio);
            return NULL;
        }
        init->tamanio = atoi(tamanio_str);
        instruccio->instruccion = init;
    }
    else if (strcmp(codigo_str, "EXIT") == 0) {
        log_info(logger, "INSTRUCCION EXIT");
        instruccio->codigo = EXIT;
        int* pc = malloc(sizeof(int));
        if (!pc) {
            free(linea_copia);
            free(instruccio);
            return NULL;
        }
        *pc = 1;
        instruccio->instruccion = pc;
    }
    else if (strcmp(codigo_str, "DUMP_MEMORY") == 0) {
        log_info(logger, "INSTRUCCION DUMP MEMORY");
        instruccio->codigo = DUMP_MEMORY;
        int* pc = malloc(sizeof(int));
        if (!pc) {
            free(linea_copia);
            free(instruccio);
            return NULL;
        }
        *pc = 1;
        instruccio->instruccion = pc;
    }
    else {
        log_error(logger, "Código de instrucción desconocido: %s", codigo_str);
        free(linea_copia);
        free(instruccio);
        return NULL;
    }
    
    free(linea_copia);
    return instruccio;
}

