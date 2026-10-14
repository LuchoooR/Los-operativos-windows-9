#include <thread_io.h>
#include <socket_utils.h>
#include <stdlib.h>
#include <string.h>

void* atender_io(void* args){
    t_args_io* datos = (t_args_io*) args;

    log_info(datos->logger, "## PID: %d - Inicio de IO - Tiempo: %d", datos->pid, datos->tiempo_io);
    usleep(datos->tiempo_io);  
    log_info(datos->logger, "## PID: %d - Fin de IO", datos->pid);

    t_fin_io fin_io;
    fin_io.pid = datos->pid;
    fin_io.dispositivo = datos->dispositivo;

    enviar_fin_io(fin_io,datos->socket_kernel);

    free(datos);
    return NULL;
}