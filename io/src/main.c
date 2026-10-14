#include <hello.h>
#include <socket_utils.h>
#include <op_code.h>
#include <protocolo.h>
#include <commons/config.h>
#include <commons/log.h>
#include <thread_io.h>


int main(int argc, char* argv[]) {
    // me asguro de que se indico el dispositivo
    if (argc != 2) {
        printf("Uso: %s [NOMBRE_DISPOSITIVO]\n", argv[0]);
        return EXIT_FAILURE;
    }
    
    char* nombre_dispositivo = argv[1];

    t_log* logger_inicio = iniciar_logger("IO.log", "IO", LOG_LEVEL_INFO);
    t_config* config = leer_config("configI.cfg", logger_inicio);
    if (config == NULL) {
        log_error(logger_inicio, "Error al leer el archivo de configuración.");
        return EXIT_FAILURE;
    }

    log_destroy(logger_inicio);

    char* log_level_str = config_get_string_value(config, "LOG_LEVEL");
    t_log_level nivel_log = log_level_from_string(log_level_str);

    t_log* logger = iniciar_logger("IO.log", "IO", nivel_log);
    char* ip_kernel = config_get_string_value(config,"IP_KERNEL");
    int puerto_kernel = config_get_int_value(config,"PUERTO_KERNEL");

    log_info(logger,"Iniciando IO: %s",nombre_dispositivo);
    log_info(logger, "IP KERNEL: %s", ip_kernel);
    log_info(logger, "PUERTO KERNEL: %d", puerto_kernel);
    
    int socket_kernel = crear_conexion(ip_kernel,puerto_kernel);


    handshake_de_io_kernel(socket_kernel, logger, nombre_dispositivo);
    
    while (1) {
        int cod_op = recibir_operacion(socket_kernel, logger);
        
        switch (cod_op) {
            case SOLICITUD_IO: {
                t_peticion_io peticion = recibir_peticion_IO(socket_kernel);

                log_info(logger, "## PID: %d - Inicio de IO - Tiempo: %d", peticion.pid, peticion.tiempo_io);
                
                int64_t TIEMPO_REAL_IO = (peticion.tiempo_io/1000);
                sleep(TIEMPO_REAL_IO);
                
                log_info(logger, "## PID: %d - Fin de IO", peticion.pid);

                t_fin_io fin_io;
                fin_io.pid = peticion.pid;
                fin_io.dispositivo = nombre_dispositivo;

                enviar_fin_io(&fin_io,socket_kernel);
                break;
            }
            case -1:{
                log_error(logger, "Se desconectó el Kernel");
                config_destroy(config);
                log_destroy(logger);
                liberar_conexion(socket_kernel);
                return EXIT_FAILURE;
            }
            case 1: {
                log_warning(logger, "Operacion desconocida en IO");
                break;
            }
        }
    }
    
    config_destroy(config);
    log_destroy(logger); 
    liberar_conexion(socket_kernel);
    return EXIT_SUCCESS;
}
