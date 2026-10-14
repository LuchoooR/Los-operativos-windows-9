#ifndef THREAD_INTERRUPT_H
#define THREAD_INTERRUPT_H

#include <commons/log.h>

void* thread_interrupt(void* args);

extern volatile bool interrupcion; // variable global compartida

#endif


