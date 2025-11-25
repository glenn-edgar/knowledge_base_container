#include "cfl_exception.h"
#include <stdio.h>
#include <stdlib.h>
#include "cfl_common_functions.h"
/* Disable interrupts - ARM Cortex-M specific */
#if defined(__ARM_ARCH_6M__) || defined(__ARM_ARCH_7M__) || defined(__ARM_ARCH_7EM__)
    #define DISABLE_INTERRUPTS() __asm volatile ("cpsid i" : : : "memory")
#else
    #define DISABLE_INTERRUPTS() /* No-op for other architectures */
#endif

/* Simple helper to convert uint16_t to string */


void cfl_exception_handler(const char* file, const char* func, uint16_t line, const char* msg) {
    char line_str[6];
    
    /* Disable interrupts - system is going down */
    DISABLE_INTERRUPTS();
    
    /* Print exception info using puts (safer than printf if heap is corrupted) */
    puts("\n");
    puts("************************************");
    puts("***         EXCEPTION            ***");
    puts("************************************");
    
    puts("File: ");
    puts(file);
    
    puts("Func: ");
    puts(func);
    
    puts("Line: ");
    cfl_uint16_to_str(line, line_str);
    puts(line_str);
    
    puts("Msg:  ");
    puts(msg);
    
    puts("************************************");
    puts("*** Waiting for watchdog reset   ***");
    puts("************************************");
    abort();
    /* Spin forever - watchdog will fire */
    while (1) {
        /* Optional: Toggle GPIO/LED for visual indication */
        __asm volatile ("nop");
    }
}