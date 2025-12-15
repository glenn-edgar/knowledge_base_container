/**
 * cfl_exception.h
 * 
 * Stub exception handler for standalone testing.
 * Replace with your actual implementation.
 */

 #ifndef CFL_EXCEPTION_H
 #define CFL_EXCEPTION_H
 
 #include <stdint.h>
 #include <stdio.h>
 #include <stdlib.h>
 
 /**
  * Exception handler function - implement this for your platform.
  * 
  * Options:
  *   - Log and continue
  *   - Log and halt
  *   - Trigger watchdog reset
  *   - longjmp to error handler
  */
 static inline void cfl_exception_handler(const char* file, const char* func, 
                                           uint16_t line, const char* msg) {
     fprintf(stderr, "EXCEPTION: %s\n", msg);
     fprintf(stderr, "  at %s:%u in %s()\n", file, line, func);
     
     // For testing - halt. In production, you might reset or continue.
     // exit(1);
 }
 
 #define EXCEPTION(msg) \
     cfl_exception_handler(__FILE__, __func__, __LINE__, (msg))
 
 #endif // CFL_EXCEPTION_H