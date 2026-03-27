/*
 * cfl_mp_config.h - Platform-specific sizing for CFL on MicroPython
 */

#ifndef CFL_MP_CONFIG_H
#define CFL_MP_CONFIG_H

#if defined(__aarch64__)
    /* Linux ARM64 dev — generous memory */
    #define CFL_MAX_TREE_DEPTH    64
    #define CFL_MAX_NODES         1024
    #define CFL_ARENA_SIZE        (64 * 1024)
#elif defined(__thumb2__)
    #if defined(STM32F413xx)
        /* SPIKE Prime — M4, 320KB RAM */
        #define CFL_MAX_TREE_DEPTH    32
        #define CFL_MAX_NODES         256
        #define CFL_ARENA_SIZE        (8 * 1024)
    #elif defined(STM32H7xx) || defined(STM32F7xx)
        /* M7 targets */
        #define CFL_MAX_TREE_DEPTH    48
        #define CFL_MAX_NODES         512
        #define CFL_ARENA_SIZE        (32 * 1024)
    #endif
#else
    /* Default/desktop */
    #define CFL_MAX_TREE_DEPTH    64
    #define CFL_MAX_NODES         1024
    #define CFL_ARENA_SIZE        (64 * 1024)
#endif

#endif /* CFL_MP_CONFIG_H */
