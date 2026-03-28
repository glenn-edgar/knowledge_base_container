/* VTF_FUSE_TEST_test.c
 *
 * 4-level water treatment plant with VFT_fuse.
 * Exercises: normal startup, overcurrent trip, fuse persistence,
 *            operator clear cycle, safety interlock, power loss.
 */
 #include "vtf_fuse_test.h"
 #include "st_display.h"
 #include <stdio.h>
 #include <stdlib.h>
 
 /* ================================================================
  * Fuse action callbacks — called once when a fuse blows
  * ================================================================ */
 
 
 /* ================================================================
  * Main test sequence
  * ================================================================ */
 
 int main(void)
 {
     st_handle_t h;
     if (st_init(&h, &vtf_fuse_test_desc) != 0) {
         fprintf(stderr, "init failed\n");
         return 1;
     }
 
     /* Cache raw buffer pointers — type-checked */
     uint8_t *power     = ST_RAW_PTR(&h, VTF_FUSE_TEST_POWER_INPUTS_ID, uint8_t);
     uint8_t *safety    = ST_RAW_PTR(&h, VTF_FUSE_TEST_SAFETY_INPUTS_ID, uint8_t);
     float   *pump_cur  = ST_RAW_PTR(&h, VTF_FUSE_TEST_PUMP_CURRENT_ID, float);
     float   *pump_lim  = ST_RAW_PTR(&h, VTF_FUSE_TEST_PUMP_LIMITS_ID, float);
     float   *cl_level  = ST_RAW_PTR(&h, VTF_FUSE_TEST_CHLORINE_LEVEL_ID, float);
     float   *cl_max    = ST_RAW_PTR(&h, VTF_FUSE_TEST_CHLORINE_MAX_ID, float);
     uint8_t *fuse_clr  = ST_RAW_PTR(&h, VTF_FUSE_TEST_FUSE_CLEAR_ID, uint8_t);
 
     if (!power || !safety || !pump_cur || !pump_lim ||
         !cl_level || !cl_max || !fuse_clr) {
         fprintf(stderr, "raw buffer type mismatch\n");
         return 1;
     }
 
     /* Build display hierarchy once */
     st_display_t *disp = st_display_init(&h);
     if (!disp) { fprintf(stderr, "display init failed\n"); return 1; }
 
     st_print_info(&h);
 
     /* ============================================================
      * Step 0: Initial — all NOT_OP
      * ============================================================ */
     printf("\n--- Step 0: Initial (all NOT_OP) ---\n");
     st_display_tree(disp, &h);
 
     /* ============================================================
      * Step 1: Normal startup — power on, safety clear, normal ops
      * ============================================================ */
     printf("\n--- Step 1: Normal startup ---\n");
     power[0] = 1;  /* grid on */
     power[1] = 1;  /* generator on */
     power[2] = 1;  /* UPS on */
     safety[0] = 0; safety[1] = 0; safety[2] = 0;  /* no alarms */
 
     pump_cur[0] = 10.0f;  pump_cur[1] = 12.0f;  /* intake pumps normal */
     pump_cur[2] = 8.0f;   pump_cur[3] = 9.0f;   /* dist pumps normal */
     pump_lim[0] = 50.0f;  pump_lim[1] = 50.0f;  /* intake limits */
     pump_lim[2] = 50.0f;  pump_lim[3] = 50.0f;  /* dist limits */
 
     cl_level[0] = 2.0f;   /* chlorine 2 ppm */
     cl_max[0] = 5.0f;     /* max 5 ppm */
 
     st_cycle(&h);
     st_display_tree(disp, &h);
 
     /* ============================================================
      * Step 2: Intake pump 0 overcurrent — fuse blows
      * ============================================================ */
     printf("\n--- Step 2: Intake pump 0 overcurrent (80A > 50A limit) ---\n");
     pump_cur[0] = 80.0f;
     st_cycle(&h);
     st_display_tree(disp, &h);
 
     /* ============================================================
      * Step 3: Current drops but fuse stays blown — no operator clear
      * ============================================================ */
     printf("\n--- Step 3: Current drops to 10A — fuse still blown ---\n");
     pump_cur[0] = 10.0f;
     st_cycle(&h);
     st_display_tree(disp, &h);
 
     /* ============================================================
      * Step 4: Operator clear cycle — assert clear high
      * ============================================================ */
     printf("\n--- Step 4: Operator asserts clear for intake pump 0 ---\n");
     fuse_clr[0] = 1;
     st_cycle(&h);
     st_display_tree(disp, &h);
 
     /* ============================================================
      * Step 5: Operator releases clear — fuse re-arms
      * ============================================================ */
     printf("\n--- Step 5: Operator releases clear — fuse re-armed ---\n");
     fuse_clr[0] = 0;
     st_cycle(&h);
     st_display_tree(disp, &h);
 
     /* ============================================================
      * Step 6: Chlorine alarm + dist pump 1 overcurrent (double fault)
      * ============================================================ */
     printf("\n--- Step 6: Chlorine high (8 ppm) + dist pump 1 overcurrent (70A) ---\n");
     cl_level[0] = 8.0f;
     pump_cur[3] = 70.0f;
     st_cycle(&h);
     st_display_tree(disp, &h);
 
     /* ============================================================
      * Step 7: Emergency stop pressed
      * ============================================================ */
     printf("\n--- Step 7: Emergency stop pressed ---\n");
     safety[0] = 1;
     st_cycle(&h);
     st_display_tree(disp, &h);
 
     /* ============================================================
      * Step 8: E-stop released, clear all fuses
      * ============================================================ */
     printf("\n--- Step 8: E-stop released, assert all clears ---\n");
     safety[0] = 0;
     cl_level[0] = 2.0f;   /* chlorine back to normal */
     pump_cur[3] = 9.0f;   /* dist pump 1 back to normal */
     fuse_clr[2] = 1;      /* clear chlorine */
     fuse_clr[4] = 1;      /* clear dist pump 1 */
     st_cycle(&h);
     st_display_tree(disp, &h);
 
     /* ============================================================
      * Step 9: Release all clears — fuses re-arm
      * ============================================================ */
     printf("\n--- Step 9: Release all clears — full recovery ---\n");
     fuse_clr[2] = 0;
     fuse_clr[4] = 0;
     st_cycle(&h);
     st_display_tree(disp, &h);
 
     /* ============================================================
      * Step 10: Power loss
      * ============================================================ */
     printf("\n--- Step 10: Total power loss ---\n");
     power[0] = 0;  power[1] = 0;  power[2] = 0;
     st_cycle(&h);
     st_display_tree(disp, &h);
 
     st_display_destroy(disp);
     st_destroy(&h);
     printf("\nDone.\n");
     return 0;
 }