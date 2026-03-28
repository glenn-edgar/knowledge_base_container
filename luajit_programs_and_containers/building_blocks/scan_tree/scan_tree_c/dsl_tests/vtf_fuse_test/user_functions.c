

#include "vtf_fuse_test_fuse_actions.h"
#include  <stdio.h>

/* ================================================================
  * Fuse action callbacks — called once when a fuse blows
  * ================================================================ */
 
  void on_intake_p0_fuse(void *user_handle)
  {
      (void)user_handle;
      printf("  ** FUSE: Intake pump 0 tripped! **\n");
  }
  
  void on_intake_p1_fuse(void *user_handle)
  {
      (void)user_handle;
      printf("  ** FUSE: Intake pump 1 tripped! **\n");
  }
  
  void on_chlorine_fuse(void *user_handle)
  {
      (void)user_handle;
      printf("  ** FUSE: Chlorine dosing high alarm! **\n");
  }
  
  void on_dist_p0_fuse(void *user_handle)
  {
      (void)user_handle;
      printf("  ** FUSE: Distribution pump 0 tripped! **\n");
  }
  
  void on_dist_p1_fuse(void *user_handle)
  {
      (void)user_handle;
      printf("  ** FUSE: Distribution pump 1 tripped! **\n");
  }
  