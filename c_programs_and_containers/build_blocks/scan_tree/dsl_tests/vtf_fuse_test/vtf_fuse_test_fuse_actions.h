/* vtf_fuse_test_fuse_actions.h - Fuse action callback prototypes */
#ifndef VTF_FUSE_TEST_FUSE_ACTIONS_H
#define VTF_FUSE_TEST_FUSE_ACTIONS_H

/*
 * on_intake_p0_fuse
 *   Triggered when fuse blows for: VFT_fuse
 *   Input: vtf_fuse_test.equipment.intake_pumps.intake_output:0
 *   Clear: vtf_fuse_test.fuse_clear:0
 */
extern void on_intake_p0_fuse(void *user_handle);

/*
 * on_intake_p1_fuse
 *   Triggered when fuse blows for: VFT_fuse
 *   Input: vtf_fuse_test.equipment.intake_pumps.intake_output:1
 *   Clear: vtf_fuse_test.fuse_clear:1
 */
extern void on_intake_p1_fuse(void *user_handle);

/*
 * on_chlorine_fuse
 *   Triggered when fuse blows for: VFT_fuse
 *   Input: vtf_fuse_test.equipment.dosing.dosing_output:0
 *   Clear: vtf_fuse_test.fuse_clear:2
 */
extern void on_chlorine_fuse(void *user_handle);

/*
 * on_dist_p0_fuse
 *   Triggered when fuse blows for: VFT_fuse
 *   Input: vtf_fuse_test.equipment.dist_pumps.dist_output:0
 *   Clear: vtf_fuse_test.fuse_clear:3
 */
extern void on_dist_p0_fuse(void *user_handle);

/*
 * on_dist_p1_fuse
 *   Triggered when fuse blows for: VFT_fuse
 *   Input: vtf_fuse_test.equipment.dist_pumps.dist_output:1
 *   Clear: vtf_fuse_test.fuse_clear:4
 */
extern void on_dist_p1_fuse(void *user_handle);

#endif
