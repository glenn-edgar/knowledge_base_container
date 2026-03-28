/* pump_station.h - Generated const data for: pump_station */
#ifndef PUMP_STATION_H
#define PUMP_STATION_H

#include "scan_tree.h"
#include "pump_station_user_vft.h"

/* buf_id=0  pump_station.pump_faults */
#define PUMP_STATION_PUMP_FAULTS_ID 0
#define PUMP_STATION_PUMP_FAULTS_KEY 0xAC640DE8u
#define PUMP_STATION_PUMP_FAULTS_SIZE 4
#define PUMP_STATION_PUMP_FAULTS_P0_FAULT 0
#define PUMP_STATION_PUMP_FAULTS_P1_FAULT 1
#define PUMP_STATION_PUMP_FAULTS_P2_FAULT 2
#define PUMP_STATION_PUMP_FAULTS_P3_FAULT 3

/* buf_id=1  pump_station.power_status */
#define PUMP_STATION_POWER_STATUS_ID 1
#define PUMP_STATION_POWER_STATUS_KEY 0x24A508D6u
#define PUMP_STATION_POWER_STATUS_SIZE 2
#define PUMP_STATION_POWER_STATUS_GRID_POWER 0
#define PUMP_STATION_POWER_STATUS_BACKUP_POWER 1

/* buf_id=2  pump_station.alarm_clear */
#define PUMP_STATION_ALARM_CLEAR_ID 2
#define PUMP_STATION_ALARM_CLEAR_KEY 0x88D94F05u
#define PUMP_STATION_ALARM_CLEAR_SIZE 4
#define PUMP_STATION_ALARM_CLEAR_CLR_P0 0
#define PUMP_STATION_ALARM_CLEAR_CLR_P1 1
#define PUMP_STATION_ALARM_CLEAR_CLR_P2 2
#define PUMP_STATION_ALARM_CLEAR_CLR_P3 3

/* buf_id=3  pump_station.motor_current */
#define PUMP_STATION_MOTOR_CURRENT_ID 3
#define PUMP_STATION_MOTOR_CURRENT_KEY 0xE1C37E31u
#define PUMP_STATION_MOTOR_CURRENT_SIZE 4
#define PUMP_STATION_MOTOR_CURRENT_P0_AMPS 0
#define PUMP_STATION_MOTOR_CURRENT_P1_AMPS 1
#define PUMP_STATION_MOTOR_CURRENT_P2_AMPS 2
#define PUMP_STATION_MOTOR_CURRENT_P3_AMPS 3

/* buf_id=4  pump_station.motor_thresholds */
#define PUMP_STATION_MOTOR_THRESHOLDS_ID 4
#define PUMP_STATION_MOTOR_THRESHOLDS_KEY 0xDE8EEA7Cu
#define PUMP_STATION_MOTOR_THRESHOLDS_SIZE 4
#define PUMP_STATION_MOTOR_THRESHOLDS_P0_MAX 0
#define PUMP_STATION_MOTOR_THRESHOLDS_P1_MAX 1
#define PUMP_STATION_MOTOR_THRESHOLDS_P2_MAX 2
#define PUMP_STATION_MOTOR_THRESHOLDS_P3_MAX 3

/* buf_id=5  pump_station.power.power_output */
#define PUMP_STATION_POWER_POWER_OUTPUT_ID 5
#define PUMP_STATION_POWER_POWER_OUTPUT_KEY 0x051ADDFCu
#define PUMP_STATION_POWER_POWER_OUTPUT_SIZE 1
#define PUMP_STATION_POWER_POWER_OUTPUT_POWER_OK 0

/* buf_id=6  pump_station.actuation.actuation_output */
#define PUMP_STATION_ACTUATION_ACTUATION_OUTPUT_ID 6
#define PUMP_STATION_ACTUATION_ACTUATION_OUTPUT_KEY 0x79345F68u
#define PUMP_STATION_ACTUATION_ACTUATION_OUTPUT_SIZE 2
#define PUMP_STATION_ACTUATION_ACTUATION_OUTPUT_PUMPS_OK 0
#define PUMP_STATION_ACTUATION_ACTUATION_OUTPUT_HAS_POWER 1

/* buf_id=7  pump_station.actuation.group_a.group_a_output */
#define PUMP_STATION_ACTUATION_GROUP_A_GROUP_A_OUTPUT_ID 7
#define PUMP_STATION_ACTUATION_GROUP_A_GROUP_A_OUTPUT_KEY 0x233C7F1Eu
#define PUMP_STATION_ACTUATION_GROUP_A_GROUP_A_OUTPUT_SIZE 3
#define PUMP_STATION_ACTUATION_GROUP_A_GROUP_A_OUTPUT_P0_HEALTHY 0
#define PUMP_STATION_ACTUATION_GROUP_A_GROUP_A_OUTPUT_P1_HEALTHY 1
#define PUMP_STATION_ACTUATION_GROUP_A_GROUP_A_OUTPUT_GA_OK 2

/* buf_id=8  pump_station.actuation.group_b.group_b_output */
#define PUMP_STATION_ACTUATION_GROUP_B_GROUP_B_OUTPUT_ID 8
#define PUMP_STATION_ACTUATION_GROUP_B_GROUP_B_OUTPUT_KEY 0x5E6676BAu
#define PUMP_STATION_ACTUATION_GROUP_B_GROUP_B_OUTPUT_SIZE 3
#define PUMP_STATION_ACTUATION_GROUP_B_GROUP_B_OUTPUT_P2_HEALTHY 0
#define PUMP_STATION_ACTUATION_GROUP_B_GROUP_B_OUTPUT_P3_HEALTHY 1
#define PUMP_STATION_ACTUATION_GROUP_B_GROUP_B_OUTPUT_GB_OK 2

static const st_buf_desc_t pump_station_buf_descs[] = {
    /* [0] */ {"pump_station.pump_faults", 0xAC640DE8u, 4, 1, 0, 0, 255},
    /* [1] */ {"pump_station.power_status", 0x24A508D6u, 2, 1, 0, 1, 255},
    /* [2] */ {"pump_station.alarm_clear", 0x88D94F05u, 4, 1, 0, 2, 255},
    /* [3] */ {"pump_station.motor_current", 0xE1C37E31u, 4, 4, 0, 3, 255},
    /* [4] */ {"pump_station.motor_thresholds", 0xDE8EEA7Cu, 4, 4, 0, 4, 255},
    /* [5] */ {"pump_station.power.power_output", 0x051ADDFCu, 1, 1, 1, 0, 0},
    /* [6] */ {"pump_station.actuation.actuation_output", 0x79345F68u, 2, 1, 1, 1, 1},
    /* [7] */ {"pump_station.actuation.group_a.group_a_output", 0x233C7F1Eu, 3, 1, 1, 2, 1},
    /* [8] */ {"pump_station.actuation.group_b.group_b_output", 0x5E6676BAu, 3, 1, 1, 3, 1},
};

static const st_node_desc_t pump_station_node_descs[] = {
    /* [0] VFT_or [system] -> buf[5][0] */
    {st_vft_or, 5, 0, {{1, 0, 2, 0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}}, 1, {0x00000002u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u}},
    /* [1] VFT_user_motor_health_check [user] -> buf[7][0] */
    {user_vft_motor_health_check, 7, 0, {{3, 0, 1, 0}, {4, 0, 1, 0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}}, 2, {0x00000018u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u}},
    /* [2] VFT_user_motor_health_check [user] -> buf[7][1] */
    {user_vft_motor_health_check, 7, 1, {{3, 1, 1, 0}, {4, 1, 1, 0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}}, 2, {0x00000018u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u}},
    /* [3] VFT_or [system] -> buf[7][2] */
    {st_vft_or, 7, 2, {{7, 0, 2, 0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}}, 1, {0x00000018u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u}},
    /* [4] VFT_user_motor_health_check [user] -> buf[8][0] */
    {user_vft_motor_health_check, 8, 0, {{3, 2, 1, 0}, {4, 2, 1, 0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}}, 2, {0x00000018u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u}},
    /* [5] VFT_user_motor_health_check [user] -> buf[8][1] */
    {user_vft_motor_health_check, 8, 1, {{3, 3, 1, 0}, {4, 3, 1, 0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}}, 2, {0x00000018u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u}},
    /* [6] VFT_or [system] -> buf[8][2] */
    {st_vft_or, 8, 2, {{8, 0, 2, 0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}}, 1, {0x00000018u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u}},
    /* [7] VFT_copy [system] -> buf[6][0] */
    {st_vft_copy, 6, 0, {{7, 2, 1, 0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}}, 1, {0x00000018u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u}},
    /* [8] VFT_copy [system] -> buf[6][1] */
    {st_vft_copy, 6, 1, {{5, 0, 1, 0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}, {0,0,0,0}}, 1, {0x00000002u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u}},
};

static const st_lookup_entry_t pump_station_lookup[] = {
    {0x051ADDFCu, 5},
    {0x233C7F1Eu, 7},
    {0x24A508D6u, 1},
    {0x5E6676BAu, 8},
    {0x79345F68u, 6},
    {0x88D94F05u, 2},
    {0xAC640DE8u, 0},
    {0xDE8EEA7Cu, 4},
    {0xE1C37E31u, 3},
};

static const st_system_desc_t pump_station_desc = {
    "pump_station",
    pump_station_buf_descs, 9,
    pump_station_node_descs, 9,
    pump_station_lookup, 9,
    5, 4,
    NULL, 0
};

#endif /* PUMP_STATION_H */
