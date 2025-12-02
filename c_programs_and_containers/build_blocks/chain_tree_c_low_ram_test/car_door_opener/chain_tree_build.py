from calendar import c

from chain_tree_c_low_ram.chain_tree_build.ct_build.chain_tree_master import ChainTreeMaster


from pathlib import Path
def insert_bitmask_column(ct):
    bitmask_column = ct.define_column(column_name="bitmask_column", column_data=None,auto_start=True)
    ct.asm_one_shot_handler(one_shot_fn="INITIALIZE_MOTOR_BITMASK",{})
    bitmask_column_inner = ct.define_column(column_name="bitmask_column_inner", column_data=None,auto_start=True)
    ct.asm_one_shot_handler(one_shot_fn="ASSIGN_MOTOR_BITMASK",one_shot_data={})
    ct.asm_reset()
    ct.end_column(column_name=bitmask_column_inner)
    ct.end_column(column_name=bitmask_column)
    return bitmask_column

def insert_emergency_stop(ct):
   
    emergency_stop_column = ct.define_data_flow_event_mask(column_name="emergency_stop_column", event_list=["EMERGENCY_STOP_BIT"], auto_start=True)
    emergency_stop_inner_column = ct.define_column(column_name="emergency_stop_inner_column", column_data=None,auto_start=True)
    ct.asm_one_shot_handler(one_shot_fn="SET_PWM",one_shot_data={"pwm":0})
    ct.asm_one_shot_handler(one_shot_fn="SET_MOTOR_DIRECTION",one_shot_data={"direction":"STOP"})
    ct.asm_one_shot_handler(one_shot_fn="SEND_SERIAL_MESSAGE_EMERGENCY_STOP",{})
    ct.asm_wait_for_bitmask(bitmask_event_list=["EMERGENCY_STOP_BIT"],reset_flag=False,timeout=5000)
    ct.asm_halt()
    ct.end_column(column_name=emergency_stop_column)
    ct.define_join_link(parent_node_name=emergency_stop_inner_column)
    ct.asm_clear_bitmask(bitmask_event_list=["EMERGENCY_STOP_BIT"])
    ct.end_column(column_name=emergency_stop_column)
    return emergency_stop_column

def insert_callibration(ct):
    callibration_column =  ct.define_data_flow_event_mask(column_name="callibration_column", event_list=["CALLIBRATION_BIT"], auto_start=True)
    callibration_inner_column = ct.define_column(column_name="callibration_inner_column", column_data=None,auto_start=True)
    ct.asm_one_shot_handler("SEND_SERIAL_MESSAGE_CALLIBRATION",one_shot_data={})
    find_open_limit_column = ct.define_column(column_name="find_open_limit_column", column_data=None,auto_start=True)
    ct.asm_one_shot_handler("RESET_POSITION_COUNTER",one_shot_data={"position":0})
    ct.asm_one_shot_handler("SET_PWM",one_shot_data={"pwm":0})
    ct.asm_one_shot_handler("SET_MOTOR_DIRECTION",one_shot_data={"direction":"REVERSE"})
    ct.asm_one_shot_handler("SET_PWM",one_shot_data={"pwm":200})
    ct.asm_verify("CALLIBRATION_CURRENT_CALIBRATION_REVERSE",reset_flag = False, error_fn = "CFL_NULL", error_data = None )
    ct.asm_wait("CALLIBRATION_POSITION_TIMEOUT",fn_data=None, reset_flag = False, timeout=3000, error_fn = "CALLIBRATION_TOO_LONG", error_data = None )
    ct.asm_terminate()
    ct.end_column(column_name=find_open_limit_column)
    ct.define_join_link(parent_node_name=find_open_limit_column)
    ct.asm_one_shot_handler("SET_PWM",one_shot_data={"pwm":0})
    ct.asm_one_shot_handler("SET_MOTOR_DIRECTION",one_shot_data={"direction":"STOP"})
    ct.asm_one_shot_handler("LOG_FINAL_POSITION",one_shot_data={})
    find_close_limit_column = ct.define_column(column_name="find_close_limit_column", column_data=None,auto_start=True)
    ct.asm_one_shot_handler("RESET_POSITION_COUNTER",one_shot_data={"position":0})
    ct.asm_one_shot_handler("SET_PWM",one_shot_data={"pwm":0})
    ct.asm_one_shot_handler("SET_MOTOR_DIRECTION",one_shot_data={"direction":"FORWARD"})
    ct.asm_one_shot_handler("SET_PWM",one_shot_data={"pwm":200})
    ct.asm_verify("CALLIBRATION_CURRENT_CALIBRATION_FORWARD",reset_flag = False, error_fn = "CFL_NULL", error_data = None )
    ct.asm_wait("CALLIBRATION_POSITION_TIMEOUT",fn_data=None, reset_flag = False, timeout=3000, error_fn = "CALLIBRATION_TOO_LONG", error_data = None )
    ct.asm_terminate()
    ct.end_column(column_name=find_open_limit_column)
    ct.define_join_link(parent_node_name=find_open_limit_column)
    ct.asm_one_shot_handler("SET_PWM",one_shot_data={"pwm":0})
    ct.asm_one_shot_handler("SET_MOTOR_DIRECTION",one_shot_data={"direction":"STOP"})
    ct.asm_one_shot_handler("LOG_FINAL_POSITION",one_shot_data={})
    ct.asm_one_shot_handler("SET_CALIBRATION_SUCCESS_FULLY",one_shot_data={"successfully_calibrated":True})
    ct.define_column_link(main_function_name="STORE_EPROM_DATA", initialization_function_name="STORE_EPROM_DATA_INIT")
    ct.asm_clear_bitmask(bitmask_event_list=["CALLIBRATION_BIT"])
    ct.end_column(column_name=callibration_inner_column)
    ct.end_column(column_name=callibration_column)
    return callibration_column
    
    
   

def insert_open_door(ct):
    open_door_column =  ct.define_data_flow_event_mask(column_name="close_door_column", event_list=["CLOSE_DOOR_BIT"], auto_start=True)
    open_door_inner_column = ct.define_column(column_name="open_door_inner_column", column_data=None,auto_start=True)
    ct.asm_one_shot_handler("SEND_SERIAL_MESSAGE_CLOSE_DOOR",one_shot_data={})
    ct.asm_one_shot_handler("SET_MOTOR_DIRECTION",one_shot_data={"direction":"REVERSE"})
    ct.asm_one_shot_handler("SET_PWM",one_shot_data={"pwm":200})
    ct.asm_verify("OPEN_DOOR_REVERSE_STOP_CURRENT",reset_flag = False, error_fn = "CFL_NULL", error_data = None )
    ct.asm_wait("OPEN_DOOR_VERIFY",fn_data=None, reset_flag = False, timeout=3000, error_fn = "DOOR_OPEN_TOO_LONG", error_data = None )
    ct.asm_terminate()
    ct.asm_one_shot_handler("SET_MOTOR_DIRECTION",one_shot_data={"direction":"STOP"})
    ct.asm_one_shot_handler("SEND_SERIAL_MESSAGE_CLOSE_DOOR_COMPLETE",one_shot_data={})
    ct.asm_clear_bitmask(bitmask_event_list=["CLOSE_DOOR_BIT"])
    ct.asm_halt()
    ct.end_column(column_name=open_door_inner_column)
    ct.define_join_link(parent_node_name=open_door_inner_column)
    ct.asm_one_shot_handler("SET_PWM",one_shot_data={"pwm":0})
    ct.asm_one_shot_handler("SET_MOTOR_DIRECTION",one_shot_data={"direction":"STOP"})
    ct.asm_one_shot_handler("SEND_SERIAL_MESSAGE_OPEN_DOOR_COMPLETE",one_shot_data={})
    ct.asm_one_shot_handler("LOG_FINAL_POSITION",one_shot_data={})
    ct.asm_clear_bitmask(bitmask_event_list=["OPEN_DOOR_BIT"])
    ct.asm_halt()
    ct.end_column(column_name=open_door_column)
    return open_door_column

def insert_close_door(ct):
    close_door_column =  ct.define_data_flow_event_mask(column_name="close_door_column", event_list=["CLOSE_DOOR_BIT"], auto_start=True)
    close_door_inner_column = ct.define_column(column_name="close_door_inner_column", column_data=None,auto_start=True)
    ct.asm_one_shot_handler("SEND_SERIAL_MESSAGE_CLOSE_DOOR",one_shot_data={})
    ct.asm_one_shot_handler("SET_MOTOR_DIRECTION",one_shot_data={"direction":"FORWARD"})
    ct.asm_one_shot_handler("SET_PWM",one_shot_data={"pwm":200})
    ct.asm_verify("CLOSE_DOOR_REVERSE_STOP_CURRENT",reset_flag = False, error_fn = "CFL_NULL", error_data = None )
    ct.asm_wait("CLOSE_DOOR_VERIFY",fn_data=None, reset_flag = False, timeout=3000, error_fn = "DOOR_CLOSE_TOO_LONG", error_data = None )
    ct.asm_terminate()
    ct.asm_one_shot_handler("SET_MOTOR_DIRECTION",one_shot_data={"direction":"STOP"})
    ct.asm_one_shot_handler("SEND_SERIAL_MESSAGE_CLOSE_DOOR_COMPLETE",one_shot_data={})
    ct.asm_clear_bitmask(bitmask_event_list=["CLOSE_DOOR_BIT"])
    ct.asm_halt()
    ct.end_column(column_name=close_door_inner_column)
    ct.define_join_link(parent_node_name=close_door_inner_column)
    ct.asm_one_shot_handler("SET_PWM",one_shot_data={"pwm":0})
    ct.asm_one_shot_handler("SET_MOTOR_DIRECTION",one_shot_data={"direction":"STOP"})
    ct.asm_one_shot_handler("SEND_SERIAL_MESSAGE_CLOSE_DOOR_COMPLETE",one_shot_data={})
    ct.asm_one_shot_handler("LOG_FINAL_POSITION",one_shot_data={})
    ct.asm_clear_bitmask(bitmask_event_list=["CLOSE_DOOR_BIT"])
    ct.asm_halt()
    ct.end_column(column_name=close_door_column)
    return close_door_column

def insert_auto_revese(ct):
    auto_revese_column =  ct.define_data_flow_event_mask(column_name="auto_reverse_column", event_list=["AUTO_REVERSE_BIT"], auto_start=True)
    ct.asm_one_shot_handler(one_shot_fn="SET_MOTOR_DIRECTION",one_shot_data={"direction":"STOP"})
    ct.asm_one_shot_handler(one_shot_fn="SET_PWM",one_shot_data={"pwm":0})
    ct.asm_one_shot_handler(one_shot_fn="SEND_SERIAL_MESSAGE_OBSTRUCTION_DETECTED",one_shot_data={})
    ct.wait_time(time_delay=.1)
    ct.asm_one_shot_handler(one_shot_fn="SEND_SERIAL_MESSAGE_AUTO_REVERSE",one_shot_data={})
    ct.asm_one_shot_handler(one_shot_fn="SET_PWM",one_shot_data={"pwm":100})
    ct.asm_one_shot_handler(one_shot_fn="SET_MOTOR_DIRECTION",one_shot_data={"direction":"FORWARD"})
    ct.wait_time(time_delay=.50)
    ct.asm_one_shot_handler(one_shot_fn="SET_PWM",one_shot_data={"pwm":0})
    ct.asm_one_shot_handler(one_shot_fn="SET_MOTOR_DIRECTION",one_shot_data={"direction":"STOP"})
    ct.asm_one_shot_handler(one_shot_fn="SEND_SERIAL_MESSAGE_AUTO_REVERSE_COMPLETE",one_shot_data={})
    ct.asm_clear_bitmask(bitmask_event_list=["AUTO_REVERSE_BIT"])
    ct.asm_halt()
    ct.end_column(column_name=auto_revese_column)
    return auto_revese_column

def insert_idle(ct):
    idle_column = ct.define_data_flow_event_mask(column_name="idle_column", aux_function="CFL_NULL", event_list=["IDLE_BIT"], auto_start=True)
    ct.define_column_link(main_function_name="IDLE_MAIN", initialization_function_name="IDLE_INIT", aux_function_name="CFL_NULL",
                           termination_function_name="CFL_NULL", node_data={})
    ct.wait_time(time_delay=.1)
    ct.asm_reset()
    ct.end_column(column_name=idle_column)
    return idle_column

def insert_motor(ct):
    motor_column = ct.define_bitmask_node(column_name="motor_column", column_data=None,auto_start=True)
    bitmask_column = insert_bitmask_column(ct)
    motor_inner_column = ct.define_column(column_name="motor_inner_column", column_data=None,auto_start=True)
    emergency_stop_column = insert_emergency_stop(ct)
    callibration_column = insert_callibration(ct)
    open_door_column = insert_open_door(ct)
    close_door_column = insert_close_door(ct)
    auto_revese_column = insert_auto_revese(ct)
    idle_column = insert_idle(ct)
    ct.asm_halt()
    ct.end_column(column_name=motor_inner_column)
    ct.end_column(column_name=motor_column)
    return motor_column

def insert_initialization(ct):
    initialization_column = ct.define_column(column_name="initialization_column", column_data=None,auto_start=True)
    ct.asm_one_shot_handler(one_shot_fn="CHIP_INITIALIZATION",one_shot_data={})
    ct.asm_terminate()
    ct.end_column(column_name=initialization_column)
    return initialization_column

def insert_serial(ct):
    serial_column = ct.define_column(column_name="serial_column", column_data=None,auto_start=True)
    ct.define_column_link(main_function_name="SERIAL_MAIN", initialization_function_name="SERIAL_INIT", aux_function_name="CFL_NULL",
                           termination_function_name="CFL_NULL", node_data={})
    ct.asm_halt()
    ct.end_column(column_name=serial_column)
    return serial_column


def insert_current_monitor(ct):
    current_monitor_column = ct.define_column(column_name="current_monitor_column", column_data=None,auto_start=True)
    ct.wait_time(time_delay=.01)
    ct.define_column_link(main_function_name="CURRENT_MONITOR_MAIN", initialization_function_name="CURRENT_MONITOR_INIT", aux_function_name="CFL_NULL",
                           termination_function_name="CFL_NULL", node_data={"moving_average":4,"current_threshold":3.0,"current_alarm_threshold":5})
    ct.asm_halt()
    ct.end_column(column_name=current_monitor_column)
    return current_monitor_column

def hall_sensor_check(ct):
    hall_sensor_check_column = ct.define_column(column_name="hall_sensor_check_column", column_data=None,auto_start=True)
    ct.asm_wait_for_bitmask(bitmask_event_list=["WINDOW_MOVING_BIT"])
    ct.asm_one_shot_handler(one_shot_fn="HALL_SENSOR_SETUP",one_shot_data={})
    check_hall_sensor_column = ct.define_column(column_name="check_hall_sensor_column", column_data=None,auto_start=True)
    ct.asm_verify_bitmask(bitmask_event_list=["WINDOW_MOVING_BIT"],reset_flag=False)
    ct.define_column_link(main_function_name="HALL_SENSOR_CHECK_MAIN", initialization_function_name="HALL_SENSOR_CHECK_INIT", aux_function_name="CFL_NULL",
                           termination_function_name="CFL_NULL", node_data={})
    ct.asm_clear_bitmask(bitmask_event_list=["HALL_SENSOR_SETUP"
    ct.wait_time(time_delay=.1)
    ct.asm_reset()
    ct.end_column(column_name=check_hall_sensor_column)
    ct.define_join_link(parent_node_name=check_hall_sensor_column)
    ct.reset()
    ct.end_column(column_name=hall_sensor_check_column)
    return hall_sensor_check_column

def insert_status_monitor(ct):
    statua_monitor_column = ct.define_column(column_name="status_monitor_column", column_data=None,auto_start=True)
    event_column = ct.define_column(column_name="event_column", column_data=None,auto_start=True)
    ct.define_column_link(main_function_name="STATUS_MONITOR_MAIN", initialization_function_name="STATUS_MONITOR_INIT", aux_function_name="CFL_NULL",
                           termination_function_name="CFL_NULL", node_data={})
    ct.wait_event(event_id="STATUS_REQUEST_EVENT",event_count=1,reset_flag=True,timeout=100,error_fn="CFL_NULL",error_data={})
    ct.asm_reset()
    ct.end_column(column_name=event_column)
    ct.end_column(column_name=statua_monitor_column)

def insert_watchdog(ct):
    watchdog_column = ct.define_column(column_name="watchdog_column", column_data=None,auto_start=True)
   
    ct.asm_one_shot_handler(one_shot_fn="MONITOR_TEMPERATURE",one_shot_data={"temp":150})
    ct.asm_one_shot_handler(one_shot_fn="BATTERY_VOLTAGE",one_shot_data={"min_voltage":6.0,"max_voltage":18.0})
    ct.wait_time(time_delay=.1)
    ct.asm_reset()
    ct.end_column(column_name=watchdog_column)
    return watchdog_column

def first_test(ct,kb_name):
    
    ct.start_test(test_name=kb_name,kb_memory_factor=10)
    launch_column = ct.define_column(column_name="launch_column", column_data=None,auto_start=True)
    set_up_column = insert_initialization(ct)
    ct.define_join_link(parent_node_name=set_up_column)
    serial_column =insert_serial(ct)
    motor_column =insert_motor(ct)
    current_monitor_column =insert_current_monitor(ct)
    status_monitor_column =insert_status_monitor(ct)
    watchdog_column =insert_watchdog(ct)
    ct.asm_halt()
    ct.end_column(column_name=launch_column)
    ct.end_test()
    
def add_header(yaml_file):
    yaml_file = Path(yaml_file)
    
    ct = ChainTreeMaster(yaml_file=yaml_file)
    return ct
    
   
    #ct.display_chain_tree_function_mapping()
    


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python chain_tree_small_build.py <yaml_file>")
        sys.exit(1)
    yaml_file = str(sys.argv[1])
    print(yaml_file)
    test_list = ["first_test"]
                
    test_dict = { "first_test": first_test}


    single_test = "first_test"
    single_test_flag = False
    if single_test_flag == True:
        ct = add_header("basic_tests.yaml")
        test_dict[single_test](ct,single_test)
        ct.check_and_generate_yaml()
        ct.display_chain_tree_function_mapping()
        exit()
        
    #test_list = ["seventeenth_test","eighteenth_test"]    
   
    ct = add_header(yaml_file)
    for test in test_list:
    
        test_dict[test](ct,test)
    
    ct.check_and_generate_yaml()
    ct.display_chain_tree_function_mapping()
    print(ct.list_kbs())
    print("total nodds ",ct.ctb.get_total_node_count())
    
    exit()
    
