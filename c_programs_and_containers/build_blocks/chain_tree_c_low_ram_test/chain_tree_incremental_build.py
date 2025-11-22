from calendar import c

from chain_tree_c_low_ram.chain_tree_build.ct_build.chain_tree_master import ChainTreeMaster


from pathlib import Path

def first_test(ct,kb_name):
    
    ct.start_test(test_name=kb_name)
    
    activate_valve_column = ct.define_column(column_name="activate_valve", column_data=None,auto_start=True)
    ct.asm_one_shot_handler(one_shot_fn="ACTIVATE_VALVE",one_shot_data={"state":"open"})
    ct.asm_log_message("Valve activated")
    ct.asm_terminate()
    ct.end_column(column_name=activate_valve_column)
    
    terminate_engine_column = ct.define_column(column_name="terminate_engine", column_data=None, auto_start=True)
    ct.asm_log_message("waiting time 12 seconds to terminate engine")
    ct.asm_wait_time(time_delay=12.)
    ct.asm_log_message("terminating engine")
    ct.asm_terminate_system()
    ct.end_column(column_name=terminate_engine_column)
    
    wait_for_event_column = ct.define_column(column_name="wait_for_event", column_data=None, auto_start=True)
    ct.asm_log_message("waiting for event")
    wait_for_event_node = ct.asm_wait_for_event(event_id="WAIT_FOR_EVENT",event_count = 1,reset_flag = True,timeout= 5,
                           error_fn = "WAIT_FOR_EVENT_ERROR",time_out_event ="CFL_SECOND_EVENT",error_data = {"error_message":"WAIT_FOR_EVENT_ERROR"})
    ct.asm_log_message("event received")
    ct.asm_reset()
    ct.end_column(column_name=wait_for_event_column)

    
    reset_node_column = ct.define_column(column_name="reset_node", column_data=None, auto_start=True)
    ct.asm_log_message("waiting 2 seconds to reset node")
    ct.asm_wait_time(time_delay=2.)
    ct.asm_log_message("sending system event")
    #ct.asm_send_system_event("WAIT_FOR_EVENT",event_data={})
    #sending an event to a column link or leaf node

    ct.asm_send_named_event(node_id=wait_for_event_column,event_id="WAIT_FOR_EVENT",event_data={})
    ct.asm_log_message("resetting node")
    ct.asm_reset()
    ct.end_column(column_name=reset_node_column)

    verify_column = ct.define_column(column_name="verify", column_data=None, auto_start=True)
    ct.asm_log_message("verifying")
    ct.asm_verify(verify_fn="CFL_BOOL_FALSE",fn_data={},reset_flag=False,
                  error_fn="VERIFY_ERROR",error_data={"failure_data":"failure_data"})
    ct.asm_log_message("waiting for verify to fail")
    ct.asm_halt()
    ct.end_column(column_name=verify_column)
    
    verify_timeout_column = ct.define_column(column_name="verify_timeout", column_data=None, auto_start=True)
    ct.asm_log_message("verifying timeout")
    ct.asm_verify_timeout(time_out=5.,reset_flag=False,error_fn="VERIFY_ERROR",error_data={"failure_data":"failure_data"})
    ct.asm_log_message("waiting for verify timeout to fail which will result in a terminate column")
    ct.asm_halt()
    ct.end_column(column_name=verify_timeout_column)

    ct.end_test()
 
def second_test(ct,kb_name):
    ct.start_test(test_name=kb_name)
    
    activate_valve_column = ct.define_column(column_name="activate_valve", column_data=None,auto_start=True)
    ct.asm_one_shot_handler(one_shot_fn="ACTIVATE_VALVE",one_shot_data={"state":"open"})
    ct.asm_log_message("Valve activated")
    ct.asm_terminate()
    ct.end_column(column_name=activate_valve_column)
    
    terminate_engine_column = ct.define_column(column_name="terminate_engine", column_data=None, auto_start=False)
    ct.asm_log_message("waiting time 20 seconds to terminate engine")
    ct.asm_wait_time(time_delay=20)
    ct.asm_log_message("terminating engine")
    ct.asm_terminate_system()
    ct.end_column(column_name=terminate_engine_column)
    
    wait_for_event_column = ct.define_column(column_name="wait_for_event", column_data=None, auto_start=False)
    ct.asm_log_message("waiting for event")
    wait_for_event_node = ct.asm_wait_for_event(event_id="WAIT_FOR_EVENT",event_count = 1,reset_flag = True,timeout= 5,
                           error_fn = "WAIT_FOR_EVENT_ERROR",time_out_event ="CFL_SECOND_EVENT",error_data = {"error_message":"WAIT_FOR_EVENT_ERROR"})
    ct.asm_log_message("event received")
    ct.asm_reset()
    ct.end_column(column_name=wait_for_event_column)

    
    reset_node_column = ct.define_column(column_name="reset_node", column_data=None, auto_start=False)
    ct.asm_log_message("waiting 2 seconds to reset node")
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("sending system event")
    #ct.asm_send_system_event("WAIT_FOR_EVENT",event_data={})
    #sending an event to a column link or leaf node

    ct.asm_send_named_event(node_id=wait_for_event_node,event_id="WAIT_FOR_EVENT",event_data={})
    ct.asm_log_message("resetting node")
    ct.asm_reset()
    ct.end_column(column_name=reset_node_column)

    enable_column = ct.define_column(column_name="start_column", column_data=None, auto_start=True)
    ct.asm_log_message("waiting 5 seconds to start rest of columns")
    ct.asm_wait_time(time_delay=5)
    ct.asm_log_message("starting rest of columns")
    ct.asm_enable_nodes([activate_valve_column,terminate_engine_column,wait_for_event_column,reset_node_column])
    ct.asm_log_message("waiting 8 seconds to disable column")
    ct.asm_wait_time(time_delay=8)
    ct.asm_disable_nodes([terminate_engine_column])
    ct.asm_log_message("waiting 20 seconds to end test")
    ct.asm_wait_time(time_delay=20)
    ct.asm_log_message("ending test")
    ct.asm_terminate_system()
    ct.end_column(column_name=enable_column)
    
    ct.end_test()
    


    
def fourth_test(ct,kb_name):
    ct.start_test(test_name=kb_name)
    
    top_column = ct.define_column(column_name="top_column", column_data=None,auto_start=True)
    ct.asm_log_message("top column")
    
    middle_column = ct.define_column(column_name="middle_column", column_data=None,auto_start=True)
    ct.asm_log_message("middle column")
    ct.asm_event_logger("displaying middle column events",["PUBLISH_EVENT"])
    #ct.asm_wait_time(time_delay=1)
    #ct.asm_log_message("terminating middle column")
    #ct.asm_terminate()
    ct.asm_halt()
    ct.end_column(column_name=middle_column)
    
    
    
    ct.asm_send_named_event(node_id=top_column,event_id="PUBLISH_EVENT",event_data={"event_data":"event_data"})
    ct.asm_log_message("waiting 2 seconds")
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("resetting top column")
    ct.asm_reset()
    ct.end_column(column_name=top_column)
    
    
    time_out_column = ct.define_column(column_name="time_out_column", column_data=None,auto_start=True)
    ct.asm_wait_time(time_delay=20)
    ct.asm_terminate_system()
    ct.end_column(column_name=time_out_column)
    
    
    
    ct.end_test()
    
    
def fifth_test(ct,kb_name): # state machine
    ct.start_test(test_name=kb_name)
    launch_column = ct.define_column(column_name="launch_column", column_data=None,auto_start=True)
    ct.asm_log_message("launch column")
    ct.asm_log_message("launching state machine 1")
    sm_name_1 = "state_machine_1"
    state_machine_1 = ct.define_state_machine(column_name="state_machine_1",sm_name=sm_name_1,state_names=["state1","state2","state3"],
                                            initial_state="state2",auto_start=True)
    
    state1_1 = ct.define_state(state_name="state1",column_data=None)
    ct.asm_log_message("state1")
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("changing state to state2")
    ct.change_state(sm_node_id=state_machine_1,new_state="state2")
    ct.asm_halt()
    ct.end_column(column_name=state1_1)
    
    state2_1 = ct.define_state(state_name="state2",column_data=None)
    ct.asm_log_message("state2")
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("changing state to state3")
    ct.change_state(state_machine_1,new_state="state3")
    ct.asm_halt()
    ct.end_column(column_name=state2_1)

    state3_1 = ct.define_state(state_name="state3",column_data=None)
    ct.asm_log_message("state3")
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("changing state to state1")
    ct.change_state(state_machine_1,new_state="state1")
    ct.asm_halt()
    ct.end_column(column_name = state3_1)
    
    ct.end_state_machine(state_node=state_machine_1,sm_name="state_machine_1")
    ct.asm_wait_time(time_delay=10)
    ct.asm_log_message("terminating state machine 1")
    ct.terminate_state_machine(state_machine_1)
    sm_name_2 = "state_machine_2"
    state_machine_2 = ct.define_state_machine(column_name="state_machine_2",sm_name=sm_name_2,state_names=["state1","state2","state3"],
                                            initial_state="state3",auto_start=True,aux_function_name="CFL_SM_EVENT_SYNC")
    
    state1_2 = ct.define_state(state_name="state1",column_data=None)
    ct.asm_log_message("state1")
    ct.asm_event_logger("displaying state 1 events",["TEST_EVENT_1","TEST_EVENT_2","TEST_EVENT_3"])
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("changing state to state2")
    ct.asm_send_named_event(node_id=state_machine_2,event_id="TEST_EVENT_1",event_data={})
    ct.asm_send_named_event(node_id=state_machine_2,event_id="TEST_EVENT_2",event_data={})
    ct.asm_send_named_event(node_id=state_machine_2,event_id="TEST_EVENT_3",event_data={})
    ct.change_state(sm_node_id=state_machine_2,new_state="state2",sync_event_id="SYNC_EVENT")
    ct.asm_halt()
    ct.end_column(column_name=state1_2)
    
    state2_2 = ct.define_state(state_name="state2",column_data=None)
    ct.asm_log_message("state2")
    ct.asm_event_logger("displaying state 2 events",["TEST_EVENT_1","TEST_EVENT_2","TEST_EVENT_3"])
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("changing state to state3")
    ct.asm_send_named_event(node_id=state_machine_2,event_id="TEST_EVENT_1",event_data={})
    ct.asm_send_named_event(node_id=state_machine_2,event_id="TEST_EVENT_2",event_data={})
    ct.asm_send_named_event(node_id=state_machine_2,event_id="TEST_EVENT_3",event_data={})
    ct.change_state(state_machine_2,new_state="state3")
    ct.asm_halt()
    ct.end_column(column_name=state2_2)

    state3_2 = ct.define_state(state_name="state3",column_data=None)
    ct.asm_log_message("state3")
    ct.asm_event_logger("displaying state 3 events",["TEST_EVENT_1","TEST_EVENT_2","TEST_EVENT_3"])
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("changing state to state1")
    ct.asm_send_named_event(node_id=state_machine_2,event_id="TEST_EVENT_1",event_data={})
    ct.asm_send_named_event(node_id=state_machine_2,event_id="TEST_EVENT_2",event_data={})
    ct.asm_send_named_event(node_id=state_machine_2,event_id="TEST_EVENT_3",event_data={})
    ct.change_state(state_machine_2,new_state="state1",sync_event_id="SYNC_EVENT")
    ct.asm_halt()
    ct.end_column(column_name = state3_2)
    
    ct.end_state_machine(state_node=state_machine_2,sm_name="state_machine_2")
    ct.asm_log_message("terminating state machine 2")
    ct.asm_wait_time(time_delay=20)
    ct.terminate_state_machine(state_machine_2)
    
    ct.asm_log_message("launch column is terminating")
    
    ct.end_column(column_name=launch_column)
    
    ct.end_test()

def add_header(yaml_file):
    yaml_file = Path(yaml_file)
    
    ct = ChainTreeMaster(yaml_file=yaml_file)
    return ct
    
   
    #ct.display_chain_tree_function_mapping()
    


if __name__ == "__main__":
    test_list = ["first_test","second_test","fourth_test","fifth_test"]
                
    test_dict = { "first_test": first_test}
    test_dict = { "first_test": first_test,
                 "second_test": second_test,
                 "fourth_test": fourth_test,
                 "fifth_test": fifth_test}
    import sys
    if len(sys.argv) != 2:
        print("Usage: python chain_tree_incremental_build.py <yaml_file>")
        sys.exit(1)
    yaml_file = str(sys.argv[1])
    print(yaml_file)
    single_test = "seventeenth_test"
    single_test_flag = False
    if single_test_flag == True:
        ct = add_header(yaml_file)
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
    
