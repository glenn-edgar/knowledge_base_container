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
    ct.asm_wait_time(time_delay=12)
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
    ct.asm_wait_time(time_delay=2)
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
    ct.asm_verify_timeout(time_out=5,reset_flag=False,error_fn="VERIFY_ERROR",error_data={"failure_data":"failure_data"})
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
    
    terminate_engine_column = ct.define_column(column_name="terminate_engine", column_data=None, auto_start=True)
    ct.asm_log_message("waiting time 20 seconds to terminate engine")
    ct.asm_wait_time(time_delay=20)
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
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("sending system event")
    #ct.asm_send_system_event("WAIT_FOR_EVENT",event_data={})
    #sending an event to a column link or leaf node

    ct.asm_send_named_event(node_id=wait_for_event_node,event_id="WAIT_FOR_EVENT",event_data={})
    ct.asm_log_message("resetting node")
    ct.asm_reset()
    ct.end_column(column_name=reset_node_column)

    enable_column = ct.define_column(column_name="start_column", column_data=None, auto_start=True)
    ct.asm_log_message("waiting 2 seconds to start rest of columns")
    ct.asm_wait_time(time_delay=2)
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
    test_list = ["first_test","second_test"]
                
    test_dict = { "first_test": first_test, "second_test": second_test}


    single_test = "seventeenth_test"
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
    
