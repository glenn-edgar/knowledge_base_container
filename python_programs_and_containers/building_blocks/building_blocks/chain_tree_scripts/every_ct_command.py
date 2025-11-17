from yaml_generator.data_structures import DataStructures
from chain_tree.ct_build.chain_tree_master import ChainTreeMaster
from pathlib import Path

def define_activate_valve(ct,name):
    function_name = "ACTIVATE_VALVE"
    
    description = "Activate the valve"
    instructions = """
    ;activate the valve  valve data is in node data
    (seq open_valve )
    """
    ct.add_one_shot_instruction(name, description, instructions)
    
if __name__ == "__main__":
    ds = DataStructures(yaml_file=Path("chain_tree_test.yaml"), starting_kb="test_subsystem")
    ct = ChainTreeMaster(ds)
    c = ct.define_root_node(version="1.0.0")
    column_name = ct.define_column(column_name="test_column", column_function_type="COLUMN_FLOW", aux_function ="CFL_NONE", column_data=None)
    ct.asm_one_shot_handler(one_shot_fn="ACTIVATE_VALVE",one_shot_data={"state":"open"})
    ct.asm_log_message("Valve activated")
    define_activate_valve(ct, "ACTIVATE_VALVE")
    ct.asm_bidirectional_one_shot_handler(one_shot_fn="ACTIVATE_VALVE",termination_fn="CLOSE_VALVE",one_shot_data={"state":"open"})
    ct.asm_return_code("CFL_HALT")
    ct.asm_terminate_system()
    ct.asm_disable()
    ct.asm_reset()
    ct.asm_terminate()
    ct.asm_halt("test_halt")
    ct.asm_send_system_event(event_id="test_event",event_data={"state":"open"})
    ct.asm_send_named_event(node_id=column_name,event_id="test_event",event_data={"state":"open"})
    ct.asm_send_parent_event(level=1,event_id="test_event",event_data={"state":"open"})
    ct.asm_wait_time(time_delay=10.0)
    ct.asm_wait_for_event(event_id="test_event",event_count=1,reset_flag=False,timeout=None,
                          error_fn="CFL_NONE",time_out_event ="CF_TIMER_EVENT",error_data = None)
    ct.asm_wait(wait_fn="WAIT_FOR_PRESSURE",  wait_fn_init="WAIT_FOR_PRESSURE_INIT",wait_fn_term="CFL_NONE",fn_data={"state":"open"}, 
                 reset_flag = False, timeout=None,time_out_event="CF_TIMER_EVENT",error_fn = "CFL_NONE",error_data = None)
    ct.end_column(column_name)
    
    parallel_column_name = ct.define_parallel_node(column_name="test_parallel_node")
    ct.end_column(column_name=parallel_column_name)

    sequence_column_name = ct.define_sequence_node(column_name="test_sequence_node")
    ct.end_column(column_name=sequence_column_name)

    selector_column_name = ct.define_selector_node(column_name="test_selector_node")
    ct.end_column(column_name=selector_column_name)
        
    loop_column_name = ct.define_loop_node(column_name="test_loop_node",loop_number=10)
    ct.end_column(column_name=loop_column_name)
        
    while_column_name = ct.define_while_node(column_name="test_while_node",aux_function="TEST_COLUMN")
    ct.end_column(column_name=while_column_name)
    
    watch_dog_column_name = ct.define_watch_dog_node(column_name="test_watch_dog_node",watch_dog_timeout=10)
    
    ct.asm_verify(verify_fn="TEST_VERIFY",verify_fn_init="TEST_VERIFY_INIT",verify_fn_term="TEST_VERIFY_TERM",fn_data={"state":"open"},
                   reset_flag = False,timeout = None,time_out_event = "CF_TIMER_EVENT",failure_fn = "CFL_NONE", failure_data = None )
    ct.end_column(column_name=watch_dog_column_name)
    
        
    state_machine_column_name = ct.define_state_machine(column_name="test_state_machine",sm_name="test_state_machine",state_names=["state1","state2","state3"],
                                                        initialization_state="state1")

    state_column_name = ct.define_state(state_name="state1")
    ct.end_column(column_name=state_column_name)
    state_column_name = ct.define_state(state_name="state2")
    ct.end_column(column_name=state_column_name)
    state_column_name = ct.define_state(state_name="state3")
    ct.end_column(column_name=state_column_name)
    ct.end_state_machine(state_machine_column_name,sm_name="test_state_machine")
    
    ct.finalize_and_write_yaml()
    
    
    ds.generate_yaml()
    ct.display_chain_tree_function_mapping()