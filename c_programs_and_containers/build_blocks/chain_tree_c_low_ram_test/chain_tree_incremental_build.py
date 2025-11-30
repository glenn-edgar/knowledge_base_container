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
    
    ct.asm_wait_time(time_delay=20)
    ct.asm_log_message("terminating state machine 2")
    ct.terminate_state_machine(state_machine_2)
    
    ct.asm_log_message("launch column is terminating")
    
    ct.end_column(column_name=launch_column)
    
    ct.end_test()

def insert_fork_column(ct):
    
    fork_column = ct.define_fork_column(column_name="fork_column")
    fork_child_1 = ct.define_column("fork_child_1")
    ct.asm_log_message("fork child 1 starting")
    ct.asm_event_logger("displaying fork child 1 events",["TEST_EVENT"])
    ct.asm_halt()
    ct.end_column(column_name=fork_child_1)
    
    
    fork_child_2 = ct.define_column("fork_child_2")
    ct.asm_log_message("fork child 2 starting")
    ct.asm_event_logger("displaying fork child 2 events",["TEST_EVENT"])
    ct.asm_halt()
    ct.end_column(column_name=fork_child_2)
    
    fork_child_3 = ct.define_column("fork_child_3")
    ct.asm_log_message("fork child 3 starting")
    ct.asm_event_logger("displaying fork child 3 events",["TEST_EVENT"])
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("child 3 executed a time delay of 2 seconds")
    ct.asm_wait_time(time_delay=15)
    ct.asm_halt()
    ct.end_column(column_name=fork_child_3)
    ct.end_column(column_name=fork_column)
    
    
    
    
    
def sixth_test(ct,kb_name):

    ct.start_test(test_name=kb_name)
    
    
    
    launch_column = ct.define_column(column_name="launch_column", column_data=None,auto_start=True)
    ct.asm_log_message("launch column")

    ct.asm_wait_time(time_delay=1.5)
    ct.asm_log_message("launching fork column")
   
    insert_fork_column(ct)

    ct.asm_log_message("fork column launched")
    ct.asm_event_logger("displaying fork column events",["TEST_EVENT"])

    ct.asm_wait_time(time_delay=5)
    ct.asm_log_message("resetting launch column")
    ct.asm_reset()
    ct.end_column(column_name=launch_column)
    
    
    
    
    event_generator_column = ct.define_column(column_name="event_generator_column", column_data=None,auto_start=True)
    ct.asm_log_message("sending event to launch column")
    ct.asm_send_named_event(node_id=launch_column,event_id="TEST_EVENT",event_data={"event_data":"event_data"})
    ct.asm_wait_time(time_delay=1)
    ct.asm_reset()
    ct.end_column(column_name=event_generator_column)
    
    end_column =ct.define_column(column_name="end_column", column_data=None,auto_start=True)
    
    ct.asm_wait_time(time_delay=20)
    ct.asm_log_message("ending test")
    ct.asm_terminate_system()
    ct.end_column(column_name=end_column)
    
    ct.end_test()
    
def insert_fork_join_column(ct):
    fork_join_column = ct.define_fork_column(column_name="fork_column")
    fork_child_1 = ct.define_column("fork_child_1")
    ct.asm_log_message("fork child 1 starting")
    ct.asm_event_logger("displaying fork child 1 events",["TEST_EVENT"])
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("fork 1 is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=fork_child_1)
    
    
    fork_child_2 = ct.define_column("fork_child_2")
    ct.asm_log_message("fork child 2 starting")
    ct.asm_event_logger("displaying fork child 2 events",["TEST_EVENT"])
    ct.asm_wait_time(time_delay=3)
    ct.asm_log_message("fork 2 is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=fork_child_2)
    
    fork_child_3 = ct.define_column("fork_child_3")
    ct.asm_log_message("fork child 3 starting")
    ct.asm_event_logger("displaying fork child 3 events",["TEST_EVENT"])
    ct.asm_wait_time(time_delay=4)
    ct.asm_log_message("fork 3 is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=fork_child_3)
    
    ct.end_column(column_name=fork_join_column)
    ct.define_join_link(parent_node_name=fork_join_column)
    
    
def seventh_test(ct,kb_name): # fork column
    ct.start_test(test_name=kb_name)
    
    
    
    launch_column = ct.define_column(column_name="launch_column", column_data=None,auto_start=True)
    ct.asm_log_message("launch column")

    ct.asm_wait_time(time_delay=1.5)
    ct.asm_log_message("launching fork column")
   
    insert_fork_join_column(ct)
    ct.asm_log_message("fork column joined")
    ct.asm_event_logger("displaying fork column events",["TEST_EVENT"])
    ct.asm_log_message("waiting 5 seconds to reset launch column")
    ct.asm_wait_time(time_delay=5)
    ct.asm_log_message("resetting launch column")
    ct.asm_reset()
    ct.end_column(column_name=launch_column)
    
    
    
    
    event_generator_column = ct.define_column(column_name="event_generator_column", column_data=None,auto_start=True)
    ct.asm_log_message("sending event to launch column")
    ct.asm_send_named_event(node_id=launch_column,event_id="TEST_EVENT",event_data={"event_data":"event_data"})
    ct.asm_wait_time(time_delay=1)
    ct.asm_reset()
    ct.end_column(column_name=event_generator_column)
    
    end_column =ct.define_column(column_name="end_column", column_data=None,auto_start=True)
    ct.asm_wait_time(time_delay=20)
    ct.asm_log_message("ending test")
    ct.asm_terminate_system()
    ct.end_column(column_name=end_column)
    
    
    
    ct.end_test()
    
     
def insert_fork_join_column_a(ct):
    
    sequence_til_pass_node = ct.define_sequence_til_pass_node (column_name="sequence_til_pass_node",finalize_function="DISPLAY_SEQUENCE_TILL_RESULT",
                                                               user_data={"message":"sequence till pass"})

    fork_child_1 = ct.define_column("fork_child_1")
    ct.asm_log_message("fork child 1 starting")
    ct.asm_event_logger("displaying fork child 1 events",["TEST_EVENT"])
    ct.asm_wait_time(time_delay=2)
    ct.mark_sequence_false_link(parent_node_name=sequence_til_pass_node,data={"message":"first sequence failed"})
    ct.asm_log_message("fork 1 is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=fork_child_1)
    
    
    fork_child_2 = ct.define_column("fork_child_2")
    ct.asm_log_message("fork child 2 starting")
    ct.asm_event_logger("displaying fork child 2 events",["TEST_EVENT"])
    ct.asm_wait_time(time_delay=3)
    ct.mark_sequence_false_link(parent_node_name=sequence_til_pass_node,data={"message":"second sequence failed"})
    ct.asm_log_message("fork 2 is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=fork_child_2)
    
    fork_child_3 = ct.define_column("fork_child_3")
    ct.asm_log_message("fork child 3 starting")
    ct.asm_event_logger("displaying fork child 3 events",["TEST_EVENT"])
    ct.asm_wait_time(time_delay=5)
    ct.mark_sequence_false_link(parent_node_name=sequence_til_pass_node,data={"message":"third sequence failed"})
    ct.asm_log_message("fork 3 is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=fork_child_3)

    ct.end_sequence_node(column_name=sequence_til_pass_node)
    
        
def eighth_test(ct,kb_name): # sequence til
    ct.start_test(test_name=kb_name)
    
    main_node = ct.define_sequence_start_node(column_name="main_node",initialize_function="INITIALIZE_SEQUENCE",finalize_function="DISPLAY_SEQUENCE_RESULT",auto_start=True)
    ct.asm_log_message("main node")
    insert_fork_join_column_a(ct)
    ct.asm_log_message("main node is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=main_node)
    
    ct.end_test()
    
def insert_sequence_til_fail_column(ct):
    
    sequence_til_fail_node = ct.define_sequence_til_fail_node (column_name="sequence_til_fail_node",finalize_function="DISPLAY_SEQUENCE_TILL_RESULT"
                                                               ,user_data={"message":"sequence till fail"})

    fork_child_1 = ct.define_column("fork_child_1")
    ct.asm_log_message("fork child 1 starting")
    ct.asm_event_logger("displaying fork child 1 events",["TEST_EVENT"])
    ct.asm_wait_time(time_delay=2)
    ct.mark_sequence_true_link(parent_node_name=sequence_til_fail_node,data={"message":"first sequence passed"})
    ct.asm_log_message("fork 1 is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=fork_child_1)
    
    
    fork_child_2 = ct.define_column("fork_child_2")
    ct.asm_log_message("fork child 2 starting")
    ct.asm_event_logger("displaying fork child 2 events",["TEST_EVENT"])
    ct.asm_wait_time(time_delay=3)
    ct.mark_sequence_true_link(parent_node_name=sequence_til_fail_node,data={"message":"second sequence passed"})
    ct.asm_log_message("fork 2 is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=fork_child_2)
    
    fork_child_3 = ct.define_column("fork_child_3")
    ct.asm_log_message("fork child 3 starting")
    ct.asm_event_logger("displaying fork child 3 events",["TEST_EVENT"])
    ct.asm_wait_time(time_delay=5)
    ct.mark_sequence_true_link(parent_node_name=sequence_til_fail_node,data={"message":"third sequence passed"})
    ct.asm_log_message("fork 3 is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=fork_child_3)

    ct.mark_sequence_true_link(parent_node_name=sequence_til_fail_node,data={"message":"fourth sequence passed"})
    ct.asm_terminate()
    ct.end_sequence_node(column_name=sequence_til_fail_node)
    
def ninth_test(ct,kb_name): # sequence til
    ct.start_test(test_name=kb_name)
    
    main_node = ct.define_sequence_start_node(column_name="main_node",finalize_function="DISPLAY_SEQUENCE_RESULT",auto_start=True)
    ct.asm_log_message("main node")
    insert_sequence_til_fail_column(ct)
    ct.asm_log_message("main node is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=main_node)
    
    ct.end_test()

def test_one_for_one_test(ct,top_column_name):
    top_column = ct.define_column(column_name=top_column_name,auto_start=True)
    
    supervisor_node = ct.define_supervisor_one_for_one_node(column_name="supervisor_node",aux_function ="CFL_NULL",
                                           user_data = {},reset_limited_enabled=False,auto_start = True)
    
    branch_1 = ct.define_column(column_name="branch_1",auto_start=True)
    ct.asm_log_message("branch 1 starting")
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("branch 1 is terminating")
    ct.define_mark_supervisor_node_failure(data={"message":"branch 1 failed"})
    ct.asm_terminate()
    ct.end_column(column_name=branch_1)

    branch_2 = ct.define_column(column_name="branch_2",auto_start=True)
    ct.asm_log_message("branch 2 starting")
    ct.asm_wait_time(time_delay=3)
    ct.asm_log_message("branch 2 is terminating")
    ct.define_mark_supervisor_node_failure(data={"message":"branch 2 failed"})
    ct.asm_terminate()
    ct.end_column(column_name=branch_2)
    
    ct.end_column(column_name=supervisor_node)
    ct.asm_log_message("waiting 20 seconds to terminate top column")
    ct.asm_wait_time(time_delay=20)
    ct.asm_log_message("top column is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=top_column)
    return top_column
    
def test_one_for_all_test(ct,top_column_name):
    top_column = ct.define_column(column_name=top_column_name,auto_start=True)

    supervisor_node = ct.define_supervisor_one_for_all_node(column_name="supervisor_node",aux_function ="CFL_NULL",
                                           user_data = {},reset_limited_enabled=False,auto_start = True)
    
    branch_1 = ct.define_column(column_name="branch_1",auto_start=True)
    ct.asm_log_message("branch 1 starting")
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("branch 1 is terminating")
    ct.define_mark_supervisor_node_failure(data={"message":"branch 1 failed"})
    ct.asm_terminate()
    ct.end_column(column_name=branch_1)

    branch_2 = ct.define_column(column_name="branch_2",auto_start=True)
    ct.asm_log_message("branch 2 starting")
    ct.asm_wait_time(time_delay=3)
    ct.asm_log_message("branch 2 is terminating")
    ct.define_mark_supervisor_node_failure(data={"message":"branch 2 failed"})
    ct.asm_terminate()
    ct.end_column(column_name=branch_2)
    
    branch_3 = ct.define_column(column_name="branch_3",auto_start=True)
    ct.asm_log_message("branch 3 starting")
    ct.asm_wait_time(time_delay=20)
    ct.asm_log_message("branch 3 is resetting")
    ct.asm_reset()
    ct.end_column(column_name=branch_3)
    
    
    ct.end_column(column_name=supervisor_node)
    
    ct.asm_log_message("waiting 20 seconds to terminate top column")
    ct.asm_wait_time(time_delay=20)
    ct.asm_log_message("top column is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=top_column)
    return top_column  

    
def test_rest_for_all_test(ct,top_column_name):
        
    top_column = ct.define_column(column_name=top_column_name,auto_start=True)
    
    supervisor_node = ct.define_supervisor_rest_for_all_node(column_name="supervisor_node",aux_function ="CFL_NULL",
                                           user_data = {},reset_limited_enabled=False,auto_start = True)
    
    branch_1 = ct.define_column(column_name="branch_1",auto_start=True)
    ct.asm_log_message("branch 1 starting")
    ct.asm_wait_time(time_delay=21)
    ct.asm_log_message("branch 1 is resetting")
    ct.asm_reset()
    ct.end_column(column_name=branch_1)

    branch_2 = ct.define_column(column_name="branch_2",auto_start=True)
    ct.asm_log_message("branch 2 starting")
    ct.asm_wait_time(time_delay=3)
    ct.asm_log_message("branch 2 is terminating")
    ct.define_mark_supervisor_node_failure(data={"message":"branch 2 failed"})
    ct.asm_terminate()
    ct.end_column(column_name=branch_2)
    
    
    
    branch_3 = ct.define_column(column_name="branch_3",auto_start=True)
    ct.asm_log_message("branch 3 starting")
    ct.asm_wait_time(time_delay=120)
    ct.asm_log_message("branch 3 is resetting")
    ct.asm_reset()
    ct.end_column(column_name=branch_3)
    
    
    ct.end_column(column_name=supervisor_node)
    ct.asm_log_message("waiting 20 seconds to terminate top column")
    ct.asm_wait_time(time_delay=20)
    ct.asm_log_message("top column is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=top_column)
    return top_column   
 
def test_failure_window_test(ct,top_column_name):
    top_column = ct.define_column(column_name=top_column_name,auto_start=True)
    # should get a failure in around 3 seconds for the window test
    uplink_node_id = 34 # dummy will be filled in actual use
    supervisor_node = ct.define_supervisor_one_for_all_node(column_name="supervisor_node",aux_function ="CFL_NULL",
                                           user_data = {"uplink_node_id":uplink_node_id},reset_limited_enabled=True,
                                           max_reset_number=3,reset_window=100,finalize_function="DISPLAY_FAILURE_WINDOW_RESULT",finalize_function_data={},
                                           auto_start = True)
    
    branch_1 = ct.define_column(column_name="branch_1",auto_start=True)
    ct.asm_log_message("branch 1 starting")
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("branch 1 is terminating")
    ct.define_mark_supervisor_node_failure(data={"message":"branch 1 failed"})
    ct.asm_terminate()
    ct.end_column(column_name=branch_1)

    branch_2 = ct.define_column(column_name="branch_2",auto_start=True)
    ct.asm_log_message("branch 2 starting")
    ct.asm_wait_time(time_delay=120)
    ct.asm_log_message("branch 2 is terminating")
    ct.define_mark_supervisor_node_failure(data={"message":"branch 2 failed"})
    ct.asm_terminate()
    ct.end_column(column_name=branch_2)
    
    branch_3 = ct.define_column(column_name="branch_3",auto_start=True)
    ct.asm_log_message("branch 3 starting")
    ct.asm_wait_time(time_delay=120)
    ct.asm_log_message("branch 3 is resetting")
    ct.asm_reset()
    ct.end_column(column_name=branch_3)
    
    
    ct.end_column(column_name=supervisor_node)
    ct.define_join_link(parent_node_name=supervisor_node)
    #ct.asm_log_message("waiting 20 seconds to terminate top column")
    #ct.asm_wait_time(time_delay=20)
    ct.asm_log_message("top column is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=top_column)
    return top_column  
 
def tenth_test(ct,kb_name): # supervisor node
    ct.start_test(test_name=kb_name)
    test_start = ct.define_column(column_name="test_coordinator_node",column_data=None,auto_start=True)
    
    ct.asm_log_message("starting test one for one")
    test_one_for_one = test_one_for_one_test(ct,"one_for_one_column")
    ct.define_join_link(test_one_for_one)
    
    
    ct.asm_log_message("starting test one for all")
    test_one_for_all = test_one_for_all_test(ct,"one_for_all_column")
    ct.define_join_link(test_one_for_all)
    
    ct.asm_log_message("starting test rest for all")
    test_reset_for_all = test_rest_for_all_test(ct,"rest_for_all_column")
    ct.define_join_link(test_reset_for_all)
    
    ct.asm_log_message("testing failure window test")
    test_failure_window = test_failure_window_test(ct,"failure_window_column")
    ct.define_join_link(test_failure_window)
    
    ct.asm_log_message("test coordinator node is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=test_start)
    
    ct.end_test()
    
def eleventh_test(ct,kb_name): # supervisor node
    ct.start_test(test_name=kb_name)
    launch_column = ct.define_column(column_name="launch_column",auto_start=True)
    for_column = ct.define_for_column(column_name="for_column",number_of_iterations=3,auto_start=True)
    branch_1 = ct.define_column(column_name="branch_1",auto_start=True)
    ct.asm_log_message("branch 1 starting")
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("branch 1 is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=branch_1)
    
    ct.end_column(column_name=for_column)
    
    ct.define_join_link(for_column)
    ct.asm_log_message("for column is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=launch_column)
    ct.end_test()
    
    
def twelfth_test(ct,kb_name): # while column
    ct.start_test(test_name=kb_name)
    
    while_column = ct.define_while_column(column_name="while_column",aux_function="WHILE_TEST",user_data={"count":5},auto_start=True)
    branch_1 = ct.define_column(column_name="branch_1",auto_start=True)
    ct.asm_log_message("branch 1 starting")
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("branch 1 is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=branch_1)
    ct.end_column(column_name=while_column)
    
    ct.end_test() 
    
def thirteenth_test(ct,kb_name): # watch dog
    ct.start_test(test_name=kb_name)
    
    watch_dog_column = ct.define_column(column_name="watch_dog_column",auto_start=True)
    ct.asm_log_message("starting watch dog column")
    wd_node_id = ct.asm_watch_dog_node(wd_time_count=30,wd_reset=True,wd_fn="WATCH_DOG_TIME_OUT",
                          wd_fn_data={"message":"************ watch dog time out  reset action"})
    ct.asm_log_message("watch dog node enabled")
    ct.asm_enable_watch_dog(node_id=wd_node_id)
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("patting watch dog")
    ct.asm_pat_watch_dog(node_id=wd_node_id)
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("disabling watch dog")
    ct.asm_disable_watch_dog(node_id=wd_node_id)
    ct.asm_wait_time(time_delay=4)
    ct.asm_log_message("enabling watch dog")
    ct.asm_enable_watch_dog(node_id=wd_node_id)
    ct.asm_wait_time(time_delay=10)
    ct.asm_log_message("this should not be reached")
    ct.asm_terminate()
    ct.end_column(column_name=watch_dog_column)
    
    end_column = ct.define_column(column_name="end_column",auto_start=True)
    ct.asm_wait_time(time_delay=33)
    ct.asm_log_message("ending test")
    ct.asm_terminate_system()
    ct.end_column(column_name=end_column)
    
    ct.end_test() 
 
def insert_event_mask_df_a(ct):
    
    
    data_flow_mask_column = ct.define_data_flow_event_mask("df_mask",aux_function="CFL_NULL",event_list=["a","c"])
                                                           
    ct.asm_log_message("data flow expression column df_a is active")
    ct.asm_event_logger("----------->  displaying data flow mask events",["CFL_SECOND_EVENT"])
    ct.asm_halt()
    ct.end_column(column_name=data_flow_mask_column)
    return data_flow_mask_column

def insert_event_mask_df_b(ct):
    
    
    data_flow_mask_column = ct.define_data_flow_event_mask("df_mask",aux_function="CFL_NULL",event_list=["b","c"])
                                                           
    ct.asm_log_message("data flow expression column df_b is active")
    ct.asm_event_logger("----------->  displaying data flow mask events",["CFL_SECOND_EVENT"])
    ct.asm_halt()
    ct.end_column(column_name=data_flow_mask_column)
    return data_flow_mask_column




def fourteenth_test(ct,kb_name): # data f

    ct.start_test(test_name=kb_name)


    ##### register data flow events
    launch_column = ct.define_column(column_name="launch_column",auto_start=True)
    insert_event_mask_df_a(ct)
    insert_event_mask_df_b(ct)

    ct.asm_log_message("data flow columns are instantiated")
    ct.asm_wait_time(time_delay=5)
    ct.asm_set_bitmask(["a","c"])
    ct.asm_log_message("bitmask event a and c are set")
    ct.asm_wait_time(time_delay=5)
    ct.asm_log_message("bitmask event b is now set")
    ct.asm_set_bitmask(["b"])
    ct.asm_log_message("bitmask event a is now cleared")
    ct.asm_clear_bitmask(["a"])
    ct.asm_wait_time(time_delay=5)
    ct.asm_log_message("bitmask event b and c are now cleared")
    ct.asm_clear_bitmask(["b","c"])
    
    ct.asm_wait_time(time_delay=5)
    ct.asm_log_message("test is terminating")
   
    ct.asm_terminate()
    ct.end_column(column_name=launch_column)
    
    ct.end_test() 
   

def insert_good_main_column(ct,name:str):
    
    main_column = ct.define_main_exception_column(name=name,auto_start=True)
    ct.asm_log_message("main column is starting")
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("main column is terminating")
    ct.asm_terminate()
    ct.end_main_exception_column(name=main_column)
    return main_column

def insert_bad_main_column(ct,name:str):
    main_column = ct.define_main_exception_column(name=name,auto_start=True)
    ct.asm_log_message("main column is starting")
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("setting step 1")
    ct.asm_set_exception_step(step=1)
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("setting step 2")
    ct.asm_set_exception_step(step=2)
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("setting step 3")
    ct.asm_set_exception_step(step=3)
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("main column is terminating")
    ct.asm_raise_exception(exception_id=1,exception_data={"exception_data":"exception_data"})
    ct.asm_terminate()
    ct.end_main_exception_column(name=main_column)
    return main_column

def insert_good_recovery_column(ct,name:str):
    recover_column = ct.define_recovery_column(name=name,max_steps=5,skip_condition_function="USER_SKIP_CONDITION",
                                               skip_condition_data={"skip_condition_data":"good_recovery_condition"})

    step_5_column = ct.define_column(column_name="step_5_column",auto_start=True)
    ct.asm_log_message("step 5 column is starting")
    ct.asm_wait_time(time_delay=5)
    ct.asm_log_message("step 5 column is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=step_5_column)
    step_4_column = ct.define_column(column_name="step_4_column",auto_start=True)
    ct.asm_log_message("step 4 column is starting")
    ct.asm_wait_time(time_delay=5)
    ct.asm_log_message("step 4 column is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=step_4_column)
    step_3_column = ct.define_column(column_name="step_3_column",auto_start=True)
    ct.asm_log_message("step 3 column is starting")
    ct.asm_wait_time(time_delay=5)
    ct.asm_log_message("step 3 column is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=step_3_column)
    step_2_column = ct.define_column(column_name="step_2_column",auto_start=True)
    ct.asm_log_message("step 2 column is starting")
    ct.asm_wait_time(time_delay=5)
    ct.asm_log_message("step 2 column is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=step_2_column)
    step_1_column = ct.define_column(column_name="step_1_column",auto_start=True)
    ct.asm_log_message("step 1 column is starting")
    ct.asm_wait_time(time_delay=5)
    ct.asm_log_message("step 1 column is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=step_1_column)
    step_0_column = ct.define_column(column_name="step_0_column",auto_start=True)
    ct.asm_log_message("step 0 column is starting")
    ct.asm_wait_time(time_delay=5)
    ct.asm_log_message("step 0 column is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=step_0_column)

    ct.asm_log_message("recovery column is terminating")
    ct.asm_terminate()
    ct.end_recovery_column(name=recover_column)
    return recover_column

def insert_bad_recovery_column(ct,name:str):
    
    recover_column = ct.define_recovery_column(name=name,max_steps=5,skip_condition_function="USER_SKIP_CONDITION",
                                               skip_condition_data={"skip_condition_data":"has_raised_exception"})

    step_5_column = ct.define_column(column_name="step_5_column",auto_start=True)
    ct.asm_log_message("step 5 column is starting")
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("step 5 column is raising exception")
    ct.asm_raise_exception(exception_id=1,exception_data={"exception_data":"exception_data"})
    ct.asm_terminate()
    ct.end_column(column_name=step_5_column)
    step_4_column = ct.define_column(column_name="step_4_column",auto_start=True)
    ct.asm_log_message("step 4 column is starting")
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("step 4 column is raising exception")
    ct.asm_raise_exception(exception_id=1,exception_data={"exception_data":"exception_data"})
    ct.asm_terminate()
    ct.end_column(column_name=step_4_column)
    step_3_column = ct.define_column(column_name="step_3_column",auto_start=True)
    ct.asm_log_message("step 3 column is starting")
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("step 3 column is raising exception")
    ct.asm_raise_exception(exception_id=1,exception_data={"exception_data":"exception_data"})
    ct.asm_terminate()
    ct.end_column(column_name=step_3_column)
    step_2_column = ct.define_column(column_name="step_2_column",auto_start=True)
    ct.asm_log_message("step 2 column is starting")
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("step 2 column is raising exception")
    ct.asm_raise_exception(exception_id=1,exception_data={"exception_data":"exception_data"})
    ct.asm_terminate()
    ct.end_column(column_name=step_2_column)
    step_1_column = ct.define_column(column_name="step_1_column",auto_start=True)
    ct.asm_log_message("step 1 column is starting")
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("step 1 column is raising exception")
    ct.asm_raise_exception(exception_id=1,exception_data={"exception_data":"exception_data"})
    ct.asm_terminate()
    ct.end_column(column_name=step_1_column)
    step_0_column = ct.define_column(column_name="step_0_column",auto_start=True)
    ct.asm_log_message("step 0 column is starting")
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("step 0 column is raising exception")
    ct.asm_raise_exception(exception_id=1,exception_data={"exception_data":"exception_data"})
    ct.asm_terminate()
    ct.end_column(column_name=step_0_column)
    ct.asm_log_message("recovery column is terminating")
    ct.asm_terminate()
    ct.end_recovery_column(name=recover_column)
    return recover_column

    


def insert_good_finalize_column(ct,name:str):
    finalize_column = ct.define_finalize_column(name=name)
    ct.asm_log_message("finalize column is starting")
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("finalize column is terminating")
    ct.asm_terminate()
    ct.end_finalize_column(name=finalize_column)
    return finalize_column

def insert_bad_finalize_column(ct,name:str):
    finalize_column = ct.define_finalize_column(name=name)
    ct.asm_log_message("finalize column is starting")
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("finalize column is generating exception")
    ct.asm_raise_exception(exception_id=3,exception_data={"exception_data":"exception_data"})
    ct.asm_terminate()
    ct.end_finalize_column(name=finalize_column)
    return finalize_column
    

def insert_exception_catch_column(ct,name:str):
    
    exception_catch_column = ct.define_exception_catch(name,"EXCEPTION_FILTER",{"exception_filter_data":"exception_filter_data"},"EXCEPTION_LOGGING",
                                                       {"logging_function_data":"logging_function_data"},auto_start=True)
                    
    return exception_catch_column
    
def end_exception_catch_column(ct,name:str):
    
    ct.exception_catch_end(name)
    
def seventeenth_test(ct,kb_name): # exception handler
    ct.start_test(test_name=kb_name)
    launch_column = ct.define_column(column_name="launch_column",auto_start=True)
    ct.asm_log_message("launch column is starting")
    catch_all_exception_column = ct.catch_all_exception(column_name="catch_all_exception_column",aux_function="CATCH_ALL_EXCEPTION",
                                                        aux_data={"aux_data":"aux_data"},auto_start=True)
    ct.asm_log_message("exception combo 1 is starting")
    exception_catch_column_1 = insert_exception_catch_column(ct,"combo_1")
    insert_good_main_column(ct,"combo_1_main")
    insert_good_recovery_column(ct,"combo_1_recovery")
    insert_good_finalize_column(ct,"combo_1_finalize")
    end_exception_catch_column(ct,exception_catch_column_1)
    ct.define_join_link(exception_catch_column_1)
    ct.asm_wait_time(time_delay=1)
    ct.asm_log_message("exception combo 2 is starting")
    exception_catch_column_2 = insert_exception_catch_column(ct,"combo_2")
    insert_bad_main_column(ct,"combo_2_main")
    insert_good_recovery_column(ct,"combo_2_recovery")
    insert_good_finalize_column(ct,"combo_2_finalize")
    end_exception_catch_column(ct,exception_catch_column_2)
    ct.define_join_link(exception_catch_column_2)
    ct.asm_wait_time(time_delay=1)
    ct.asm_log_message("exception combo 3 is starting")
    exception_catch_column_3 = insert_exception_catch_column(ct,"combo_3")
    insert_bad_main_column(ct,"combo_3_main")
    insert_bad_recovery_column(ct,"combo_3_recovery")
    insert_good_finalize_column(ct,"combo_3_finalize")
    end_exception_catch_column(ct,exception_catch_column_3)
    ct.define_join_link(exception_catch_column_3)
    ct.asm_wait_time(time_delay=1)
    ct.asm_log_message("exception combo 4 is starting")
    exception_catch_column_4 = insert_exception_catch_column(ct,"combo_4")
    insert_good_main_column(ct,"combo_4_main")
    insert_good_recovery_column(ct,"combo_4_recovery")
    insert_bad_finalize_column(ct,"combo_4_finalize")
    end_exception_catch_column(ct,exception_catch_column_4)
    ct.define_join_link(exception_catch_column_4)
    
    ct.end_column(column_name=catch_all_exception_column)
    ct.define_join_link(catch_all_exception_column)
    ct.asm_log_message("launch column is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=launch_column)
    ct.end_test()
    
    
def insert_good_main_column_heartbeat(ct,name:str):
    main_column = ct.define_main_exception_column(name=name,auto_start=True)
    ct.asm_log_message("main column is starting")
    ct.asm_turn_heartbeat_on(time_out=50)
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("setting step 1")
    ct.asm_set_exception_step(step=1)
    ct.asm_heartbeat_event()
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("setting step 2")
    ct.asm_set_exception_step(step=2)
    ct.asm_heartbeat_event()
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("setting step 3")
    ct.asm_set_exception_step(step=3)
    ct.asm_wait_time(time_delay=2)
    ct.asm_turn_heartbeat_off()
    ct.asm_log_message("main column is terminating")

    ct.asm_terminate()
    ct.end_main_exception_column(name=main_column)
    return main_column


def insert_bad_main_column_heartbeat(ct,name:str):
    main_column = ct.define_main_exception_column(name=name,auto_start=True)
    ct.asm_log_message("main column is starting")
    ct.asm_turn_heartbeat_on(time_out=50)
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("setting step 1")
    ct.asm_set_exception_step(step=1)
    ct.asm_heartbeat_event()
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("setting step 2")
    ct.asm_set_exception_step(step=2)
    #ct.asm_heartbeat_event()
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("setting step 3")
    ct.asm_set_exception_step(step=3)
    ct.asm_wait_time(time_delay=2)
    ct.asm_turn_heartbeat_off()
    ct.asm_log_message("main column is terminating")

    ct.asm_terminate()
    ct.end_main_exception_column(name=main_column)
    return main_column


def insert_good_recovery_column_heartbeat(ct,name:str):
    recover_column = ct.define_recovery_column(name=name,max_steps=5,skip_condition_function="USER_SKIP_CONDITION",
                                               skip_condition_data={"skip_condition_data":"good_recovery_condition"})

    step_5_column = ct.define_column(column_name="step_5_column",auto_start=True)
    ct.asm_log_message("step 5 column is starting")
    ct.asm_wait_time(time_delay=5)
    ct.asm_log_message("step 5 column is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=step_5_column)
    step_4_column = ct.define_column(column_name="step_4_column",auto_start=True)
    ct.asm_log_message("step 4 column is starting")
    ct.asm_wait_time(time_delay=5)
    ct.asm_log_message("step 4 column is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=step_4_column)
    step_3_column = ct.define_column(column_name="step_3_column",auto_start=True)
    ct.asm_log_message("step 3 column is starting")
    ct.asm_wait_time(time_delay=5)
    ct.asm_log_message("step 3 column is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=step_3_column)
    step_2_column = ct.define_column(column_name="step_2_column",auto_start=True)
    ct.asm_log_message("step 2 column is starting")
    ct.asm_wait_time(time_delay=5)
    ct.asm_log_message("step 2 column is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=step_2_column)
    step_1_column = ct.define_column(column_name="step_1_column",auto_start=True)
    ct.asm_log_message("step 1 column is starting")
    ct.asm_wait_time(time_delay=5)
    ct.asm_log_message("step 1 column is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=step_1_column)
    step_0_column = ct.define_column(column_name="step_0_column",auto_start=True)
    ct.asm_log_message("step 0 column is starting")
    ct.asm_wait_time(time_delay=5)
    ct.asm_log_message("step 0 column is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=step_0_column)

    ct.asm_log_message("recovery column is terminating")
    ct.asm_terminate()
    ct.end_recovery_column(name=recover_column)
    return recover_column


    


def insert_good_finalize_column_heartbeat(ct,name:str):
    finalize_column = ct.define_finalize_column(name=name)
    ct.asm_log_message("finalize column is starting")
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("finalize column is terminating")
    ct.asm_terminate()
    ct.end_finalize_column(name=finalize_column)
    return finalize_column

def insert_bad_finalize_column_heartbeat(ct,name:str):
    finalize_column = ct.define_finalize_column(name=name)
    ct.asm_log_message("finalize column is starting")
    ct.asm_turn_heartbeat_on(time_out=10)
    ct.asm_wait_time(time_delay=2)
    ct.asm_log_message("finalize column is generating exception")
    ct.asm_raise_exception(exception_id=3,exception_data={"exception_data":"exception_data"})
    ct.asm_terminate()
    ct.end_finalize_column(name=finalize_column)
    return finalize_column
    

def insert_exception_catch_column_heartbeat(ct,name:str):
    
    exception_catch_column = ct.define_exception_catch(name,"EXCEPTION_FILTER",{"exception_filter_data":"exception_filter_data"},"EXCEPTION_LOGGING",
                                                       {"logging_function_data":"logging_function_data"},auto_start=True)
                    
    return exception_catch_column
    
def end_exception_catch_column_heartbeat(ct,name:str):
    
    ct.exception_catch_end(name)
 
 
def eighteenth_test(ct,kb_name): # exception handler
    ct.start_test(test_name=kb_name)
    launch_column = ct.define_column(column_name="launch_column",auto_start=True)
    ct.asm_log_message("launch column is starting")
    catch_all_exception_column = ct.catch_all_exception(column_name="catch_all_exception_column",aux_function="CATCH_ALL_EXCEPTION",
                                                        aux_data={"aux_data":"aux_data"},auto_start=True)
    ct.asm_log_message("exception combo 1 is starting")
    exception_catch_column_1 = insert_exception_catch_column(ct,"combo_1")
    insert_good_main_column_heartbeat(ct,"combo_1_main_heartbeat")
    insert_good_recovery_column_heartbeat(ct,"combo_1_recovery_heartbeat")
    insert_good_finalize_column_heartbeat(ct,"combo_1_finalize_heartbeat")
    end_exception_catch_column_heartbeat(ct,exception_catch_column_1)
    ct.define_join_link(exception_catch_column_1)
    ct.asm_wait_time(time_delay=1)
    ct.asm_log_message("exception combo 2 is starting")
    exception_catch_column_2 = insert_exception_catch_column(ct,"combo_2")
    insert_bad_main_column_heartbeat(ct,"combo_2_main_heartbeat")
    insert_good_recovery_column_heartbeat(ct,"combo_2_recovery_heartbeat")
    insert_good_finalize_column_heartbeat(ct,"combo_2_finalize_heartbeat")
    end_exception_catch_column_heartbeat(ct,exception_catch_column_2)
    ct.define_join_link(exception_catch_column_2)
    ct.asm_wait_time(time_delay=1)
   
    ct.asm_log_message("exception combo 4 is starting")
    exception_catch_column_4 = insert_exception_catch_column(ct,"combo_4")
    insert_good_main_column_heartbeat(ct,"combo_4_main_heartbeat")
    insert_good_recovery_column_heartbeat(ct,"combo_4_recovery_heartbeat")
    insert_bad_finalize_column_heartbeat(ct,"combo_4_finalize_heartbeat")
    end_exception_catch_column_heartbeat(ct,exception_catch_column_4)
    ct.define_join_link(exception_catch_column_4)
    
    ct.end_column(column_name=catch_all_exception_column)
    ct.define_join_link(catch_all_exception_column)
    ct.asm_log_message("launch column is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=launch_column)
    ct.end_test()   
    
    
def inner_state_machine(ct,name:str):
    
    launch_column = ct.define_column(column_name="launch_column",auto_start=True)
    ct.asm_log_message("launch column is starting")
    ct.asm_log_message("launching state machine 1")
    
    
    sm_envelope_column_1 = ct.define_sm_envelope(column_name="sm_envelope_column_1", auto_start=True)
    
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
    
    ct.end_column(column_name=sm_envelope_column_1)
    ct.define_join_link(sm_envelope_column_1)
    
    sm_name_2 = "state_machine_2"
    
    sm_envelope_column_2 = ct.define_sm_envelope(column_name="sm_envelope_column_1",auto_start=True)
    
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
    ct.end_column(column_name=sm_envelope_column_2)
    ct.define_join_link(sm_envelope_column_2)
    ct.asm_log_message("launch column is terminating")
    ct.asm_terminate()
    ct.end_column(column_name=launch_column)
    return launch_column
    
    
    
       
def ninteenth_test(ct,kb_name): # state machine
    ct.start_test(test_name=kb_name)
    launch_column = inner_state_machine(ct,"inner_state_machine")
    ct.end_test()



def add_header(yaml_file):
    yaml_file = Path(yaml_file)
    
    ct = ChainTreeMaster(yaml_file=yaml_file)
    return ct
    
   
    #ct.display_chain_tree_function_mapping()
    


if __name__ == "__main__":
    test_list = ["first_test","second_test","fourth_test","fifth_test","sixth_test","seventh_test","eighth_test","ninth_test",
                 "tenth_test","eleventh_test","twelfth_test","thirteenth_test","fourteenth_test","seventeenth_test","eighteenth_test","ninteenth_test"]
                
    test_dict = { "first_test": first_test}
    test_dict = { "first_test": first_test,
                 "second_test": second_test,
                 "fourth_test": fourth_test,
                 "fifth_test": fifth_test,
                 "sixth_test": sixth_test,
                 "seventh_test": seventh_test,
                 "eighth_test": eighth_test,
                 "ninth_test": ninth_test,
                 "tenth_test": tenth_test,
                 "eleventh_test": eleventh_test,
                 "twelfth_test": twelfth_test,
                 "thirteenth_test": thirteenth_test,
                 "fourteenth_test": fourteenth_test,
                 "seventeenth_test": seventeenth_test,
                 "eighteenth_test": eighteenth_test,
                 "ninteenth_test": ninteenth_test}
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
    
