from datetime import datetime, timedelta, timezone
import uuid
import psycopg2
from psycopg2.extras import RealDictCursor
from kb_query_support import KB_Search
from kb_status_data import KB_Status_Data
from kb_job_table import KB_Job_Queue
from kb_stream import KB_Stream
from kb_rpc_client import KB_RPC_Client
from kb_rpc_server import KB_RPC_Server
from kb_link_table import KB_Link_Table
from kb_link_mount_table import KB_Link_Mount_Table

class KB_Data_Structures:
    """
    A class to handle the data structures for the knowledge base.
    """
    def __init__(self, host, port, dbname, user, password, database):
        self.query_support = KB_Search(host, port, dbname, user, password, database)
        self.clear_filters = self.query_support.clear_filters
        self.search_label = self.query_support.search_label
        self.search_name = self.query_support.search_name
        self.search_property_key = self.query_support.search_property_key
        self.search_property_value = self.query_support.search_property_value
        self.search_has_link = self.query_support.search_has_link
        self.search_has_link_mount = self.query_support.search_has_link_mount
        self.search_path = self.query_support.search_path
        self.search_starting_path = self.query_support.search_starting_path
        self.execute_kb_search = self.query_support.execute_query
        self.find_description = self.query_support.find_description
        self.find_description_paths = self.query_support.find_description_paths
        self.find_path_values = self.query_support.find_path_values
        self.decode_link_nodes = self.query_support.decode_link_nodes

        self.status_data = KB_Status_Data(self.query_support, database)
        self.find_status_node_ids = self.status_data.find_node_ids
        self.find_status_node_id = self.status_data.find_node_id
        self.get_status_data = self.status_data.get_status_data
        self.set_status_data = self.status_data.set_status_data
        self.get_status_data = self.status_data.get_status_data
        
        self.job_queue = KB_Job_Queue(self.query_support, database)
        self.find_job_ids = self.job_queue.find_job_ids
        self.find_job_id = self.job_queue.find_job_id
        self.get_queued_number = self.job_queue.get_queued_number
        self.get_free_number = self.job_queue.get_free_number
        self.peak_job_data = self.job_queue.peak_job_data
        self.mark_job_completed = self.job_queue.mark_job_completed
        self.push_job_data = self.job_queue.push_job_data
        self.list_pending_jobs = self.job_queue.list_pending_jobs
        self.list_active_jobs = self.job_queue.list_active_jobs
        self.clear_job_queue = self.job_queue.clear_job_queue
        
        self.stream = KB_Stream(self.query_support, database)
        self.find_stream_ids = self.stream.find_stream_ids
        self.find_stream_id = self.stream.find_stream_id
        self.find_stream_table_keys = self.stream.find_stream_table_keys
        self.push_stream_data = self.stream.push_stream_data
        self.list_stream_data = self.stream.list_stream_data
        self.clear_stream_data = self.stream.clear_stream_data
        self.get_stream_data_count = self.stream.get_stream_data_count
        self.get_stream_data_range = self.stream.get_stream_data_range
        self.get_stream_statistics = self.stream.get_stream_statistics
        self.get_stream_data_by_id = self.stream.get_stream_data_by_id
        
        
        self.rpc_client = KB_RPC_Client(self.query_support, database)
        self.rpc_client_find_rpc_client_id = self.rpc_client.find_rpc_client_id
        self.rpc_client_find_rpc_client_ids = self.rpc_client.find_rpc_client_ids
        self.rpc_client_find_rpc_client_keys = self.rpc_client.find_rpc_client_keys
        self.rpc_client_find_free_slots = self.rpc_client.find_free_slots
        self.rpc_client_find_queued_slots = self.rpc_client.find_queued_slots
        self.rpc_client_peak_and_claim_reply_data = self.rpc_client.peak_and_claim_reply_data 
        self.rpc_client_clear_reply_queue = self.rpc_client.clear_reply_queue
        self.rpc_client_push_and_claim_reply_data = self.rpc_client.push_and_claim_reply_data
        self.rpc_client_list_waiting_jobs = self.rpc_client.list_waiting_jobs
        
        self.rpc_server = KB_RPC_Server(self.query_support, database)
        self.rpc_server_id_find = self.rpc_server.find_rpc_server_id
        self.rpc_server_ids_find = self.rpc_server.find_rpc_server_ids
        self.rpc_server_table_keys_find = self.rpc_server.find_rpc_server_table_keys
        
        
        self.rpc_server_list_jobs_job_types = self.rpc_server.list_jobs_job_types
        self.rpc_server_count_all_jobs = self.rpc_server.count_all_jobs
        self.rpc_server_count_empty_jobs = self.rpc_server.count_empty_jobs
        self.rpc_server_count_new_jobs = self.rpc_server.count_new_jobs
        self.rpc_server_count_processing_jobs = self.rpc_server.count_processing_jobs 
        self.rpc_server_count_jobs_job_types = self.rpc_server.count_jobs_job_types
        
        self.rpc_server_push_rpc_queue = self.rpc_server.push_rpc_queue
        self.rpc_server_peak_server_queue = self.rpc_server.peak_server_queue
        self.rpc_server_mark_job_completion = self.rpc_server.mark_job_completion
        self.rpc_server_mark_job_completion  = self.rpc_server.mark_job_completion
        self.rpc_server_clear_server_queue = self.rpc_server.clear_server_queue
        
        self.link_table = KB_Link_Table(self.query_support.conn, self.query_support.cursor, database)
        self.link_table_find_records_by_link_name = self.link_table.find_records_by_link_name
        self.link_table_find_records_by_node_path = self.link_table.find_records_by_node_path
        self.link_table_find_all_link_names = self.link_table.find_all_link_names
        self.link_table_find_all_node_names = self.link_table.find_all_node_names
        
        
        self.link_mount_table = KB_Link_Mount_Table(self.query_support.conn, self.query_support.cursor, database)
        self.link_mount_table_find_records_by_link_name = self.link_mount_table.find_records_by_link_name
        self.link_mount_table_find_records_by_mount_path = self.link_mount_table.find_records_by_mount_path
        self.link_mount_table_find_all_link_names = self.link_mount_table.find_all_link_names
        self.link_mount_table_find_all_mount_paths = self.link_mount_table.find_all_mount_paths
    
        

# Example usage:
if __name__ == "__main__":
    password = input("Enter PostgreSQL password: ")
    # Create a new KB_Search instance
    kb_data_structures = KB_Data_Structures(
        dbname="knowledge_base",
        user="gedgar",
        password=password,
        host="localhost",
        port="5432",
        database="knowledge_base"
    )   
    
    def test_server_functions(self, server_path):
        print("rpc_server_path", server_path)
        print("initial state")
    
        print("clear server queue")
        self.rpc_server_clear_server_queue(server_path)
        print("list_jobs_job_types", self.rpc_server_list_jobs_job_types(server_path, 'new_job'))
        
        request_id1 = str(uuid.uuid4())
        self.rpc_server_push_rpc_queue(server_path=server_path, request_id=request_id1, rpc_action="rpc_action1", request_payload={"data1": "data1"}, transaction_tag="transaction_tag_1",
                    priority=1, rpc_client_queue="rpc_client_queue", max_retries=5, wait_time=0.5)

        request_id2 = str(uuid.uuid4())
        self.rpc_server_push_rpc_queue(server_path=server_path, request_id=request_id2, rpc_action="rpc_action2", request_payload={"data2": "data1"}, transaction_tag="transaction_tag_2",
                    priority=2, rpc_client_queue="rpc_client_queue", max_retries=5, wait_time=0.5)
        print("list_jobs_job_types", self.rpc_server_list_jobs_job_types(server_path, 'new_job'))
        request_id3 = str(uuid.uuid4())
        self.rpc_server_push_rpc_queue(server_path=server_path, request_id=request_id3, rpc_action="rpc_action3", request_payload={"data3": "data1"}, transaction_tag="transaction_tag_3",
                    priority=3, rpc_client_queue="rpc_client_queue", max_retries=5, wait_time=0.5)
        print("list_jobs_job_types", self.rpc_server_list_jobs_job_types(server_path, 'new_job'))
        print("requst_id",request_id1, request_id2, request_id3)
        print("queued jobs", self.rpc_server_list_jobs_job_types(server_path, 'new_job') )
        print("server_path", server_path)
        job_data_1 = self.rpc_server_peak_server_queue(server_path)
        print("job_data_1", job_data_1)
       
        self.rpc_server_count_all_jobs(server_path)
        job_data_2 = self.rpc_server_peak_server_queue(server_path)
        print("job_data_2", job_data_2)
        
        self.rpc_server_count_all_jobs(server_path)
        job_data_3 = self.rpc_server_peak_server_queue(server_path)
        print("job_data_3", job_data_3)
        
        self.rpc_server_count_all_jobs(server_path)
        print("job_data_1['id']", job_data_1['id'])
        for i , j in job_data_1.items():
            print("i", i, "j", j)
        id1 = job_data_1['id']
        print("id1", id1)
        self.rpc_server_mark_job_completion(server_path, id1)
        print("count_all_jobs", self.rpc_server_count_all_jobs(server_path))
        id2 = job_data_2['id']
        self.rpc_server_mark_job_completion(server_path, id2)
        print("count_all_jobs", self.rpc_server_count_all_jobs(server_path))
        id3 = job_data_3['id']
        self.rpc_server_mark_job_completion(server_path, id3)
        print("count_all_jobs", self.rpc_server_count_all_jobs(server_path))
        

    def test_client_queue(self, client_path):
        """
        Test client queue operations in a specific sequence.
        
        Args:
            client_path (str): The path to the client to test
        """
        # Initial state
        print("=== Initial State ===")
        free_slots = self.rpc_client_find_free_slots(client_path)
        print(f"Number of free slots: {free_slots}")
        
        queued_slots = self.rpc_client_find_queued_slots(client_path)
        print(f"Number of queued slots: {queued_slots}")
        
        waiting_jobs = self.rpc_client_list_waiting_jobs(client_path)
        print(f"Waiting jobs: {waiting_jobs}")
        self.rpc_client_clear_reply_queue(client_path)
        free_slots = self.rpc_client_find_free_slots(client_path)
        print(f"Number of free slots: {free_slots}")
        
        queued_slots = self.rpc_client_find_queued_slots(client_path)
        print(f"Number of queued slots: {queued_slots}")
        
        waiting_jobs = self.rpc_client_list_waiting_jobs(client_path)
        print(f"Waiting jobs: {waiting_jobs}")
        # Push first set of reply data
        print("\n=== Pushing First Set of Reply Data ===")
        request_id1 = uuid.uuid4()
        self.rpc_client_push_and_claim_reply_data(
            client_path, 
            request_id1, 
            "xxx", 
            "Action1", 
            "xxx", 
            {"data1": "data1"}
        )
        print(f"Pushed reply data with request ID: {request_id1}")
        
        request_id2 = str(uuid.uuid4())
        self.rpc_client_push_and_claim_reply_data(
            client_path, 
            request_id2, 
            "xxx", 
            "Action2", 
            "yyy", 
            {"data2": "data2"}
        )
        print(f"Pushed reply data with request ID: {request_id2}")
        
        # After first push
        print("\n=== After First Push ===")
        free_slots = self.rpc_client_find_free_slots(client_path)
        print(f"Number of free slots: {free_slots}")
        
        queued_slots = self.rpc_client_find_queued_slots(client_path)
        print(f"Number of queued slots: {queued_slots}")
        
        waiting_jobs = self.rpc_client_list_waiting_jobs(client_path)
        print(f"Waiting jobs: {waiting_jobs}")
        
        # Peek and release first data
        print("\n=== Peek and Release First Data ===")
        peak_data = self.rpc_client_peak_and_claim_reply_data(client_path)
        print(f"Peek data: {peak_data}")
        free_slots = self.rpc_client_find_free_slots(client_path)
        print(f"Number of free slots: {free_slots}")
        
        queued_slots = self.rpc_client_find_queued_slots(client_path)
        print(f"Number of queued slots: {queued_slots}")
        
        waiting_jobs = self.rpc_client_list_waiting_jobs(client_path)
        print(f"Waiting jobs: {waiting_jobs}")

        # Repeated check of queued slots and waiting jobs
        print("\n=== Additional Queue Check ===")
        queued_slots = self.rpc_client_find_queued_slots(client_path)
        print(f"Number of queued slots: {queued_slots}")
        
        waiting_jobs = self.rpc_client_list_waiting_jobs(client_path)
        print(f"Waiting jobs: {waiting_jobs}",waiting_jobs)
        
        # Peek and release second data
        print("\n=== Peek and Release Second Data ===")
        peak_data = self.rpc_client_peak_and_claim_reply_data(client_path)
        print(f"Peek data: {peak_data}")
        
    
        # After second release
        print("\n=== After Second Release ===")
        free_slots = self.rpc_client_find_free_slots(client_path)
        print(f"Number of free slots: {free_slots}")
        
        queued_slots = self.rpc_client_find_queued_slots(client_path)
        print(f"Number of queued slots: {queued_slots}")
        
        waiting_jobs = self.rpc_client_list_waiting_jobs(client_path)
        print(f"Waiting jobs: {waiting_jobs}")
        
        
        
        # After second release
        print("\n=== After Second Release ===")
        free_slots = self.rpc_client_find_free_slots(client_path)
        print(f"Number of free slots: {free_slots}")
        
        queued_slots = self.rpc_client_find_queued_slots(client_path)
        print(f"Number of queued slots: {queued_slots}")
        
        waiting_jobs = self.rpc_client_list_waiting_jobs(client_path)
        print(f"Waiting jobs: {waiting_jobs}")
        
        # Push second set of reply data
        print("\n=== Pushing Second Set of Reply Data ===")
        request_id3 = str(uuid.uuid4())
        self.rpc_client_push_and_claim_reply_data(
            client_path, 
            request_id3, 
            "xxx", 
            "Action1", 
            "xxx", 
            {"data1": "data1"}
        )
        print(f"Pushed reply data with request ID: {request_id3}")
        
        request_id4 = str(uuid.uuid4())
        self.rpc_client_push_and_claim_reply_data(
            client_path, 
            request_id4, 
            "xxx", 
            "Action2", 
            "yyy", 
            {"data2": "data2"}
        )
        print(f"Pushed reply data with request ID: {request_id4}")
        
        # After second push
        print("\n=== After Second Push ===")
        free_slots = self.rpc_client_find_free_slots(client_path)
        print(f"Number of free slots: {free_slots}")
        
        queued_slots = self.rpc_client_find_queued_slots(client_path)
        print(f"Number of queued slots: {queued_slots}")
        
        waiting_jobs = self.rpc_client_list_waiting_jobs(client_path)
        print(f"Waiting jobs: {waiting_jobs}")
        
        # Clear reply queue
        print("\n=== Clearing Reply Queue ===")
        self.rpc_client_clear_reply_queue(client_path)
        print(f"Cleared reply queue for client path: {client_path}")
        
        # Final state
        print("\n=== Final State After Clear ===")
        free_slots = self.rpc_client_find_free_slots(client_path)
        print(f"Number of free slots: {free_slots}")
        
        queued_slots = self.rpc_client_find_queued_slots(client_path)
        print(f"Number of queued slots: {queued_slots}")
        
        waiting_jobs = self.rpc_client_list_waiting_jobs(client_path)
        print(f"Waiting jobs: {waiting_jobs}")
        
        print("\n=== Test Complete ===")

    """
    Status Data  
    This test demonstrates the use of the status data functions.
    It first finds all the status nodes in the knowledge base, then finds the path values for each node.
    It then finds the description for each node, and the data for each node.
    It then sets the data for each node.
    It then finds the data for each node again.
    It then deletes the data for each node.
    It then finds the data for each node again.
    """
    print("\n\n\n***************************  status data ***************************\n\n\n")
   
    print("find all status nodes in the knowledge base")
    node_ids = kb_data_structures.find_status_node_ids(None,None,None,None)
    print("node_ids", node_ids)
    path_values = kb_data_structures.find_path_values(node_ids)
    print("path_values", path_values)
    
    print("\n\n\n find a specific status node in the knowledge base\n\n\n")
    
    node_id = kb_data_structures.find_status_node_id(
        kb="kb1",
        node_name='info2_status',
        properties={'prop3': 'val3'},
        node_path='*.header1_link.header1_name.KB_STATUS_FIELD.info2_status'
    )
    
    print("node_id", node_id)
    path_values = kb_data_structures.find_path_values(node_id)
    
    print("path of specific status node", path_values[0])
    description = kb_data_structures.find_description(node_id)
    print("description of specific status node", description)
    
    data = kb_data_structures.get_status_data(path_values[0])
    print("initial data of specific status node", data)

    kb_data_structures.set_status_data(path_values[0], {"prop1": "val1", "prop2": "val2"})
    data = kb_data_structures.get_status_data(path_values[0])
    print("data after setting data", data)
    print("ending status data test")
    
    """
    Job Queue
    This test demonstrates the use of job queue functions
    """
    print("***************************  job queue data ***************************")
    
    print("find all job queues in the knowledge base")
    node_ids = kb_data_structures.find_job_ids(kb=None,node_name=None, properties=None, node_path=None)
    
    
    job_table_paths = kb_data_structures.find_path_values(node_ids)
    print("job table paths", job_table_paths)
    print("\n\n\n")
    job_path = job_table_paths[0]
    print("first job path", job_path)
    print("clear job queue")
    kb_data_structures.clear_job_queue(job_path)
    
    
    queued_number = kb_data_structures.get_queued_number(job_path)
    print("queued_number", queued_number)
    
    
    free_number = kb_data_structures.get_free_number(job_path)
    print("free_number", free_number)
    
    print("peak empty job queue", kb_data_structures.peak_job_data(job_path))
    
    
    print("push_job_data")
    
    kb_data_structures.push_job_data(job_path, {"prop1": "val1", "prop2": "val2"})
    queued_number = kb_data_structures.get_queued_number(job_path)
    print("queued_number", queued_number)
    
    free_number = kb_data_structures.get_free_number(job_path)
    print("free_number", free_number)
    
    
    print("list_pending_jobs", kb_data_structures.list_pending_jobs(job_path))
    
    print("list_active_jobs", kb_data_structures.list_active_jobs(job_path))
   
    
    row_data = kb_data_structures.peak_job_data(job_path)
    job_id = row_data['id']
    print("job_id", job_id)
    print("job_data", row_data['data'])
    
    
    free_number = kb_data_structures.get_free_number(job_path)
    print("free_number", free_number)
    
    print("list_pending_jobs", kb_data_structures.list_pending_jobs(job_path))
    print("list_active_jobs", kb_data_structures.list_active_jobs(job_path))
    
    kb_data_structures.mark_job_completed(job_id)
    free_number = kb_data_structures.get_free_number(job_path)
    print("free_number", free_number)
    
    print("list_pending_jobs", kb_data_structures.list_pending_jobs(job_path))
    print("list_active_jobs", kb_data_structures.list_active_jobs(job_path))
    print("peak_job_data", kb_data_structures.peak_job_data(job_path))
    
    kb_data_structures.clear_job_queue(job_path)
    free_number = kb_data_structures.get_free_number(job_path)
    print("free_number", free_number)
    
    """
    Stream tables
    """
    print("***************************  stream data ***************************")
    
    node_ids = kb_data_structures.find_stream_ids(kb="kb1", node_name="info1_stream", properties=None, node_path=None)
   
    stream_table_keys = kb_data_structures.find_stream_table_keys(node_ids)
    print("stream_table_keys", stream_table_keys)
    
    descriptions = kb_data_structures.find_description_paths(stream_table_keys)
    print("descriptions", descriptions)
    
    kb_data_structures.clear_stream_data(stream_table_keys[0])
    kb_data_structures.push_stream_data(stream_table_keys[0], {"prop1": "val1", "prop2": "val2"})
    print("list_stream_data", kb_data_structures.list_stream_data(stream_table_keys[0]))
    
    past_timestamp = datetime.now(timezone.utc) - timedelta(minutes=15)
    before_timestamp = datetime.now(timezone.utc)
    print("past_timestamp", past_timestamp)
    print("past data")
    print("list_stream_data", kb_data_structures.list_stream_data(stream_table_keys[0], recorded_after=past_timestamp, recorded_before=before_timestamp))
    
    """
    RPC Functions
    """
    print("***************************  RPC Functions ***************************")
    
    node_ids = kb_data_structures.rpc_client_find_rpc_client_ids(node_name=None, properties=None, node_path=None)
    print("rpc_client_node_ids", node_ids)
    
    node_ids = kb_data_structures.rpc_client_find_rpc_client_ids(node_name=None, properties=None, node_path=None)
    print("rpc_client_node_ids", node_ids)
    
    client_keys = kb_data_structures.rpc_client_find_rpc_client_keys(node_ids)
    print("client_keys", client_keys)
    
    client_descriptions = kb_data_structures.find_description_paths(client_keys)
    print("client_descriptions", client_descriptions)
    
    test_client_queue(kb_data_structures, client_keys[0])
    
    
   
    
    node_ids = kb_data_structures.rpc_server_id_find(node_name=None, properties=None, node_path=None)
    print("rpc_server_node_ids", node_ids)   
    
    server_keys = kb_data_structures.rpc_server_table_keys_find(node_ids)
    print("server_keys", server_keys)    
    
    server_descriptions = kb_data_structures.find_description_paths(server_keys)
    print("server_descriptions", server_descriptions)    
    
    test_server_functions(kb_data_structures, server_keys[0])
    
    """
    Link Tables
    """
    print("--------------------------------search_starting_path--------------------------------")
    kb_data_structures.clear_filters()
    kb_data_structures.search_starting_path("kb1.header1_link.header1_name")
    results = kb_data_structures.execute_kb_search()
    print("------------------results", results)
    kb_data_structures.clear_filters()
    kb_data_structures.search_starting_path("kb1.header1_link.header1_name.KB_LINK_NODE.info1_link_mount")
    results = kb_data_structures.execute_kb_search()
    print("------------------results", results)
    kb_data_structures.clear_filters()
    kb_data_structures.search_starting_path("kb1")
    results = kb_data_structures.execute_kb_search()
    print("------------------results", results)
    print("--------------------------------decode_link_nodes--------------------------------")
    for data in results:
        path = data['path']
        print(kb_data_structures.decode_link_nodes(path))
  
    print("***************************  Link Tables ***************************")
    kb_data_structures.clear_filters()
    kb_data_structures.search_has_link()
    results = kb_data_structures.execute_kb_search()
    print("results", results)
    
    
    
   
    print("--------------------------------link_mount_table--------------------------------")
    kb_data_structures.clear_filters()
    kb_data_structures.search_has_link_mount()
    results = kb_data_structures.execute_kb_search()
    print("results", results)
    kb_data_structures.clear_filters()
    
    print("--------------------------------link_table db --------------------------------")
    names = kb_data_structures.link_table_find_all_link_names()
    print("find_all_link_names", names)
    mounts = kb_data_structures.link_table_find_all_node_names()
    print("find_all_node_names", mounts)
    print("find_records_by_link_name", kb_data_structures.link_table_find_records_by_link_name(names[0]))
    print("find_records_by_link_name", kb_data_structures.link_table_find_records_by_link_name(names[0], kb="kb1"))
    print("find_records_by_node_path", kb_data_structures.link_table_find_records_by_node_path(mounts[0]))
    print("find_records_by_node_path", kb_data_structures.link_table_find_records_by_node_path(mounts[0], kb="kb1"))
    
    print("--------------------------------link_mount_table db --------------------------------")
    names = kb_data_structures.link_mount_table_find_all_link_names()
    print("find_all_link_names", names)
    mounts = kb_data_structures.link_mount_table_find_all_mount_paths()
    print("find_all_mount_points", mounts)
    print("find_records_by_link_name", kb_data_structures.link_mount_table_find_records_by_link_name(names[0]))
    print("find_records_by_link_name", kb_data_structures.link_mount_table_find_records_by_link_name(names[0], kb="kb1"))
    print("find_records_by_mount_path", kb_data_structures.link_mount_table_find_records_by_mount_path(mounts[0]))
    print("find_records_by_mount_path", kb_data_structures.link_mount_table_find_records_by_mount_path(mounts[0], kb="kb1"))
    
    
    
    
    kb_data_structures.query_support.disconnect()