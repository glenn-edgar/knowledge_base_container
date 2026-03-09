#!/usr/bin/env luajit
--[[
  test_driver.lua - Integration test for all KB_Data_Structures modules.
  Direct port of the Python test_driver.py.

  Usage:
    POSTGRES_PASSWORD=secret luajit test_driver.lua
]]

local dkjson = require("dkjson")
local KB_Data_Structures = require("kb_data_structures")

---------------------------------------------------------------------------
-- Helpers
---------------------------------------------------------------------------

local function tprint(t, indent)
  indent = indent or ""
  if type(t) ~= "table" then
    print(indent .. tostring(t))
    return
  end
  for k, v in pairs(t) do
    if type(v) == "table" then
      print(indent .. tostring(k) .. ":")
      tprint(v, indent .. "  ")
    else
      print(indent .. tostring(k) .. " = " .. tostring(v))
    end
  end
end

local function uuid4()
  local template = "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx"
  math.randomseed(os.clock() * 1e6 + os.time())
  return (template:gsub("[xy]", function(c)
    local v = (c == "x") and math.random(0, 0xf) or math.random(8, 0xb)
    return string.format("%x", v)
  end))
end

local function banner(msg)
  print("\n***************************  " .. msg .. "  ***************************")
end

local function section(msg)
  print("\n=== " .. msg .. " ===")
end

local function line(msg)
  print("  " .. msg)
end

---------------------------------------------------------------------------
-- Test: Bit Structures
---------------------------------------------------------------------------

local function print_bit_data_class(dc)
  print("  node_id      = " .. tostring(dc.node_id))
  print("  user_name    = " .. tostring(dc.user_name))
  print("  bit_size     = " .. tostring(dc.bit_size))
  print("  bit_mask     = " .. tostring(dc.bit_mask))
  print("  flags        = " .. dkjson.encode(dc.flags))
  print("  flag_data    = " .. dkjson.encode(dc.flag_data))
  print("  flag_change  = " .. dkjson.encode(dc.flag_change))
end

local function test_bit_structures(kb)
  banner("Bit Structures")

  local node_ids = kb:find_bit_structure_ids(nil, "info1_bit_mask", nil, nil)
  -- properties comes back as a JSON string from DBI, decode it
  local props = node_ids[1].properties
  if type(props) == "string" then props = dkjson.decode(props) or {} end
  local node_id = props.record_id
  print("record_id: " .. tostring(node_id))

  kb:set_all_ones(node_id)
  kb:set_all_zeros(node_id)

  local bit_data = kb:find_assemble_bit_data(node_ids, false, { "user_1" })
  local data_class
  for user_name, dc in pairs(bit_data) do
    print("@@@@@@@@@@@ user_name: " .. user_name)
    print_bit_data_class(dc)
    data_class = dc
  end

  kb:set_flag_data(data_class, { F = 1, G = 0, H = 0, I = 0, J = 0 })
  kb:get_flag_data(data_class)
  section("After set F=1, G=0, H=0, I=0, J=0")
  print_bit_data_class(data_class)

  kb:set_flag_data(data_class, { J = 1 })
  kb:get_flag_data(data_class)
  section("After set J=1")
  print_bit_data_class(data_class)

  -- S-expression tests
  local sexpr = "(or (and user_1:F user_1:G user_1:H user_1:I user_1:J) (and user_1:F user_1:J))"
  print("s-expr: " .. sexpr)
  local tokens = kb:s_tokenize(sexpr)
  local result = kb:s_execute(tokens, bit_data)
  print("result: " .. tostring(result))

  local expr2 = "(bit_changed  user_1:J)"
  print("s-expr: " .. expr2)
  tokens = kb:s_tokenize(expr2)
  result = kb:s_execute(tokens, bit_data)
  print("result: " .. tostring(result))

  local expr3 = "(bit_changed  user_1:F)"
  print("s-expr: " .. expr3)
  tokens = kb:s_tokenize(expr3)
  result = kb:s_execute(tokens, bit_data)
  print("result: " .. tostring(result))

  banner("Bit Structures test complete")
end

---------------------------------------------------------------------------
-- Test: Document Table
---------------------------------------------------------------------------

local function test_document_table(kb, label_field)
  banner("Document Table (" .. label_field .. ")")

  local node_ids = kb:find_document_ids(nil, label_field, nil, nil)
  local node_id = tostring(node_ids[1].path)
  print("document path: " .. tostring(node_id))

  local value = {
    name    = "Test",
    role    = "admin",
    tags    = { "python", "postgres" },
    address = { city = "LA", zip = "90001" },
  }
  kb:jsonb_set(node_id, "{}", value)
  print("jsonb_get({}):")
  tprint(kb:jsonb_get(node_id, "{}"), "  ")

  local name_json = kb:jsonb_get(node_id, "name", { as_text = false })
  print("Get name (JSON): " .. tostring(name_json))
  local name_text = kb:jsonb_get(node_id, "name", { as_text = true })
  print("Get name (text): " .. tostring(name_text))
  local city = kb:jsonb_get(node_id, "address.city", { as_text = true })
  print("Get nested city: " .. tostring(city))

  -- Key checks
  print("Has 'role' key: " .. tostring(kb:jsonb_has_key(node_id, "role")))
  print("Has any ['role','nonexistent']: " .. tostring(kb:jsonb_has_any_keys(node_id, { "role", "nonexistent" })))
  print("Has all ['name','role']: " .. tostring(kb:jsonb_has_all_keys(node_id, { "name", "role" })))
  print("Has all ['name','nonexistent']: " .. tostring(kb:jsonb_has_all_keys(node_id, { "name", "nonexistent" })))

  -- Containment
  print("Contains {role=admin}: " .. tostring(kb:jsonb_contains(node_id, { role = "admin" })))
  print("Contains {role=user}: " .. tostring(kb:jsonb_contains(node_id, { role = "user" })))
  print("Contained by superset: " .. tostring(kb:jsonb_contained_by(node_id, {
    name = "Test", role = "admin",
    tags = { "python", "postgres" },
    address = { city = "LA", zip = "90001" },
    extra = "field",
  })))

  -- Array contains
  print("Tags contain 'python': " .. tostring(kb:jsonb_array_contains(node_id, "tags", "python")))
  print("Tags contain 'ruby': " .. tostring(kb:jsonb_array_contains(node_id, "tags", "ruby")))

  -- JSON path
  print("Path exists (role==admin): " .. tostring(kb:jsonb_path_exists(node_id, '$.role ? (@ == "admin")')))
  local tags = kb:jsonb_path_query(node_id, '$.tags[*]')
  print("Query tags array:")
  tprint(tags, "  ")

  -- Set and delete
  kb:jsonb_set(node_id, "status", "active")
  print("Set status: " .. tostring(kb:jsonb_get(node_id, "status", { as_text = true })))
  kb:jsonb_delete_key(node_id, "status")
  print("After delete status: " .. tostring(kb:jsonb_get(node_id, "status")))
  kb:jsonb_delete_path(node_id, "address.zip")
  print("After delete address.zip: " .. tostring(kb:jsonb_get(node_id, "address.zip")))

  -- Array elements
  local elements = kb:jsonb_array_elements(node_id, "tags")
  print("Expanded tags:")
  tprint(elements, "  ")

  -- Queue (FIFO)
  section("Queue Operations (FIFO)")
  kb:jsonb_enqueue(node_id, { task = "Task 1", priority = 1 })
  print("  Enqueued Task 1")
  kb:jsonb_enqueue(node_id, { task = "Task 2", priority = 2 })
  print("  Enqueued Task 2")
  kb:jsonb_enqueue(node_id, { task = "Task 3", priority = 3 })
  print("  Enqueued Task 3")
  print("  Queue size: " .. tostring(kb:jsonb_size(node_id)))

  local item = kb:jsonb_dequeue(node_id)
  print("  Dequeued:")
  tprint(item, "    ")
  item = kb:jsonb_peek(node_id)
  print("  Peeked:")
  tprint(item, "    ")
  print("  Queue size after dequeue: " .. tostring(kb:jsonb_size(node_id)))

  -- Stack (LIFO)
  section("Stack Operations (LIFO)")
  kb:jsonb_push(node_id, { message = "First" })
  print("  Pushed 'First'")
  kb:jsonb_push(node_id, { message = "Second" })
  print("  Pushed 'Second'")
  kb:jsonb_push(node_id, { message = "Third" })
  print("  Pushed 'Third'")
  item = kb:jsonb_pop(node_id)
  print("  Popped (LIFO):")
  tprint(item, "    ")
  item = kb:jsonb_pop(node_id)
  print("  Popped (LIFO):")
  tprint(item, "    ")
  print("  Stack size: " .. tostring(kb:jsonb_size(node_id)))

  -- Edge cases
  section("Edge Cases")
  kb:jsonb_clear(node_id)
  item = kb:jsonb_dequeue(node_id)
  print("  Dequeue from empty: " .. tostring(item))
  item = kb:jsonb_pop(node_id)
  print("  Pop from empty: " .. tostring(item))
  kb:jsonb_enqueue(node_id, { data = "test" })
  item = kb:jsonb_peek(node_id, 10)
  print("  Peek at invalid index: " .. tostring(item))
  item = kb:jsonb_peek(node_id, -1)
  print("  Peek at negative index: " .. tostring(item))

  banner("Document Table test complete (" .. label_field .. ")")
end

---------------------------------------------------------------------------
-- Test: Status Data
---------------------------------------------------------------------------

local function test_status_data(kb)
  banner("Status Data")

  print("Find all status nodes:")
  local node_ids = kb:find_status_node_ids(nil, nil, nil, nil)
  tprint(node_ids, "  ")

  local path_values = kb:find_path_values(node_ids)
  print("path_values:")
  tprint(path_values, "  ")

  section("Find specific status node")
  local node_id = kb:find_status_node_id(
    "kb1",
    "info2_status",
    { prop3 = "val3" },
    "*.header1_link.header1_name.KB_STATUS_FIELD.info2_status"
  )
  local pv = kb:find_path_values(node_id)
  print("path: " .. tostring(pv[1]))
  local desc = kb:find_description(node_id)
  print("description:")
  tprint(desc, "  ")

  local data = kb:get_status_data(pv[1])
  print("initial data:")
  tprint(data, "  ")

  kb:set_status_data(pv[1], { prop1 = "val1", prop2 = "val2" })
  data = kb:get_status_data(pv[1])
  print("data after set:")
  tprint(data, "  ")

  print("Ending status data test")
end

---------------------------------------------------------------------------
-- Test: Job Queue
---------------------------------------------------------------------------

local function test_job_queue(kb)
  banner("Job Queue")

  print("Find all job queues:")
  local node_ids = kb:find_job_ids(nil, nil, nil, nil)
  local job_table_paths = kb:find_path_values(node_ids)
  print("job table paths:")
  tprint(job_table_paths, "  ")

  local job_path = job_table_paths[1]
  print("first job path: " .. tostring(job_path))

  print("clear job queue")
  kb:clear_job_queue(job_path)

  print("queued_number: " .. tostring(kb:get_queued_number(job_path)))
  print("free_number:   " .. tostring(kb:get_free_number(job_path)))

  print("peak empty job queue: " .. tostring(kb:peak_job_data(job_path)))

  print("push_job_data")
  kb:push_job_data(job_path, { prop1 = "val1", prop2 = "val2" })
  print("queued_number: " .. tostring(kb:get_queued_number(job_path)))
  print("free_number:   " .. tostring(kb:get_free_number(job_path)))

  print("list_pending_jobs:")
  tprint(kb:list_pending_jobs(job_path), "  ")
  print("list_active_jobs:")
  tprint(kb:list_active_jobs(job_path), "  ")

  local row_data = kb:peak_job_data(job_path)
  local job_id = row_data.id
  print("job_id: " .. tostring(job_id))
  print("job data:")
  tprint(row_data.data, "  ")

  print("free_number: " .. tostring(kb:get_free_number(job_path)))
  print("list_pending_jobs:")
  tprint(kb:list_pending_jobs(job_path), "  ")
  print("list_active_jobs:")
  tprint(kb:list_active_jobs(job_path), "  ")

  kb:mark_job_completed(job_id)
  print("free_number after complete: " .. tostring(kb:get_free_number(job_path)))
  print("list_pending_jobs:")
  tprint(kb:list_pending_jobs(job_path), "  ")
  print("list_active_jobs:")
  tprint(kb:list_active_jobs(job_path), "  ")
  print("peak_job_data: " .. tostring(kb:peak_job_data(job_path)))

  kb:clear_job_queue(job_path)
  print("free_number after clear: " .. tostring(kb:get_free_number(job_path)))
end

---------------------------------------------------------------------------
-- Test: Stream
---------------------------------------------------------------------------

local function test_stream(kb)
  banner("Stream Data")

  local node_ids = kb:find_stream_ids("kb1", "info1_stream", nil, nil)
  local stream_keys = kb:find_stream_table_keys(node_ids)
  print("stream_table_keys:")
  tprint(stream_keys, "  ")

  local descriptions = kb:find_description_paths(stream_keys)
  print("descriptions:")
  tprint(descriptions, "  ")

  kb:clear_stream_data(stream_keys[1])
  kb:push_stream_data(stream_keys[1], { prop1 = "val1", prop2 = "val2" })
  print("list_stream_data:")
  tprint(kb:list_stream_data(stream_keys[1]), "  ")

  -- Time-based query (15 min window)
  local now = os.date("!%Y-%m-%dT%H:%M:%SZ")
  local past = os.date("!%Y-%m-%dT%H:%M:%SZ", os.time() - 900)
  print("past_timestamp: " .. past)
  print("list_stream_data (time range):")
  tprint(kb:list_stream_data(stream_keys[1], past, now), "  ")
end

---------------------------------------------------------------------------
-- Test: RPC Client Queue
---------------------------------------------------------------------------

local function test_client_queue(kb, client_path)
  banner("RPC Client Queue")
  print("client_path: " .. tostring(client_path))

  section("Initial State")
  print("  free slots:   " .. tostring(kb:rpc_client_find_free_slots(client_path)))
  print("  queued slots: " .. tostring(kb:rpc_client_find_queued_slots(client_path)))
  print("  waiting jobs:")
  tprint(kb:rpc_client_list_waiting_jobs(client_path), "    ")

  kb:rpc_client_clear_reply_queue(client_path)
  print("  free slots after clear:   " .. tostring(kb:rpc_client_find_free_slots(client_path)))
  print("  queued slots after clear: " .. tostring(kb:rpc_client_find_queued_slots(client_path)))

  section("Pushing First Set of Reply Data")
  local req1 = uuid4()
  kb:rpc_client_push_and_claim_reply_data(client_path, req1, "xxx", "Action1", "xxx", { data1 = "data1" })
  print("  Pushed request: " .. req1)

  local req2 = uuid4()
  kb:rpc_client_push_and_claim_reply_data(client_path, req2, "xxx", "Action2", "yyy", { data2 = "data2" })
  print("  Pushed request: " .. req2)

  section("After First Push")
  print("  free slots:   " .. tostring(kb:rpc_client_find_free_slots(client_path)))
  print("  queued slots: " .. tostring(kb:rpc_client_find_queued_slots(client_path)))
  print("  waiting jobs:")
  tprint(kb:rpc_client_list_waiting_jobs(client_path), "    ")

  section("Peek and Release First Data")
  local peak = kb:rpc_client_peak_and_claim_reply_data(client_path)
  print("  peek data:")
  tprint(peak, "    ")
  print("  free slots:   " .. tostring(kb:rpc_client_find_free_slots(client_path)))
  print("  queued slots: " .. tostring(kb:rpc_client_find_queued_slots(client_path)))

  section("Peek and Release Second Data")
  peak = kb:rpc_client_peak_and_claim_reply_data(client_path)
  print("  peek data:")
  tprint(peak, "    ")

  section("After Second Release")
  print("  free slots:   " .. tostring(kb:rpc_client_find_free_slots(client_path)))
  print("  queued slots: " .. tostring(kb:rpc_client_find_queued_slots(client_path)))

  section("Pushing Second Set of Reply Data")
  local req3 = uuid4()
  kb:rpc_client_push_and_claim_reply_data(client_path, req3, "xxx", "Action1", "xxx", { data1 = "data1" })
  print("  Pushed request: " .. req3)

  local req4 = uuid4()
  kb:rpc_client_push_and_claim_reply_data(client_path, req4, "xxx", "Action2", "yyy", { data2 = "data2" })
  print("  Pushed request: " .. req4)

  section("After Second Push")
  print("  free slots:   " .. tostring(kb:rpc_client_find_free_slots(client_path)))
  print("  queued slots: " .. tostring(kb:rpc_client_find_queued_slots(client_path)))

  section("Clearing Reply Queue")
  kb:rpc_client_clear_reply_queue(client_path)

  section("Final State After Clear")
  print("  free slots:   " .. tostring(kb:rpc_client_find_free_slots(client_path)))
  print("  queued slots: " .. tostring(kb:rpc_client_find_queued_slots(client_path)))
  print("  waiting jobs:")
  tprint(kb:rpc_client_list_waiting_jobs(client_path), "    ")

  section("Test Complete")
end

---------------------------------------------------------------------------
-- Test: RPC Server Functions
---------------------------------------------------------------------------

local function test_server_functions(kb, server_path)
  banner("RPC Server Functions")
  print("server_path: " .. tostring(server_path))

  print("clear server queue")
  kb:rpc_server_clear_server_queue(server_path)
  print("new_job list:")
  tprint(kb:rpc_server_list_jobs_job_types(server_path, "new_job"), "  ")

  -- Push 3 jobs
  local req1 = uuid4()
  kb:rpc_server_push_rpc_queue(server_path, req1, "rpc_action1",
    { data1 = "data1" }, "transaction_tag_1", 1, "rpc_client_queue", 5, 0.5)
  print("pushed job 1: " .. req1)

  local req2 = uuid4()
  kb:rpc_server_push_rpc_queue(server_path, req2, "rpc_action2",
    { data2 = "data1" }, "transaction_tag_2", 2, "rpc_client_queue", 5, 0.5)
  print("pushed job 2: " .. req2)

  print("new_job list after 2 pushes:")
  tprint(kb:rpc_server_list_jobs_job_types(server_path, "new_job"), "  ")

  local req3 = uuid4()
  kb:rpc_server_push_rpc_queue(server_path, req3, "rpc_action3",
    { data3 = "data1" }, "transaction_tag_3", 3, "rpc_client_queue", 5, 0.5)
  print("pushed job 3: " .. req3)

  print("new_job list after 3 pushes:")
  tprint(kb:rpc_server_list_jobs_job_types(server_path, "new_job"), "  ")

  -- Peak 3 jobs
  local job1 = kb:rpc_server_peak_server_queue(server_path)
  print("job_data_1:")
  tprint(job1, "  ")
  print("count_all_jobs:")
  tprint(kb:rpc_server_count_all_jobs(server_path), "  ")

  local job2 = kb:rpc_server_peak_server_queue(server_path)
  print("job_data_2:")
  tprint(job2, "  ")
  print("count_all_jobs:")
  tprint(kb:rpc_server_count_all_jobs(server_path), "  ")

  local job3 = kb:rpc_server_peak_server_queue(server_path)
  print("job_data_3:")
  tprint(job3, "  ")
  print("count_all_jobs:")
  tprint(kb:rpc_server_count_all_jobs(server_path), "  ")

  -- Mark completions
  kb:rpc_server_mark_job_completion(server_path, job1.id)
  print("count_all_jobs after completing job1:")
  tprint(kb:rpc_server_count_all_jobs(server_path), "  ")

  kb:rpc_server_mark_job_completion(server_path, job2.id)
  print("count_all_jobs after completing job2:")
  tprint(kb:rpc_server_count_all_jobs(server_path), "  ")

  kb:rpc_server_mark_job_completion(server_path, job3.id)
  print("count_all_jobs after completing job3:")
  tprint(kb:rpc_server_count_all_jobs(server_path), "  ")
end

---------------------------------------------------------------------------
-- Test: Link Tables
---------------------------------------------------------------------------

local function test_link_tables(kb)
  banner("Link Tables")

  section("search_starting_path")
  kb:clear_filters()
  kb:search_starting_path("kb1.header1_link.header1_name")
  local results = kb:execute_kb_search()
  print("results:")
  tprint(results, "  ")

  kb:clear_filters()
  kb:search_starting_path("kb1.header1_link.header1_name.KB_LINK_NODE.info1_link_mount")
  results = kb:execute_kb_search()
  print("results:")
  tprint(results, "  ")

  kb:clear_filters()
  kb:search_starting_path("kb1")
  results = kb:execute_kb_search()
  print("results:")
  tprint(results, "  ")

  section("decode_link_nodes")
  for _, data in ipairs(results) do
    local p = tostring(data.path)
    local ok2, kb_name, link_pairs = pcall(function() return kb:decode_link_nodes(p) end)
    if ok2 then
      print("  " .. p .. " => kb=" .. tostring(kb_name) .. " pairs=" .. dkjson.encode(link_pairs))
    else
      print("  " .. p .. " => (skip: " .. tostring(kb_name) .. ")")
    end
  end

  section("search_has_link")
  kb:clear_filters()
  kb:search_has_link()
  results = kb:execute_kb_search()
  print("results:")
  tprint(results, "  ")

  section("search_has_link_mount")
  kb:clear_filters()
  kb:search_has_link_mount()
  results = kb:execute_kb_search()
  print("results:")
  tprint(results, "  ")

  section("link_table db")
  local names = kb:link_table_find_all_link_names()
  print("all link names:")
  tprint(names, "  ")
  local mounts = kb:link_table_find_all_node_names()
  print("all node names:")
  tprint(mounts, "  ")

  if #names > 0 then
    print("records by link_name:")
    tprint(kb:link_table_find_records_by_link_name(names[1]), "  ")
    print("records by link_name (kb=kb1):")
    tprint(kb:link_table_find_records_by_link_name(names[1], "kb1"), "  ")
  end
  if #mounts > 0 then
    print("records by node_path:")
    tprint(kb:link_table_find_records_by_node_path(mounts[1]), "  ")
    print("records by node_path (kb=kb1):")
    tprint(kb:link_table_find_records_by_node_path(mounts[1], "kb1"), "  ")
  end

  section("link_mount_table db")
  names = kb:link_mount_table_find_all_link_names()
  print("all link names:")
  tprint(names, "  ")
  mounts = kb:link_mount_table_find_all_mount_paths()
  print("all mount paths:")
  tprint(mounts, "  ")

  if #names > 0 then
    print("records by link_name:")
    tprint(kb:link_mount_table_find_records_by_link_name(names[1]), "  ")
    print("records by link_name (kb=kb1):")
    tprint(kb:link_mount_table_find_records_by_link_name(names[1], "kb1"), "  ")
  end
  if #mounts > 0 then
    print("records by mount_path:")
    tprint(kb:link_mount_table_find_records_by_mount_path(mounts[1]), "  ")
    print("records by mount_path (kb=kb1):")
    tprint(kb:link_mount_table_find_records_by_mount_path(mounts[1], "kb1"), "  ")
  end
end

---------------------------------------------------------------------------
-- Main
---------------------------------------------------------------------------

local function main()
  local password = os.getenv("POSTGRES_PASSWORD")
  if not password then
    error("POSTGRES_PASSWORD environment variable is not set")
  end

  local kb = KB_Data_Structures.new({
    host     = "localhost",
    port     = "5432",
    dbname   = "knowledge_base",
    user     = "gedgar",
    password = password,
    database = "knowledge_base",
  })

  -- Run all tests
  local ok, err

  ok, err = pcall(test_bit_structures, kb)
  if not ok then print("ERROR in test_bit_structures: " .. tostring(err)) end

  ok, err = pcall(test_document_table, kb, "info1_jsonb")
  if not ok then print("ERROR in test_document_table(info1_jsonb): " .. tostring(err)) end

  ok, err = pcall(test_document_table, kb, "info2_jsonb")
  if not ok then print("ERROR in test_document_table(info2_jsonb): " .. tostring(err)) end

  ok, err = pcall(test_document_table, kb, "info3_jsonb")
  if not ok then print("ERROR in test_document_table(info3_jsonb): " .. tostring(err)) end

  ok, err = pcall(test_status_data, kb)
  if not ok then print("ERROR in test_status_data: " .. tostring(err)) end

  ok, err = pcall(test_job_queue, kb)
  if not ok then print("ERROR in test_job_queue: " .. tostring(err)) end

  ok, err = pcall(test_stream, kb)
  if not ok then print("ERROR in test_stream: " .. tostring(err)) end

  -- RPC Client
  ok, err = pcall(function()
    banner("RPC Functions")
    local node_ids = kb:rpc_client_find_rpc_client_ids(nil, nil, nil, nil)
    print("rpc_client_node_ids:")
    tprint(node_ids, "  ")

    local client_keys = kb:rpc_client_find_rpc_client_keys(node_ids)
    print("client_keys:")
    tprint(client_keys, "  ")

    local client_descs = kb:find_description_paths(client_keys)
    print("client_descriptions:")
    tprint(client_descs, "  ")

    test_client_queue(kb, client_keys[1])
  end)
  if not ok then print("ERROR in RPC Client tests: " .. tostring(err)) end

  -- RPC Server
  ok, err = pcall(function()
    local node_ids = kb:rpc_server_id_find(nil, nil, nil, nil)
    print("rpc_server_node_ids:")
    tprint(node_ids, "  ")

    local server_keys = kb:rpc_server_table_keys_find(node_ids)
    print("server_keys:")
    tprint(server_keys, "  ")

    local server_descs = kb:find_description_paths(server_keys)
    print("server_descriptions:")
    tprint(server_descs, "  ")

    test_server_functions(kb, server_keys[1])
  end)
  if not ok then print("ERROR in RPC Server tests: " .. tostring(err)) end

  -- Link Tables
  ok, err = pcall(test_link_tables, kb)
  if not ok then print("ERROR in test_link_tables: " .. tostring(err)) end

  -- Cleanup
  kb:disconnect()
  print("\n\nAll tests finished.")
end

main()