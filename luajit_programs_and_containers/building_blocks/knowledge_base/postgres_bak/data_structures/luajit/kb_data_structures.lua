--[[
  KB_Data_Structures - Unified facade for all knowledge base data structure modules.
  LuaJIT port using luadbi-postgresql and dkjson.

  Combines:  KB_Search, KB_Status_Data, KB_Job_Queue, KB_Stream,
             KB_RPC_Client, KB_RPC_Server, KB_Link_Table,
             KB_Link_Mount_Table, KB_Bit_Structures, KB_Document_Table

  Usage:
    local KB_Data_Structures = require("kb_data_structures")
    local kb = KB_Data_Structures.new({
      host     = "localhost",
      port     = "5432",
      dbname   = "knowledge_base",
      user     = "gedgar",
      password = os.getenv("POSTGRES_PASSWORD"),
      database = "knowledge_base",
    })
    -- All sub-module methods are available directly:
    kb:clear_filters()
    kb:search_label("KB_STATUS_FIELD")
    local rows = kb:execute_kb_search()
]]

local KB_Search          = require("kb_search")
local KB_Status_Data     = require("kb_status_data")
local KB_Job_Queue       = require("kb_job_queue")
local KB_Stream          = require("kb_stream")
local KB_RPC_Client      = require("kb_rpc_client")
local KB_RPC_Server      = require("kb_rpc_server")
local KB_Link_Table      = require("kb_link_table")
local KB_Link_Mount_Table = require("kb_link_mount_table")
local kb_bit_mod         = require("kb_bit_structures")
local KB_Bit_Structures  = kb_bit_mod.KB_Bit_Structures
local KB_Document_Table  = require("kb_document_table")

local KB_Data_Structures = {}
KB_Data_Structures.__index = KB_Data_Structures

---------------------------------------------------------------------------
-- Constructor
---------------------------------------------------------------------------

function KB_Data_Structures.new(cfg)
  local self = setmetatable({}, KB_Data_Structures)

  -- Core search / connection
  self.query_support = KB_Search.new(cfg)

  -- Sub-modules (each gets a reference to query_support for DB access)
  self.status_data      = KB_Status_Data.new(self.query_support, cfg.database)
  self.job_queue        = KB_Job_Queue.new(self.query_support, cfg.database)
  self.stream           = KB_Stream.new(self.query_support, cfg.database)
  self.rpc_client       = KB_RPC_Client.new(self.query_support, cfg.database)
  self.rpc_server       = KB_RPC_Server.new(self.query_support, cfg.database)
  self.link_table       = KB_Link_Table.new(self.query_support, cfg.database)
  self.link_mount_table = KB_Link_Mount_Table.new(self.query_support, cfg.database)
  self.bit_structures   = KB_Bit_Structures.new(self.query_support, cfg.database)
  self.document_table   = KB_Document_Table.new(self.query_support, cfg.database)

  return self
end

---------------------------------------------------------------------------
-- KB_Search  (query support)
---------------------------------------------------------------------------

function KB_Data_Structures:clear_filters()           return self.query_support:clear_filters() end
function KB_Data_Structures:search_label(v)           return self.query_support:search_label(v) end
function KB_Data_Structures:search_name(v)            return self.query_support:search_name(v) end
function KB_Data_Structures:search_kb(v)              return self.query_support:search_kb(v) end
function KB_Data_Structures:search_property_key(k)    return self.query_support:search_property_key(k) end
function KB_Data_Structures:search_property_value(k,v) return self.query_support:search_property_value(k,v) end
function KB_Data_Structures:search_has_link()         return self.query_support:search_has_link() end
function KB_Data_Structures:search_has_link_mount()   return self.query_support:search_has_link_mount() end
function KB_Data_Structures:search_path(p)            return self.query_support:search_path(p) end
function KB_Data_Structures:search_starting_path(p)   return self.query_support:search_starting_path(p) end
function KB_Data_Structures:execute_kb_search()       return self.query_support:execute_query() end
function KB_Data_Structures:find_description(rows)    return self.query_support:find_description(rows) end
function KB_Data_Structures:find_description_paths(p) return self.query_support:find_description_paths(p) end
function KB_Data_Structures:find_path_values(rows)    return self.query_support:find_path_values(rows) end
function KB_Data_Structures:decode_link_nodes(p)      return self.query_support:decode_link_nodes(p) end

---------------------------------------------------------------------------
-- Status Data
---------------------------------------------------------------------------

function KB_Data_Structures:find_status_node_ids(kb, name, props, path)
  return self.status_data:find_node_ids(kb, name, props, path)
end
function KB_Data_Structures:find_status_node_id(kb, name, props, path)
  return self.status_data:find_node_id(kb, name, props, path)
end
function KB_Data_Structures:get_status_data(path)
  return self.status_data:get_status_data(path)
end
function KB_Data_Structures:set_status_data(path, data)
  return self.status_data:set_status_data(path, data)
end

---------------------------------------------------------------------------
-- Job Queue
---------------------------------------------------------------------------

function KB_Data_Structures:find_job_ids(kb, name, props, path)
  return self.job_queue:find_job_ids(kb, name, props, path)
end
function KB_Data_Structures:find_job_id(kb, name, props, path)
  return self.job_queue:find_job_id(kb, name, props, path)
end
function KB_Data_Structures:get_queued_number(path)
  return self.job_queue:get_queued_number(path)
end
function KB_Data_Structures:get_free_number(path)
  return self.job_queue:get_free_number(path)
end
function KB_Data_Structures:peak_job_data(path, retries, delay)
  return self.job_queue:peak_job_data(path, retries, delay)
end
function KB_Data_Structures:mark_job_completed(job_id)
  return self.job_queue:mark_job_completed(job_id)
end
function KB_Data_Structures:push_job_data(path, data, retries, delay)
  return self.job_queue:push_job_data(path, data, retries, delay)
end
function KB_Data_Structures:list_pending_jobs(path)
  return self.job_queue:list_pending_jobs(path)
end
function KB_Data_Structures:list_active_jobs(path)
  return self.job_queue:list_active_jobs(path)
end
function KB_Data_Structures:clear_job_queue(path)
  return self.job_queue:clear_job_queue(path)
end

---------------------------------------------------------------------------
-- Stream
---------------------------------------------------------------------------

function KB_Data_Structures:find_stream_ids(kb, name, props, path)
  return self.stream:find_stream_ids(kb, name, props, path)
end
function KB_Data_Structures:find_stream_id(kb, name, props, path)
  return self.stream:find_stream_id(kb, name, props, path)
end
function KB_Data_Structures:find_stream_table_keys(rows)
  return self.stream:find_stream_table_keys(rows)
end
function KB_Data_Structures:push_stream_data(path, data, retries, delay)
  return self.stream:push_stream_data(path, data, retries, delay)
end
function KB_Data_Structures:list_stream_data(path, recorded_after, recorded_before)
  return self.stream:list_stream_data(path, nil, nil, recorded_after, recorded_before)
end
function KB_Data_Structures:clear_stream_data(path)
  return self.stream:clear_stream_data(path)
end
function KB_Data_Structures:get_stream_data_count(path)
  return self.stream:get_stream_data_count(path)
end
function KB_Data_Structures:get_stream_data_range(path, start_idx, end_idx)
  return self.stream:get_stream_data_range(path, start_idx, end_idx)
end
function KB_Data_Structures:get_stream_statistics(path)
  return self.stream:get_stream_statistics(path)
end
function KB_Data_Structures:get_stream_data_by_id(path, id)
  return self.stream:get_stream_data_by_id(path, id)
end

---------------------------------------------------------------------------
-- RPC Client
---------------------------------------------------------------------------

function KB_Data_Structures:rpc_client_find_rpc_client_id(kb, name, props, path)
  return self.rpc_client:find_rpc_client_id(kb, name, props, path)
end
function KB_Data_Structures:rpc_client_find_rpc_client_ids(kb, name, props, path)
  return self.rpc_client:find_rpc_client_ids(kb, name, props, path)
end
function KB_Data_Structures:rpc_client_find_rpc_client_keys(rows)
  return self.rpc_client:find_rpc_client_keys(rows)
end
function KB_Data_Structures:rpc_client_find_free_slots(path)
  return self.rpc_client:find_free_slots(path)
end
function KB_Data_Structures:rpc_client_find_queued_slots(path)
  return self.rpc_client:find_queued_slots(path)
end
function KB_Data_Structures:rpc_client_peak_and_claim_reply_data(path, retries, delay)
  return self.rpc_client:peak_and_claim_reply_data(path, retries, delay)
end
function KB_Data_Structures:rpc_client_clear_reply_queue(path, retries, delay)
  return self.rpc_client:clear_reply_queue(path, retries, delay)
end
function KB_Data_Structures:rpc_client_push_and_claim_reply_data(cpath, req_id, spath,
                                                                  action, tag, data,
                                                                  retries, delay)
  return self.rpc_client:push_and_claim_reply_data(cpath, req_id, spath,
                                                    action, tag, data,
                                                    retries, delay)
end
function KB_Data_Structures:rpc_client_list_waiting_jobs(path)
  return self.rpc_client:list_waiting_jobs(path)
end

---------------------------------------------------------------------------
-- RPC Server
---------------------------------------------------------------------------

function KB_Data_Structures:rpc_server_id_find(kb, name, props, path)
  return self.rpc_server:find_rpc_server_id(kb, name, props, path)
end
function KB_Data_Structures:rpc_server_ids_find(kb, name, props, path)
  return self.rpc_server:find_rpc_server_ids(kb, name, props, path)
end
function KB_Data_Structures:rpc_server_table_keys_find(rows)
  return self.rpc_server:find_rpc_server_table_keys(rows)
end
function KB_Data_Structures:rpc_server_list_jobs_job_types(spath, state)
  return self.rpc_server:list_jobs_job_types(spath, state)
end
function KB_Data_Structures:rpc_server_count_all_jobs(spath)
  return self.rpc_server:count_all_jobs(spath)
end
function KB_Data_Structures:rpc_server_count_empty_jobs(spath)
  return self.rpc_server:count_empty_jobs(spath)
end
function KB_Data_Structures:rpc_server_count_new_jobs(spath)
  return self.rpc_server:count_new_jobs(spath)
end
function KB_Data_Structures:rpc_server_count_processing_jobs(spath)
  return self.rpc_server:count_processing_jobs(spath)
end
function KB_Data_Structures:rpc_server_count_jobs_job_types(spath, state)
  return self.rpc_server:count_jobs_job_types(spath, state)
end
function KB_Data_Structures:rpc_server_push_rpc_queue(spath, req_id, action,
                                                      payload, tag, priority,
                                                      client_q, retries, wait)
  return self.rpc_server:push_rpc_queue(spath, req_id, action,
                                         payload, tag, priority,
                                         client_q, retries, wait)
end
function KB_Data_Structures:rpc_server_peak_server_queue(spath, retries, wait)
  return self.rpc_server:peak_server_queue(spath, retries, wait)
end
function KB_Data_Structures:rpc_server_mark_job_completion(spath, id, retries, wait)
  return self.rpc_server:mark_job_completion(spath, id, retries, wait)
end
function KB_Data_Structures:rpc_server_clear_server_queue(spath, retries, delay)
  return self.rpc_server:clear_server_queue(spath, retries, delay)
end

---------------------------------------------------------------------------
-- Link Table
---------------------------------------------------------------------------

function KB_Data_Structures:link_table_find_records_by_link_name(name, kb)
  return self.link_table:find_records_by_link_name(name, kb)
end
function KB_Data_Structures:link_table_find_records_by_node_path(path, kb)
  return self.link_table:find_records_by_node_path(path, kb)
end
function KB_Data_Structures:link_table_find_all_link_names()
  return self.link_table:find_all_link_names()
end
function KB_Data_Structures:link_table_find_all_node_names()
  return self.link_table:find_all_node_names()
end

---------------------------------------------------------------------------
-- Link Mount Table
---------------------------------------------------------------------------

function KB_Data_Structures:link_mount_table_find_records_by_link_name(name, kb)
  return self.link_mount_table:find_records_by_link_name(name, kb)
end
function KB_Data_Structures:link_mount_table_find_records_by_mount_path(path, kb)
  return self.link_mount_table:find_records_by_mount_path(path, kb)
end
function KB_Data_Structures:link_mount_table_find_all_link_names()
  return self.link_mount_table:find_all_link_names()
end
function KB_Data_Structures:link_mount_table_find_all_mount_paths()
  return self.link_mount_table:find_all_mount_paths()
end

---------------------------------------------------------------------------
-- Bit Structures
---------------------------------------------------------------------------

function KB_Data_Structures:find_bit_structure_ids(kb, name, props, path)
  return self.bit_structures:find_bit_structure_ids(kb, name, props, path)
end
function KB_Data_Structures:find_bit_structure_id(kb, name, props, path)
  return self.bit_structures:find_bit_structure_id(kb, name, props, path)
end
function KB_Data_Structures:find_assemble_bit_data(rows, clear, user_names)
  return self.bit_structures:find_assemble_bit_data(rows, clear, user_names)
end
function KB_Data_Structures:assemble_bit_data(row)
  return self.bit_structures:assemble_bit_data(row)
end
function KB_Data_Structures:get_bit_mask(node_id)
  return self.bit_structures:get_bit_mask(node_id)
end
function KB_Data_Structures:set_bit_mask(node_id, bits, mask)
  return self.bit_structures:set_bit_mask(node_id, bits, mask)
end
function KB_Data_Structures:set_flag_data(dc, flags)
  return self.bit_structures:set_flag_data(dc, flags)
end
function KB_Data_Structures:get_flag_data(dc)
  return self.bit_structures:get_flag_data(dc)
end
function KB_Data_Structures:s_tokenize(s)
  return self.bit_structures:tokenize(s)
end
function KB_Data_Structures:s_execute(tokens, kb_data)
  return self.bit_structures:execute(tokens, kb_data)
end
function KB_Data_Structures:set_all_ones(node_id)
  return self.bit_structures:set_all_ones(node_id)
end
function KB_Data_Structures:set_all_zeros(node_id)
  return self.bit_structures:set_all_zeros(node_id)
end

---------------------------------------------------------------------------
-- Document Table
---------------------------------------------------------------------------

function KB_Data_Structures:find_document_id(kb, name, props, path)
  return self.document_table:find_document_id(kb, name, props, path)
end
function KB_Data_Structures:find_document_ids(kb, name, props, path)
  return self.document_table:find_document_ids(kb, name, props, path)
end
function KB_Data_Structures:find_document_paths(rows)
  return self.document_table:find_document_paths(rows)
end
function KB_Data_Structures:jsonb_get(path, key, opts_or_as_text)
  return self.document_table:jsonb_get(path, key, opts_or_as_text)
end
function KB_Data_Structures:jsonb_set(path, key, value)
  return self.document_table:jsonb_set(path, key, value)
end
function KB_Data_Structures:jsonb_delete_key(path, key)
  return self.document_table:jsonb_delete_key(path, key)
end
function KB_Data_Structures:jsonb_delete_path(path, key)
  return self.document_table:jsonb_delete_path(path, key)
end
function KB_Data_Structures:jsonb_has_key(path, key)
  return self.document_table:jsonb_has_key(path, key)
end
function KB_Data_Structures:jsonb_has_any_keys(path, keys)
  return self.document_table:jsonb_has_any_keys(path, keys)
end
function KB_Data_Structures:jsonb_has_all_keys(path, keys)
  return self.document_table:jsonb_has_all_keys(path, keys)
end
function KB_Data_Structures:jsonb_contains(path, obj)
  return self.document_table:jsonb_contains(path, obj)
end
function KB_Data_Structures:jsonb_contained_by(path, obj)
  return self.document_table:jsonb_contained_by(path, obj)
end
function KB_Data_Structures:jsonb_path_exists(path, jp)
  return self.document_table:jsonb_path_exists(path, jp)
end
function KB_Data_Structures:jsonb_path_query(path, jp)
  return self.document_table:jsonb_path_query(path, jp)
end
function KB_Data_Structures:jsonb_query(path, q)
  return self.document_table:jsonb_query(path, q)
end
function KB_Data_Structures:jsonb_array_append(path, key, val)
  return self.document_table:jsonb_array_append(path, key, val)
end
function KB_Data_Structures:jsonb_array_prepend(path, key, val)
  return self.document_table:jsonb_array_prepend(path, key, val)
end
function KB_Data_Structures:jsonb_array_remove_index(path, key, idx)
  return self.document_table:jsonb_array_remove_index(path, key, idx)
end
function KB_Data_Structures:jsonb_array_contains(path, key, val)
  return self.document_table:jsonb_array_contains(path, key, val)
end
function KB_Data_Structures:jsonb_array_elements(path, key)
  return self.document_table:jsonb_array_elements(path, key)
end
function KB_Data_Structures:jsonb_enqueue(path, val)
  return self.document_table:enqueue(path, val)
end
function KB_Data_Structures:jsonb_dequeue(path)
  return self.document_table:dequeue(path)
end
function KB_Data_Structures:jsonb_peek(path, index)
  return self.document_table:peek(path, nil, nil, index)
end
function KB_Data_Structures:jsonb_size(path)
  return self.document_table:size(path)
end
function KB_Data_Structures:jsonb_is_empty(path)
  return self.document_table:is_empty(path)
end
function KB_Data_Structures:jsonb_clear(path)
  return self.document_table:clear(path)
end
function KB_Data_Structures:jsonb_get_all(path)
  return self.document_table:get_all(path)
end
function KB_Data_Structures:jsonb_push(path, val)
  return self.document_table:push(path, val)
end
function KB_Data_Structures:jsonb_pop(path)
  return self.document_table:pop(path)
end
function KB_Data_Structures:jsonb_get_metadata(path)
  return self.document_table:get_metadata(path)
end
function KB_Data_Structures:jsonb_set_metadata(path, meta)
  return self.document_table:set_metadata(path, meta)
end

---------------------------------------------------------------------------
-- Disconnect
---------------------------------------------------------------------------

function KB_Data_Structures:disconnect()
  return self.query_support:disconnect()
end

return KB_Data_Structures