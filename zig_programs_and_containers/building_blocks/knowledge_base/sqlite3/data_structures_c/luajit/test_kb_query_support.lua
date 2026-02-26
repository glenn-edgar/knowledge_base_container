#!/usr/bin/env luajit
--[[
  test_kb_query_support.lua — LuaJIT port of test_kb_query_support.py

  Test script for KB_Search SQLite implementation.
  Demonstrates the main functionality of the converted class.

  Usage:
    luajit test_kb_query_support.lua
]]

local ffi  = require('ffi')
local json -- resolved below

local ok, cjson = pcall(require, 'cjson')
if ok then
    json = { encode = cjson.encode, decode = cjson.decode }
else
    local ok2, dkjson = pcall(require, 'dkjson')
    if ok2 then
        json = { encode = dkjson.encode, decode = dkjson.decode }
    else
        error("No JSON library found. Install lua-cjson or dkjson.")
    end
end

-- ── FFI (guarded) ───────────────────────────────────────────────────────
pcall(ffi.cdef, [[
    typedef struct sqlite3 sqlite3;
    typedef struct sqlite3_stmt sqlite3_stmt;

    int  sqlite3_open(const char *filename, sqlite3 **ppDb);
    int  sqlite3_close(sqlite3 *db);
    int  sqlite3_exec(sqlite3 *db, const char *sql,
                      int (*callback)(void*,int,char**,char**),
                      void *arg, char **errmsg);
    void sqlite3_free(void *ptr);

    int  sqlite3_enable_load_extension(sqlite3 *db, int onoff);
    int  sqlite3_load_extension(sqlite3 *db, const char *zFile,
                                const char *zProc, char **pzErrMsg);
    const char *sqlite3_errmsg(sqlite3 *db);
]])

local sqlite3 = ffi.load('sqlite3')
local SQLITE_OK = 0

-- ── Helpers ─────────────────────────────────────────────────────────────

local function sql_exec(db, sql)
    local errmsg = ffi.new('char*[1]')
    local rc = sqlite3.sqlite3_exec(db, sql, nil, nil, errmsg)
    if rc ~= SQLITE_OK then
        local msg = errmsg[0] ~= nil and ffi.string(errmsg[0]) or 'unknown error'
        sqlite3.sqlite3_free(errmsg[0])
        error(string.format("SQL exec error (%d): %s\nSQL: %s", rc, msg, sql))
    end
end

local function find_ltree_path()
    local search = { './ltree', '/usr/local/lib/ltree', '/usr/lib/ltree' }
    for _, p in ipairs(search) do
        local f = io.open(p .. '.so', 'r')
        if f then f:close(); return p end
    end
    return './ltree'
end

-- ── Create test database ────────────────────────────────────────────────

local function create_test_database(db_path)
    local db_handle = ffi.new('sqlite3*[1]')
    local rc = sqlite3.sqlite3_open(db_path, db_handle)
    if rc ~= SQLITE_OK then
        print("✗ Failed to open database: " .. db_path)
        return false
    end
    local db = db_handle[0]

    -- Load ltree extension
    sqlite3.sqlite3_enable_load_extension(db, 1)
    local ltree_path = find_ltree_path()
    local errmsg = ffi.new('char*[1]')
    rc = sqlite3.sqlite3_load_extension(db, ltree_path, nil, errmsg)
    if rc ~= SQLITE_OK then
        local msg = errmsg[0] ~= nil and ffi.string(errmsg[0]) or 'unknown'
        sqlite3.sqlite3_free(errmsg[0])
        print("✗ Failed to load ltree extension: " .. msg)
        sqlite3.sqlite3_close(db)
        return false
    end
    print("✓ Loaded ltree extension from " .. ltree_path)
    sqlite3.sqlite3_enable_load_extension(db, 0)

    -- Create table
    sql_exec(db, [[
        CREATE TABLE IF NOT EXISTS knowledge_base (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            knowledge_base TEXT NOT NULL,
            label TEXT,
            name TEXT,
            path TEXT NOT NULL UNIQUE,
            properties TEXT,
            data TEXT,
            has_link INTEGER DEFAULT 0,
            has_link_mount INTEGER DEFAULT 0
        )
    ]])

    -- Clear existing data
    sql_exec(db, 'DELETE FROM knowledge_base')

    -- Sample data
    local sample_data = {
        {
            knowledge_base = 'tech_docs',
            label          = 'root',
            name           = 'documentation',
            path           = 'tech_docs',
            properties     = json.encode({ description = 'Root of technical documentation' }),
            data           = 'Root node data',
            has_link       = 0,
            has_link_mount = 0,
        },
        {
            knowledge_base = 'tech_docs',
            label          = 'section',
            name           = 'python',
            path           = 'tech_docs.python',
            properties     = json.encode({ description = 'Python documentation', version = '3.11' }),
            data           = 'Python section data',
            has_link       = 1,
            has_link_mount = 0,
        },
        {
            knowledge_base = 'tech_docs',
            label          = 'article',
            name           = 'basics',
            path           = 'tech_docs.python.basics',
            properties     = json.encode({ description = 'Python basics tutorial', difficulty = 'beginner' }),
            data           = 'Basic Python tutorial content',
            has_link       = 0,
            has_link_mount = 0,
        },
        {
            knowledge_base = 'tech_docs',
            label          = 'article',
            name           = 'advanced',
            path           = 'tech_docs.python.advanced',
            properties     = json.encode({ description = 'Advanced Python concepts', difficulty = 'advanced' }),
            data           = 'Advanced Python content',
            has_link       = 0,
            has_link_mount = 1,
        },
        {
            knowledge_base = 'tech_docs',
            label          = 'section',
            name           = 'javascript',
            path           = 'tech_docs.javascript',
            properties     = json.encode({ description = 'JavaScript documentation', version = 'ES2023' }),
            data           = 'JavaScript section data',
            has_link       = 0,
            has_link_mount = 0,
        },
        {
            knowledge_base = 'tech_docs',
            label          = 'article',
            name           = 'intro',
            path           = 'tech_docs.javascript.intro',
            properties     = json.encode({ description = 'JavaScript introduction', difficulty = 'beginner' }),
            data           = 'JavaScript intro content',
            has_link       = 1,
            has_link_mount = 0,
        },
        {
            knowledge_base = 'api_docs',
            label          = 'root',
            name           = 'api',
            path           = 'api_docs',
            properties     = json.encode({ description = 'API documentation root' }),
            data           = 'API root data',
            has_link       = 0,
            has_link_mount = 0,
        },
        {
            knowledge_base = 'api_docs',
            label          = 'endpoint',
            name           = 'users',
            path           = 'api_docs.users',
            properties     = json.encode({ description = 'User management API', version = 'v2' }),
            data           = 'User API data',
            has_link       = 0,
            has_link_mount = 0,
        },
    }

    -- Escape single quotes in strings for SQL
    local function esc(s)
        if s == nil then return 'NULL' end
        return "'" .. tostring(s):gsub("'", "''") .. "'"
    end

    for _, row in ipairs(sample_data) do
        local sql = string.format(
            "INSERT INTO knowledge_base "
            .. "(knowledge_base, label, name, path, properties, data, has_link, has_link_mount) "
            .. "VALUES (%s, %s, %s, %s, %s, %s, %d, %d)",
            esc(row.knowledge_base), esc(row.label), esc(row.name),
            esc(row.path), esc(row.properties), esc(row.data),
            row.has_link, row.has_link_mount)
        sql_exec(db, sql)
    end

    sqlite3.sqlite3_close(db)
    print(string.format("✓ Created test database with %d records", #sample_data))
    return true
end

-- ── Pretty-print results ────────────────────────────────────────────────

local function print_results(results, title)
    print('')
    print(string.rep('=', 60))
    print(title)
    print(string.rep('=', 60))

    if not results or #results == 0 then
        print("No results found")
        return
    end

    for i, row in ipairs(results) do
        print(string.format("\n%d. Path: %s", i, row.path or 'N/A'))
        print(string.format("   Knowledge Base: %s", row.knowledge_base or 'N/A'))
        print(string.format("   Label: %s", row.label or 'N/A'))
        print(string.format("   Name: %s", row.name or 'N/A'))

        local props_str = row.properties or '{}'
        local ok_j, props = pcall(json.decode, props_str)
        if ok_j and props and next(props) then
            -- Simple key=value display
            local parts = {}
            for k, v in pairs(props) do
                parts[#parts + 1] = string.format("%s=%s", k, tostring(v))
            end
            print("   Properties: {" .. table.concat(parts, ', ') .. "}")
        end
    end

    print(string.format("\nTotal results: %d", #results))
end

-- ═══════════════════════════════════════════════════════════════════════
-- Main test
-- ═══════════════════════════════════════════════════════════════════════

local function test_kb_search()
    local db_path = 'test_kb.db'

    print("Initializing test database...")
    if not create_test_database(db_path) then
        print("Failed to create test database. Exiting.")
        return
    end

    print("\nInitializing KB_Search...")
    local KB_Search = require('kb_query_support')
    local kb_ok, kb = pcall(KB_Search.new, {
        db_path              = db_path,
        database             = 'knowledge_base',
        ltree_extension_path = nil,
    })
    if not kb_ok then
        print("Failed to initialize KB_Search: " .. tostring(kb))
        return
    end

    -- Test 1: Search by knowledge base
    print("\n" .. string.rep('=', 60))
    print("TEST 1: Search by knowledge base")
    print(string.rep('=', 60))
    kb:clear_filters()
    kb:search_kb('tech_docs')
    local results = kb:execute_query()
    print_results(results, "All tech_docs entries")

    -- Test 2: Search by label
    print("\n" .. string.rep('=', 60))
    print("TEST 2: Search by label")
    print(string.rep('=', 60))
    kb:clear_filters()
    kb:search_label('article')
    results = kb:execute_query()
    print_results(results, "All articles")

    -- Test 3: Search by name
    print("\n" .. string.rep('=', 60))
    print("TEST 3: Search by name")
    print(string.rep('=', 60))
    kb:clear_filters()
    kb:search_name('python')
    results = kb:execute_query()
    print_results(results, "Items named 'python'")

    -- Test 4: Exact path match
    print("\n" .. string.rep('=', 60))
    print("TEST 4: Exact path match")
    print(string.rep('=', 60))
    kb:clear_filters()
    kb:search_path('tech_docs.python')
    results = kb:execute_query()
    print_results(results, "Exact match: tech_docs.python")

    -- Test 5: Path wildcard match
    print("\n" .. string.rep('=', 60))
    print("TEST 5: Path wildcard match")
    print(string.rep('=', 60))
    kb:clear_filters()
    kb:search_path('tech_docs.*')
    results = kb:execute_query()
    print_results(results, "Direct children of tech_docs")

    -- Test 6: Path quantified wildcard
    print("\n" .. string.rep('=', 60))
    print("TEST 6: Path quantified wildcard")
    print(string.rep('=', 60))
    kb:clear_filters()
    kb:search_path('tech_docs.*{1,2}')
    results = kb:execute_query()
    print_results(results, "tech_docs descendants 1-2 levels deep")

    -- Test 7: Ancestor search
    print("\n" .. string.rep('=', 60))
    print("TEST 7: Ancestor search")
    print(string.rep('=', 60))
    kb:clear_filters()
    kb:search_starting_path('tech_docs.python')
    results = kb:execute_query()
    print_results(results, "All descendants of tech_docs.python")

    -- Test 8: Property key existence
    print("\n" .. string.rep('=', 60))
    print("TEST 8: Property key existence")
    print(string.rep('=', 60))
    kb:clear_filters()
    kb:search_property_key('difficulty')
    results = kb:execute_query()
    print_results(results, "Items with 'difficulty' property")

    -- Test 9: Property value match
    print("\n" .. string.rep('=', 60))
    print("TEST 9: Property value match")
    print(string.rep('=', 60))
    kb:clear_filters()
    kb:search_property_value('difficulty', 'beginner')
    results = kb:execute_query()
    print_results(results, "Items with difficulty='beginner'")

    -- Test 10: Has link search
    print("\n" .. string.rep('=', 60))
    print("TEST 10: Has link search")
    print(string.rep('=', 60))
    kb:clear_filters()
    kb:search_has_link()
    results = kb:execute_query()
    print_results(results, "Items with has_link=TRUE")

    -- Test 11: Combined filters
    print("\n" .. string.rep('=', 60))
    print("TEST 11: Combined filters")
    print(string.rep('=', 60))
    kb:clear_filters()
    kb:search_kb('tech_docs')
    kb:search_label('article')
    kb:search_property_key('difficulty')
    results = kb:execute_query()
    print_results(results, "tech_docs articles with difficulty property")

    -- Test 12: find_description_paths
    print("\n" .. string.rep('=', 60))
    print("TEST 12: Find data by paths")
    print(string.rep('=', 60))
    local paths = { 'tech_docs.python.basics', 'tech_docs.javascript.intro', 'nonexistent.path' }
    local data_dict = kb:find_description_paths(paths)
    print("Data for specified paths:")
    for _, p in ipairs(paths) do
        -- iterate in order (Lua tables don't preserve insertion order)
        local val = data_dict[p]
        print(string.format("  %s: %s", p, tostring(val)))
    end

    -- Test 13: decode_link_nodes
    print("\n" .. string.rep('=', 60))
    print("TEST 13: Decode link nodes")
    print(string.rep('=', 60))
    local test_path = 'kb_main.uuid1.parent.uuid2.child.uuid3.grandchild'
    local dok, kb_name, node_pairs
    dok, kb_name, node_pairs = pcall(function()
        return kb:decode_link_nodes(test_path)
    end)
    if dok then
        -- pcall returns true + the two return values as separate results
        -- but since we wrapped in a function, kb_name is actually the first
        -- return and node_pairs is the second. Adjust:
    end
    -- Redo without pcall wrapper issue:
    local decode_ok, r1, r2 = pcall(kb.decode_link_nodes, kb, test_path)
    if decode_ok then
        print("Path: " .. test_path)
        print("KB: " .. r1)
        print("Node pairs:")
        for _, pair in ipairs(r2) do
            print(string.format("  [%s, %s]", pair[1], pair[2]))
        end
    else
        print("Error: " .. tostring(r1))
    end

    -- Cleanup
    kb:disconnect()
    print("\n" .. string.rep('=', 60))
    print("All tests completed!")
    print(string.rep('=', 60))
end

-- Run
test_kb_search()
