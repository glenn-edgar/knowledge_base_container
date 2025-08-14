package main

import (
    "fmt"
    "log"
    "time"

    "github.com/google/uuid"
    ds "github.com/glenn-edgar/knowledge_base/kb_modules/kb_go/postgres/data_structures/data_structures_module"
)

func main() {
    var password string
    fmt.Print("Enter PostgreSQL password: ")
    fmt.Scanln(&password)

    host := "localhost"
    port := "5432"
    dbname := "knowledge_base"
    user := "gedgar"
    database := "knowledge_base"

    kds, err := ds.NewKBDataStructures(host, port, dbname, user, password, database)
    if err != nil {
        log.Fatalf("Failed to create KBDataStructures: %v", err)
    }
    defer func() {
        if err := kds.Disconnect(); err != nil {
            log.Printf("Error during disconnect: %v", err)
        }
    }()

    // === Status Data Test ===
    fmt.Println("=== Status Data Test ===")
    statusNodes, err := kds.FindStatusNodeIDs(nil, nil, nil, nil)
    if err != nil {
        log.Fatalf("FindStatusNodeIDs error: %v", err)
    }
    fmt.Printf("All status node IDs: %v\n", statusNodes)

    paths := kds.FindPathValues(statusNodes)
    fmt.Printf("All status paths: %v\n", paths)

    // Specific status node
    kb1 := "kb1"
    nodeName := "info2_status"
    props := map[string]interface{}{"prop3": "val3"}
    nodePath := "*.header1_link.header1_name.KB_STATUS_FIELD.info2_status"
    specificNodes, err := kds.FindStatusNodeIDs(&kb1, &nodeName, props, &nodePath)
    if err != nil {
        log.Fatalf("Find specific status node error: %v", err)
    }
    if len(specificNodes) > 0 {
        spec := specificNodes[0]
        fmt.Printf("Specific status node: %v\n", spec)

        specPath := kds.FindPathValues([]map[string]interface{}{spec})[0]
        fmt.Printf("Path of specific node: %s\n", specPath)

        desc := kds.FindDescription(spec)
        fmt.Printf("Description: %v\n", desc)

        data, _, err := kds.GetStatusData(specPath)
        if err != nil {
            log.Printf("GetStatusData error: %v", err)
        }
        fmt.Printf("Initial data: %v\n", data)

        ok, _, err := kds.SetStatusData(specPath, map[string]interface{}{"prop1": "val1", "prop2": "val2"}, 3, time.Second)
        if err != nil {
            log.Printf("SetStatusData error: %v", err)
        }
        fmt.Printf("Data set successful: %t\n", ok)

        updated, _, err := kds.GetStatusData(specPath)
        if err != nil {
            log.Printf("GetStatusData error: %v", err)
        }
        fmt.Printf("Data after set: %v\n", updated)
    }

    // === Job Queue Test ===
    fmt.Println("=== Job Queue Test ===")
    jobNodes, err := kds.FindJobIDs(nil, nil, nil, nil)
    if err != nil {
        log.Fatalf("FindJobIDs error: %v", err)
    }
    jobPaths := kds.FindPathValues(jobNodes)
    if len(jobPaths) > 0 {
        jp := jobPaths[0]
        fmt.Printf("First job queue path: %s\n", jp)

        // Clear and test
        kds.ClearJobQueue(jp)
        queued, err := kds.GetQueuedNumber(jp)
        if err != nil {
            log.Printf("GetQueuedNumber error: %v", err)
        }
        free, err := kds.GetFreeNumber(jp)
        if err != nil {
            log.Printf("GetFreeNumber error: %v", err)
        }
        fmt.Printf("Queued: %d, Free: %d\n", queued, free)

        // Push a job
        if _, err := kds.PushJobData(jp, map[string]interface{}{"prop1": "val1", "prop2": "val2"}, 3, time.Second); err != nil {
            log.Printf("PushJobData error: %v", err)
        }
        queued, _ = kds.GetQueuedNumber(jp)
        free, _ = kds.GetFreeNumber(jp)
        fmt.Printf("After push -> Queued: %d, Free: %d\n", queued, free)

        pending, _ := kds.ListPendingJobs(jp, nil, 0)
        active, _ := kds.ListActiveJobs(jp, nil, 0)
        fmt.Printf("Pending: %v, Active: %v\n", pending, active)

        peakRec, _ := kds.PeakJobData(jp, 3, time.Second)
        fmt.Printf("Peak job data: %v\n", peakRec)

        id, _ := peakRec.ID, peakRec
        if _, err := kds.MarkJobCompleted(id, 3, time.Second); err != nil {
            log.Printf("MarkJobCompleted error: %v", err)
        }
    }

     // === Stream Data Test ===
	 fmt.Println("=== Stream Data Test ===")
	 streamName := "info1_stream"
	 streamNodes, err := kds.FindStreamIDs(&kb1, &streamName, nil, nil)
	 if err != nil {
		 log.Printf("FindStreamIDs error: %v", err)
	 }
	 streamKeys := kds.FindStreamTableKeys(streamNodes)
	 fmt.Printf("stream_table_keys: %v\n", streamKeys)
 
	 descs, err := kds.FindDescriptionPaths(streamKeys)
	 if err != nil {
		 log.Printf("FindDescriptionPaths error: %v", err)
	 }
	 fmt.Printf("descriptions: %v\n", descs)
 
	 if len(streamKeys) > 0 {
		 key := streamKeys[0]
		 if err := kds.ClearStreamData(key, nil); err != nil {
			 log.Printf("ClearStreamData error: %v", err)
		 }
		 if _, err := kds.PushStreamData(key, map[string]interface{}{"prop1": "val1", "prop2": "val2"}, 3, time.Second); err != nil {
			 log.Printf("PushStreamData error: %v", err)
		 }
		 recs, err := kds.ListStreamData(key, nil, 0, nil, nil, "ASC")
		 if err != nil {
			 log.Printf("ListStreamData error: %v", err)
		 }
		 fmt.Printf("list_stream_data: %v\n", recs)
 
		 pastTimestamp := time.Now().Add(-15 * time.Minute)
		 beforeTimestamp := time.Now()
		 fmt.Printf("past_timestamp: %v\n", pastTimestamp)
		 fmt.Println("past data")
		 recsPast, err := kds.ListStreamData(key, nil, 0, &pastTimestamp, &beforeTimestamp, "ASC")
		 if err != nil {
			 log.Printf("ListStreamData (past) error: %v", err)
		 }
		 fmt.Printf("list_stream_data: %v\n", recsPast)
	 }

    // === RPC Tests ===
    fmt.Println("=== RPC Client/Server Tests ===")
    // Client
    clientNodes, err := kds.FindRPCClientIDs(nil, nil, nil, nil)
    if err == nil && len(clientNodes) > 0 {
        clientKeys := kds.FindRPCClientKeys(clientNodes)
        testClientQueue(kds, clientKeys[0])
    }

    // Server
    serverNodes, err := kds.FindRPCServerIDs(nil, nil, nil, nil)
    if err == nil && len(serverNodes) > 0 {
        serverKeys := kds.FindRPCServerTableKeys(serverNodes)
        testServerFunctions(kds, serverKeys[0])
    }

    // === Link Table Tests ===
    fmt.Println("=== Link Table Tests ===")
    linkNames, err := kds.LinkTableFindAllLinkNames()
    if err == nil && len(linkNames) > 0 {
        recs, _ := kds.LinkTableFindRecordsByLinkName(linkNames[0], nil)
        fmt.Printf("Records by link name: %v\n", recs)
    }
    mountNames, err := kds.LinkMountTableFindAllMountPaths()
    if err == nil && len(mountNames) > 0 {
        recs, _ := kds.LinkMountTableFindRecordsByMountPath(mountNames[0], nil)
        fmt.Printf("Records by mount path: %v\n", recs)
    }
}

func testServerFunctions(kds *ds.KBDataStructures, serverPath string) {
    fmt.Println("--- testServerFunctions for", serverPath)
    // Clear server queue
    if _, err := kds.RPCServerClearServerQueue(serverPath, 3, time.Second); err != nil {
        log.Println("RPCServerClearServerQueue error:", err)
    }
    jobs, err := kds.RPCServerListJobsJobTypes(serverPath, "new_job")
    if err != nil {
        log.Println("RPCServerListJobsJobTypes error:", err)
    } else {
        fmt.Println("Jobs of type 'new_job':", jobs)
    }

    clientQueue := "rpc_client_queue"
    // Push three RPC requests
    for i := 1; i <= 3; i++ {
        reqID := uuid.New().String()
        action := fmt.Sprintf("rpc_action%d", i)
        payload := map[string]interface{}{fmt.Sprintf("data%d", i): fmt.Sprintf("data%d", i)}
        _, err := kds.RPCServerPushRPCQueue(serverPath, reqID, action, payload, fmt.Sprintf("transaction_tag_%d", i), i, &clientQueue, 5, 500*time.Millisecond)
        if err != nil {
            log.Println("RPCServerPushRPCQueue error:", err)
        }
    }
    // Peek and complete jobs
    for i := 0; i < 3; i++ {
        rec, err := kds.RPCServerPeakServerQueue(serverPath, 5, 500*time.Millisecond)
        if err != nil {
            log.Println("RPCServerPeakServerQueue error:", err)
            break
        }
        id := int(rec["id"].(int64))
        fmt.Println("Peaked job:", rec)
        if _, err := kds.RPCServerMarkJobCompletion(serverPath, id, 5, time.Second); err != nil {
            log.Println("RPCServerMarkJobCompletion error:", err)
        }
        count, _ := kds.RPCServerCountAllJobs(serverPath)
        fmt.Println("Remaining jobs count:", count)
    }
}

func testClientQueue(kds *ds.KBDataStructures, clientPath string) {
    fmt.Println("--- testClientQueue for", clientPath)
    free, _ := kds.RPCClientFindFreeSlots(clientPath)
    queued, _ := kds.RPCClientFindQueuedSlots(clientPath)
    fmt.Printf("Free slots: %d, Queued slots: %d\n", free, queued)

    // Clear reply queue
    if _, err := kds.RPCClientClearReplyQueue(clientPath, 3, time.Second); err != nil {
        log.Println("RPCClientClearReplyQueue error:", err)
    }

    //clientQueue := "rpc_client_queue"
    // Push and claim two replies
    for i := 1; i <= 2; i++ {
        reqID := uuid.New().String()
         err := kds.RPCClientPushAndClaimReplyData(clientPath, reqID, "xxx", fmt.Sprintf("Action%d", i), "yyy", map[string]interface{}{fmt.Sprintf("data%d", i): fmt.Sprintf("data%d", i)}, 5, time.Second)
        if err != nil {
            log.Println("RPCClientPushAndClaimReplyData error:", err)
        }
    }

    // Peek and release
    for i := 0; i < 2; i++ {
        rec, err := kds.RPCClientPeakAndClaimReplyData(clientPath, 5, time.Second)
        if err != nil {
            log.Println("RPCClientPeakAndClaimReplyData error:", err)
            break
        }
        fmt.Println("Peeked reply data:", rec)
    }
}
