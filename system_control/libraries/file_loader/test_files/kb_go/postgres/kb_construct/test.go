

package main

import (
    //"flag"
    "fmt"
    "log"
    "os"

    //_ "github.com/lib/pq"
    kb "github.com/glenn-edgar/knowledge_base/kb_modules/kb_go/postgres/kb_construct/kb_construct_module"
)

func main() {
    // Get password from user
    password := os.Getenv("POSTGRES_PASSWORD")
	if password == "" {
		log.Fatal("POSTGRES_PASSWORD is not set")
	}

    // Database configuration
    dbHost := "localhost"
    dbPort := 5432
    dbName := "knowledge_base"
    dbUser := "gedgar"
    database := "knowledge_base"

    // Create ConstructDataTables instance
    kbObj, err := kb.NewConstructDataTables(dbHost, dbPort, dbName, dbUser, password, database)
    if err != nil {
        log.Fatal("Error creating ConstructDataTables:", err)
    }
    defer kbObj.Disconnect()

    fmt.Println("Initial state:")
    fmt.Printf("Path: %v\n", kbObj.Path())

    // First test scenario
    if err := kbObj.AddKB("kb1", "First knowledge base"); err != nil {
        log.Fatal("Error adding kb1:", err)
    }

    if err := kbObj.SelectKB("kb1"); err != nil {
        log.Fatal("Error selecting kb1:", err)
    }

    if err := kbObj.AddHeaderNode("header1_link", "header1_name",
        map[string]interface{}{"prop1": "val1"},
        map[string]interface{}{"data": "header1_data"}, ""); err != nil {
        log.Fatal("Error adding header node:", err)
    }

    fmt.Println("\nAfter add_header_node:")
    fmt.Printf("Path: %v\n", kbObj.Path())

    if err := kbObj.AddInfoNode("info1_link", "info1_name",
        map[string]interface{}{"prop2": "val2"},
        map[string]interface{}{"data": "info1_data"}, ""); err != nil {
        log.Fatal("Error adding info node:", err)
    }

    fmt.Println("\nAfter add_info_node:")
    fmt.Printf("Path: %v\n", kbObj.Path())

    // Add various fields
    if _, err := kbObj.AddRPCServerField("info1_server", 25, "info1_server_data"); err != nil {
        log.Fatal("Error adding RPC server field:", err)
    }

    if _, err := kbObj.AddStatusField("info1_status",
        map[string]interface{}{"prop3": "val3"},
        "info1_status_description",
        map[string]interface{}{"prop3": "val3"}); err != nil {
        log.Fatal("Error adding status field:", err)
    }

    if _, err := kbObj.AddStatusField("info2_status",
        map[string]interface{}{"prop3": "val3"},
        "info2_status_description",
        map[string]interface{}{"prop3": "val3"}); err != nil {
        log.Fatal("Error adding status field 2:", err)
    }

    if _, err := kbObj.AddStatusField("info3_status",
        map[string]interface{}{"prop3": "val3"},
        "info3_status_description",
        map[string]interface{}{"prop3": "val3"}); err != nil {
        log.Fatal("Error adding status field 3:", err)
    }

    if _, err := kbObj.AddJobField("info1_job", 100, "info1_job_description"); err != nil {
        log.Fatal("Error adding job field:", err)
    }

    if _, err := kbObj.AddStreamField("info1_stream", 95, "info1_stream"); err != nil {
        log.Fatal("Error adding stream field:", err)
    }

    if _, err := kbObj.AddRPCClientField("info1_client", 10, "info1_client_description"); err != nil {
        log.Fatal("Error adding RPC client field:", err)
    }

    if err := kbObj.AddLinkMount("info1_link_mount", "info1_link_mount_description"); err != nil {
        log.Fatal("Error adding link mount:", err)
    }

    if err := kbObj.LeaveHeaderNode("header1_link", "header1_name"); err != nil {
        log.Fatal("Error leaving header node:", err)
    }

    fmt.Println("\nAfter leave_header_node:")
    fmt.Printf("Path: %v\n", kbObj.Path())

    if err := kbObj.AddHeaderNode("header2_link", "header2_name",
        map[string]interface{}{"prop3": "val3"},
        map[string]interface{}{"data": "header2_data"}, ""); err != nil {
        log.Fatal("Error adding header2 node:", err)
    }

    if err := kbObj.AddInfoNode("info2_link", "info2_name",
        map[string]interface{}{"prop4": "val4"},
        map[string]interface{}{"data": "info2_data"}, ""); err != nil {
        log.Fatal("Error adding info2 node:", err)
    }

    if err := kbObj.AddLinkNode("info1_link_mount"); err != nil {
        log.Fatal("Error adding link node:", err)
    }

    if err := kbObj.LeaveHeaderNode("header2_link", "header2_name"); err != nil {
        log.Fatal("Error leaving header2 node:", err)
    }

    fmt.Println("\nAfter adding and leaving another header node:")
    fmt.Printf("Path: %v\n", kbObj.Path())

    // Check installation
    if err := kbObj.CheckInstallation(); err != nil {
        fmt.Printf("Error during installation check: %v\n", err)
    }

    fmt.Println("\nFirst test scenario completed successfully!")
}



