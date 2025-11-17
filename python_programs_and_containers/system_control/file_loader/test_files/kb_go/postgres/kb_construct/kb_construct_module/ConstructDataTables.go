package kb_construct_module



import (
	"fmt"
	//"log"
)

// ConstructDataTables combines all table constructors into a single interface
type ConstructDataTables struct {
	kb             *ConstructKB
	statusTable    *ConstructStatusTable
	jobTable       *ConstructJobTable
	streamTable    *ConstructStreamTable
	rpcClientTable *ConstructRPCClientTable
	rpcServerTable *ConstructRPCServerTable
}

// NewConstructDataTables creates a new instance with all table constructors
func NewConstructDataTables(host string, port int, dbname, user, password, database string) (*ConstructDataTables, error) {
	// Create the base knowledge base constructor
	kb, err := NewConstructKB(host, port, dbname, user, password, database)
	if err != nil {
		return nil, fmt.Errorf("error creating ConstructKB: %w", err)
	}

	// Get the database connection from kb
	conn, _ := kb.GetDBObjects()

	// Create instances of all table constructors
	statusTable, err := NewConstructStatusTable(conn, kb, database)
	if err != nil {
		kb.Disconnect()
		return nil, fmt.Errorf("error creating status table: %w", err)
	}

	jobTable, err := NewConstructJobTable(conn, kb, database)
	if err != nil {
		kb.Disconnect()
		return nil, fmt.Errorf("error creating job table: %w", err)
	}

	streamTable, err := NewConstructStreamTable(conn, kb, database)
	if err != nil {
		kb.Disconnect()
		return nil, fmt.Errorf("error creating stream table: %w", err)
	}

	rpcClientTable, err := NewConstructRPCClientTable(conn, kb, database)
	if err != nil {
		kb.Disconnect()
		return nil, fmt.Errorf("error creating RPC client table: %w", err)
	}

	rpcServerTable, err := NewConstructRPCServerTable(conn, kb, database)
	if err != nil {
		kb.Disconnect()
		return nil, fmt.Errorf("error creating RPC server table: %w", err)
	}

	cdt := &ConstructDataTables{
		kb:             kb,
		statusTable:    statusTable,
		jobTable:       jobTable,
		streamTable:    streamTable,
		rpcClientTable: rpcClientTable,
		rpcServerTable: rpcServerTable,
	}

	return cdt, nil
}

// Delegated methods from ConstructKB
func (cdt *ConstructDataTables) Path() map[string][]string {
	return cdt.kb.path
}

func (cdt *ConstructDataTables) AddKB(kbName, description string) error {
	return cdt.kb.AddKB(kbName, description)
}

func (cdt *ConstructDataTables) SelectKB(kbName string) error {
	return cdt.kb.SelectKB(kbName)
}

func (cdt *ConstructDataTables) AddLinkNode(linkName string) error {
	return cdt.kb.AddLinkNode(linkName)
}

func (cdt *ConstructDataTables) AddLinkMount(linkMountName, description string) error {
	return cdt.kb.AddLinkMount(linkMountName, description)
}

func (cdt *ConstructDataTables) AddHeaderNode(link, nodeName string, nodeProperties, nodeData map[string]interface{}, description string) error {
	return cdt.kb.AddHeaderNode(link, nodeName, nodeProperties, nodeData, description)
}

func (cdt *ConstructDataTables) AddInfoNode(link, nodeName string, nodeProperties, nodeData map[string]interface{}, description string) error {
	return cdt.kb.AddInfoNode(link, nodeName, nodeProperties, nodeData, description)
}

func (cdt *ConstructDataTables) LeaveHeaderNode(label, name string) error {
	return cdt.kb.LeaveHeaderNode(label, name)
}

func (cdt *ConstructDataTables) Disconnect() error {
	return cdt.kb.Disconnect()
}

// Delegated methods from table constructors
func (cdt *ConstructDataTables) AddStreamField(streamKey string, streamLength int, description string) (*StreamFieldResult, error) {
	return cdt.streamTable.AddStreamField(streamKey, streamLength, description)
}

func (cdt *ConstructDataTables) AddRPCClientField(rpcClientKey string, queueDepth int, description string) (*RPCClientFieldResult, error) {
	return cdt.rpcClientTable.AddRPCClientField(rpcClientKey, queueDepth, description)
}

func (cdt *ConstructDataTables) AddRPCServerField(rpcServerKey string, queueDepth int, description string) (*RPCServerFieldResult, error) {
	return cdt.rpcServerTable.AddRPCServerField(rpcServerKey, queueDepth, description)
}

func (cdt *ConstructDataTables) AddStatusField(statusKey string, properties map[string]interface{}, description string, initialData map[string]interface{}) (*StatusFieldResult, error) {
	return cdt.statusTable.AddStatusField(statusKey, properties, description, initialData)
}

func (cdt *ConstructDataTables) AddJobField(jobKey string, jobLength int, description string) (*JobFieldResult, error) {
	return cdt.jobTable.AddJobField(jobKey, jobLength, description)
}

// CheckInstallation checks the installation status of all table components
func (cdt *ConstructDataTables) CheckInstallation() error {
	// Call check_installation on each component
	if err := cdt.kb.CheckInstallation(); err != nil {
		return fmt.Errorf("KB check installation failed: %w", err)
	}

	if _, err := cdt.statusTable.CheckInstallation(); err != nil {
		return fmt.Errorf("status table check installation failed: %w", err)
	}
		
	if err := cdt.jobTable.CheckInstallation(); err != nil {
		return fmt.Errorf("job table check installation failed: %w", err)
	}

	if err := cdt.streamTable.CheckInstallation(); err != nil {
		return fmt.Errorf("stream table check installation failed: %w", err)
	}

	if  err := cdt.rpcClientTable.CheckInstallation(); err != nil {
		return fmt.Errorf("RPC client table check installation failed: %w", err)
	}

	if err := cdt.rpcServerTable.CheckInstallation(); err != nil {
		return fmt.Errorf("RPC server table check installation failed: %w", err)
	}

	return nil
}
/*

func main() {
	// Get password from user
	fmt.Print("Enter PostgreSQL password: ")
	var password string
	fmt.Scanln(&password)

	// Database configuration
	dbHost := "localhost"
	dbPort := 5432
	dbName := "knowledge_base"
	dbUser := os.Getenv("POSTGRES_USER")
	database := "knowledge_base"

	// Create ConstructDataTables instance
	kb, err := NewConstructDataTables(dbHost, dbPort, dbName, dbUser, password, database)
	if err != nil {
		log.Fatal("Error creating ConstructDataTables:", err)
	}
	defer kb.Disconnect()

	fmt.Println("Initial state:")
	fmt.Printf("Path: %v\n", kb.Path())

	// First test scenario
	if err := kb.AddKB("kb1", "First knowledge base"); err != nil {
		log.Fatal("Error adding kb1:", err)
	}

	if err := kb.SelectKB("kb1"); err != nil {
		log.Fatal("Error selecting kb1:", err)
	}

	if err := kb.AddHeaderNode("header1_link", "header1_name",
		map[string]interface{}{"prop1": "val1"},
		map[string]interface{}{"data": "header1_data"}, ""); err != nil {
		log.Fatal("Error adding header node:", err)
	}

	fmt.Println("\nAfter add_header_node:")
	fmt.Printf("Path: %v\n", kb.Path())

	if err := kb.AddInfoNode("info1_link", "info1_name",
		map[string]interface{}{"prop2": "val2"},
		map[string]interface{}{"data": "info1_data"}, ""); err != nil {
		log.Fatal("Error adding info node:", err)
	}

	fmt.Println("\nAfter add_info_node:")
	fmt.Printf("Path: %v\n", kb.Path())

	// Add various fields
	if _, err := kb.AddRPCServerField("info1_server", 25, "info1_server_data"); err != nil {
		log.Fatal("Error adding RPC server field:", err)
	}

	if _, err := kb.AddStatusField("info1_status",
		map[string]interface{}{"prop3": "val3"},
		"info1_status_description",
		map[string]interface{}{"prop3": "val3"}); err != nil {
		log.Fatal("Error adding status field:", err)
	}

	if _, err := kb.AddStatusField("info2_status",
		map[string]interface{}{"prop3": "val3"},
		"info2_status_description",
		map[string]interface{}{"prop3": "val3"}); err != nil {
		log.Fatal("Error adding status field 2:", err)
	}

	if _, err := kb.AddStatusField("info3_status",
		map[string]interface{}{"prop3": "val3"},
		"info3_status_description",
		map[string]interface{}{"prop3": "val3"}); err != nil {
		log.Fatal("Error adding status field 3:", err)
	}

	if _, err := kb.AddJobField("info1_job", 100, "info1_job_description"); err != nil {
		log.Fatal("Error adding job field:", err)
	}

	if _, err := kb.AddStreamField("info1_stream", 95, "info1_stream"); err != nil {
		log.Fatal("Error adding stream field:", err)
	}

	if _, err := kb.AddRPCClientField("info1_client", 10, "info1_client_description"); err != nil {
		log.Fatal("Error adding RPC client field:", err)
	}

	if err := kb.AddLinkMount("info1_link_mount", "info1_link_mount_description"); err != nil {
		log.Fatal("Error adding link mount:", err)
	}

	if err := kb.LeaveHeaderNode("header1_link", "header1_name"); err != nil {
		log.Fatal("Error leaving header node:", err)
	}

	fmt.Println("\nAfter leave_header_node:")
	fmt.Printf("Path: %v\n", kb.Path())

	if err := kb.AddHeaderNode("header2_link", "header2_name",
		map[string]interface{}{"prop3": "val3"},
		map[string]interface{}{"data": "header2_data"}, ""); err != nil {
		log.Fatal("Error adding header2 node:", err)
	}

	if err := kb.AddInfoNode("info2_link", "info2_name",
		map[string]interface{}{"prop4": "val4"},
		map[string]interface{}{"data": "info2_data"}, ""); err != nil {
		log.Fatal("Error adding info2 node:", err)
	}

	if err := kb.AddLinkNode("info1_link_mount"); err != nil {
		log.Fatal("Error adding link node:", err)
	}

	if err := kb.LeaveHeaderNode("header2_link", "header2_name"); err != nil {
		log.Fatal("Error leaving header2 node:", err)
	}

	fmt.Println("\nAfter adding and leaving another header node:")
	fmt.Printf("Path: %v\n", kb.Path())

	// Check installation
	if err := kb.CheckInstallation(); err != nil {
		fmt.Printf("Error during installation check: %v\n", err)
	}

	fmt.Println("\nFirst test scenario completed successfully!")

	// Note: The Python code has multiple test scenarios that reuse the same database
	// In Go, we would typically run these as separate test functions or restart
	// the database between scenarios to avoid conflicts
}
*/
