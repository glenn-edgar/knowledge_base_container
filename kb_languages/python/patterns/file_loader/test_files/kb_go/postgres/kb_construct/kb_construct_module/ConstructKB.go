package kb_construct_module

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
)

// ConstructKB extends KnowledgeBaseManager with stack-based path management
type ConstructKB struct {
	*KnowledgeBaseManager
	path       map[string][]string          // Stack to keep track of paths for each KB
	pathValues map[string]map[string]bool   // Track existing paths for each KB
	workingKB  string                       // Currently selected knowledge base
}

// NewConstructKB creates a new instance of ConstructKB
func NewConstructKB(host string, port int, dbname, user, password, tableName string) (*ConstructKB, error) {
	// Create connection parameters
	connParams := ConnectionParams{
		Host:     host,
		Port:     port,
		Database: dbname,
		User:     user,
		Password: password,
	}

	// Create base KnowledgeBaseManager
	kbManager, err := NewKnowledgeBaseManager(tableName, connParams)
	if err != nil {
		return nil, fmt.Errorf("error creating knowledge base manager: %w", err)
	}

	// Create ConstructKB with embedded KnowledgeBaseManager
	ckb := &ConstructKB{
		KnowledgeBaseManager: kbManager,
		path:                make(map[string][]string),
		pathValues:          make(map[string]map[string]bool),
	}

	return ckb, nil
}

// GetDBObjects returns both the database connection and a statement (Go doesn't use cursors)
func (ckb *ConstructKB) GetDBObjects() (*sql.DB, *sql.DB) {
	return ckb.conn, ckb.conn // In Go, we return the connection twice as we don't have cursors
}

// AddKB adds a new knowledge base with path tracking
func (ckb *ConstructKB) AddKB(kbName, description string) error {
	// Check if KB already exists in path
	if _, exists := ckb.path[kbName]; exists {
		return fmt.Errorf("knowledge base %s already exists", kbName)
	}

	// Initialize path tracking for this KB
	ckb.path[kbName] = []string{kbName}
	ckb.pathValues[kbName] = make(map[string]bool)

	// Call parent AddKB method
	return ckb.KnowledgeBaseManager.AddKB(kbName, description)
}

// SelectKB selects a knowledge base to work with
func (ckb *ConstructKB) SelectKB(kbName string) error {
	if _, exists := ckb.path[kbName]; !exists {
		return fmt.Errorf("knowledge base %s does not exist", kbName)
	}
	ckb.workingKB = kbName
	return nil
}

// AddHeaderNode adds a header node to the knowledge base
func (ckb *ConstructKB) AddHeaderNode(link, nodeName string, nodeProperties, nodeData map[string]interface{}, description string) error {
	// Validate working KB is selected
	if ckb.workingKB == "" {
		return fmt.Errorf("no knowledge base selected")
	}

	// Add description to properties if provided
	if description != "" {
		if nodeProperties == nil {
			nodeProperties = make(map[string]interface{})
		}
		nodeProperties["description"] = description
	}

	// Update path
	ckb.path[ckb.workingKB] = append(ckb.path[ckb.workingKB], link)
	ckb.path[ckb.workingKB] = append(ckb.path[ckb.workingKB], nodeName)
	
	// Build node path
	nodePath := strings.Join(ckb.path[ckb.workingKB], ".")

	// Check if path already exists
	if ckb.pathValues[ckb.workingKB][nodePath] {
		// Remove the added elements before returning error
		ckb.path[ckb.workingKB] = ckb.path[ckb.workingKB][:len(ckb.path[ckb.workingKB])-2]
		return fmt.Errorf("path %s already exists in knowledge base", nodePath)
	}

	// Mark path as used
	ckb.pathValues[ckb.workingKB][nodePath] = true

	// Get current path
	path := strings.Join(ckb.path[ckb.workingKB], ".")
	fmt.Println("path", path)

	// Add node using parent method
	return ckb.KnowledgeBaseManager.AddNode(ckb.workingKB, link, nodeName, nodeProperties, nodeData, path)
}

// AddInfoNode adds an info node (non-hierarchical) to the knowledge base
func (ckb *ConstructKB) AddInfoNode(link, nodeName string, nodeProperties, nodeData map[string]interface{}, description string) error {
	// Add the node as a header node
	if err := ckb.AddHeaderNode(link, nodeName, nodeProperties, nodeData, description); err != nil {
		return err
	}

	// Remove the node from the path stack (info nodes don't create hierarchy)
	path := ckb.path[ckb.workingKB]
	if len(path) >= 2 {
		ckb.path[ckb.workingKB] = path[:len(path)-2] // Remove both nodeName and link
	}

	return nil
}

// LeaveHeaderNode leaves a header node, verifying the label and name
func (ckb *ConstructKB) LeaveHeaderNode(label, name string) error {
	// Validate working KB is selected
	if ckb.workingKB == "" {
		return fmt.Errorf("no knowledge base selected")
	}

	// Check if path is empty
	path := ckb.path[ckb.workingKB]
	if len(path) == 0 {
		return fmt.Errorf("cannot leave a header node: path is empty")
	}

	// Pop name
	if len(path) < 2 {
		return fmt.Errorf("cannot leave a header node: not enough elements in path")
	}
	
	refName := path[len(path)-1]
	path = path[:len(path)-1]
	
	// Pop label
	refLabel := path[len(path)-1]
	path = path[:len(path)-1]
	
	// Update the path in the map
	ckb.path[ckb.workingKB] = path

	// Verify the popped values
	var errors []string
	if refName != name {
		errors = append(errors, fmt.Sprintf("expected name '%s', but got '%s'", name, refName))
	}
	if refLabel != label {
		errors = append(errors, fmt.Sprintf("expected label '%s', but got '%s'", label, refLabel))
	}

	if len(errors) > 0 {
		// Restore the path on error
		ckb.path[ckb.workingKB] = append(ckb.path[ckb.workingKB], refLabel, refName)
		return fmt.Errorf("%s", strings.Join(errors, ", "))
	}

	return nil
}

// AddLinkNode adds a link node at the current path
func (ckb *ConstructKB) AddLinkNode(linkName string) error {
	if ckb.workingKB == "" {
		return fmt.Errorf("no knowledge base selected")
	}

	currentPath := strings.Join(ckb.path[ckb.workingKB], ".")
	return ckb.KnowledgeBaseManager.AddLink(ckb.workingKB, currentPath, linkName)
}

// AddLinkMount adds a link mount at the current path
func (ckb *ConstructKB) AddLinkMount(linkMountName, description string) error {
	if ckb.workingKB == "" {
		return fmt.Errorf("no knowledge base selected")
	}

	currentPath := strings.Join(ckb.path[ckb.workingKB], ".")
	_, _, err := ckb.KnowledgeBaseManager.AddLinkMount(ckb.workingKB, currentPath, linkMountName, description)
	return err
}

// CheckInstallation verifies that all paths are properly reset
func (ckb *ConstructKB) CheckInstallation() error {
	for kbName, path := range ckb.path {
		if len(path) != 1 {
			return fmt.Errorf("installation check failed: path is not empty for knowledge base %s. Path: %v", kbName, path)
		}
		if path[0] != kbName {
			return fmt.Errorf("installation check failed: path root mismatch for knowledge base %s. Path: %v", kbName, path)
		}
	}
	return nil
}
/*
// Example usage and unit test
func main() {
	// Database configuration
	dbHost := "localhost"
	dbPort := 5432
	dbName := "knowledge_base"
	dbUser := "gedgar"
	dbTable := "knowledge_base"

	// Get password from user
	fmt.Print("Enter your password: ")
	var dbPassword string
	fmt.Scanln(&dbPassword)

	fmt.Println("starting unit test")

	// Create ConstructKB instance
	kb, err := NewConstructKB(dbHost, dbPort, dbName, dbUser, dbPassword, dbTable)
	if err != nil {
		fmt.Printf("Error creating ConstructKB: %v\n", err)
		return
	}
	defer kb.Disconnect()

	// Add first knowledge base
	if err := kb.AddKB("kb1", "First knowledge base"); err != nil {
		fmt.Printf("Error adding kb1: %v\n", err)
		return
	}

	// Select kb1
	if err := kb.SelectKB("kb1"); err != nil {
		fmt.Printf("Error selecting kb1: %v\n", err)
		return
	}

	// Add header node
	if err := kb.AddHeaderNode("header1_link", "header1_name", 
		map[string]interface{}{"prop1": "val1"}, 
		map[string]interface{}{"data": "header1_data"}, ""); err != nil {
		fmt.Printf("Error adding header node: %v\n", err)
		return
	}

	// Add info node
	if err := kb.AddInfoNode("info1_link", "info1_name",
		map[string]interface{}{"prop2": "val2"},
		map[string]interface{}{"data": "info1_data"}, ""); err != nil {
		fmt.Printf("Error adding info node: %v\n", err)
		return
	}

	// Leave header node
	if err := kb.LeaveHeaderNode("header1_link", "header1_name"); err != nil {
		fmt.Printf("Error leaving header node: %v\n", err)
		return
	}

	// Add second header node
	if err := kb.AddHeaderNode("header2_link", "header2_name",
		map[string]interface{}{"prop3": "val3"},
		map[string]interface{}{"data": "header2_data"}, ""); err != nil {
		fmt.Printf("Error adding header2 node: %v\n", err)
		return
	}

	// Add info node under header2
	if err := kb.AddInfoNode("info2_link", "info2_name",
		map[string]interface{}{"prop4": "val4"},
		map[string]interface{}{"data": "info2_data"}, ""); err != nil {
		fmt.Printf("Error adding info2 node: %v\n", err)
		return
	}

	// Add link mount
	if err := kb.AddLinkMount("link1", "link1 description"); err != nil {
		fmt.Printf("Error adding link mount: %v\n", err)
		return
	}

	// Leave second header node
	if err := kb.LeaveHeaderNode("header2_link", "header2_name"); err != nil {
		fmt.Printf("Error leaving header2 node: %v\n", err)
		return
	}

	// Add second knowledge base
	if err := kb.AddKB("kb2", "Second knowledge base"); err != nil {
		fmt.Printf("Error adding kb2: %v\n", err)
		return
	}

	// Select kb2
	if err := kb.SelectKB("kb2"); err != nil {
		fmt.Printf("Error selecting kb2: %v\n", err)
		return
	}

	// Add nodes to kb2
	if err := kb.AddHeaderNode("header1_link", "header1_name",
		map[string]interface{}{"prop1": "val1"},
		map[string]interface{}{"data": "header1_data"}, ""); err != nil {
		fmt.Printf("Error adding header node to kb2: %v\n", err)
		return
	}

	if err := kb.AddInfoNode("info1_link", "info1_name",
		map[string]interface{}{"prop2": "val2"},
		map[string]interface{}{"data": "info1_data"}, ""); err != nil {
		fmt.Printf("Error adding info node to kb2: %v\n", err)
		return
	}

	if err := kb.LeaveHeaderNode("header1_link", "header1_name"); err != nil {
		fmt.Printf("Error leaving header node in kb2: %v\n", err)
		return
	}

	if err := kb.AddHeaderNode("header2_link", "header2_name",
		map[string]interface{}{"prop3": "val3"},
		map[string]interface{}{"data": "header2_data"}, ""); err != nil {
		fmt.Printf("Error adding header2 node to kb2: %v\n", err)
		return
	}

	if err := kb.AddInfoNode("info2_link", "info2_name",
		map[string]interface{}{"prop4": "val4"},
		map[string]interface{}{"data": "info2_data"}, ""); err != nil {
		fmt.Printf("Error adding info2 node to kb2: %v\n", err)
		return
	}

	// Add link node (different from kb1 which has link mount)
	if err := kb.AddLinkNode("link1"); err != nil {
		fmt.Printf("Error adding link node: %v\n", err)
		return
	}

	if err := kb.LeaveHeaderNode("header2_link", "header2_name"); err != nil {
		fmt.Printf("Error leaving header2 node in kb2: %v\n", err)
		return
	}

	// Check installation
	if err := kb.CheckInstallation(); err != nil {
		fmt.Printf("Error during installation check: %v\n", err)
		return
	}

	fmt.Println("ending unit test")
}
*/