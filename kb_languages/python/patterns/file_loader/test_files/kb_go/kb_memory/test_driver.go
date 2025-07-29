package main

import (
	"fmt"
	"log"
	"os"
	//"strings"
	km "github.com/glenn-edgar/knowledge_base/kb_modules/kb_go/kb_memory/kb_memory_module"
	_ "github.com/lib/pq" // Import the PostgreSQL driver
)


// ExampleSearchMemDBUsage demonstrates how to use SearchMemDB
func ExampleSearchMemDBUsage() {
	fmt.Println("Starting SearchMemDB example")

	// Replace with your actual database credentials
	dbHost := "localhost"
	dbPort := 5432
	dbName := "knowledge_base"
	dbUser := "gedgar"
	dbPassword := os.Getenv("POSTGRES_PASSWORD") // In real usage, get this securely
	tableName := "composite_memory_kb"

	kb, err := km.NewSearchMemDB(dbHost, dbPort, dbName, dbUser, dbPassword, tableName)
	if err != nil {
		log.Printf("Error creating SearchMemDB: %v", err)
		return
	}

	fmt.Printf("Decoded keys: %v\n", getStringMapKeys(kb.DecodedKeys))
	
	// Test various search operations
	fmt.Println("----------------------------------")
	
	// Search by knowledge base
	kb.ClearFilters()
	kb.SearchKB("kb1")
	fmt.Printf("Search KB results: %v\n", getMapKeys(kb.FilterResults))
	
	fmt.Println("----------------------------------")
	
	// Search by label
	kb.SearchLabel("info1_link")
	fmt.Printf("Search label results: %v\n", getMapKeys(kb.FilterResults))
	
	// Search by name
	kb.SearchName("info1_name")
	fmt.Printf("Search name results: %v\n", getMapKeys(kb.FilterResults))
	
	fmt.Println("----------------------------------")
	
	// Search by property value
	kb.ClearFilters()
	results := kb.SearchPropertyValue("data", "info1_data")
	fmt.Printf("Search property value results: %v\n", getMapKeys(results))
	
	fmt.Println("----------------------------------")
	
	// Search by property key
	kb.ClearFilters()
	results = kb.SearchPropertyKey("data")
	fmt.Printf("Search property key results: %v\n", getMapKeys(results))
	
	fmt.Println("----------------------------------")
	
	// Search starting path
	kb.ClearFilters()
	results, err = kb.SearchStartingPath("kb2.header2_link.header2_name")
	if err != nil {
		log.Printf("Error in SearchStartingPath: %v", err)
	} else {
		fmt.Printf("Search starting path results: %v\n", getMapKeys(results))
	}
	
	fmt.Println("----------------------------------")
	
	// Search path with operator
	kb.ClearFilters()
	results = kb.SearchPath("~", "kb2.**")
	fmt.Printf("Search path results: %v\n", getMapKeys(results))
	
	fmt.Println("----------------------------------")
	
	// Find descriptions
	kb.ClearFilters()
	descriptions := kb.FindDescriptions("kb2.header2_link.header2_name")
	fmt.Printf("Find descriptions results: %v\n", descriptions)
	
	fmt.Println("----------------------------------")
}

// Helper function to extract keys from a map
func getMapKeys(m map[string]*km.TreeNode) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

// Helper function to extract keys from string map
func getStringMapKeys(m map[string][]string) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}


// ExampleUsage demonstrates how to use ConstructMemDB
func ExampleConstructMemDBUsage() {
	fmt.Println("Starting unit test")

	// Replace with your actual database credentials
	dbHost := "localhost"
	dbPort := 5432
	dbName := "knowledge_base"
	dbUser := "gedgar"
	dbPassword := os.Getenv("POSTGRES_PASSWORD") // In real usage, get this securely
	dbTable := "knowledge_base"

	kb := km.NewConstructMemDB(dbHost, dbPort, dbName, dbUser, dbPassword, dbTable)

	// Test KB1
	err := kb.AddKB("kb1", "First knowledge base")
	if err != nil {
		log.Printf("Error adding kb1: %v", err)
		return
	}

	err = kb.SelectKB("kb1")
	if err != nil {
		log.Printf("Error selecting kb1: %v", err)
		return
	}

	err = kb.AddHeaderNode("header1_link", "header1_name", map[string]interface{}{"data": "header1_data"}, "header1_description")
	if err != nil {
		log.Printf("Error adding header1: %v", err)
		return
	}

	err = kb.AddInfoNode("info1_link", "info1_name", map[string]interface{}{"data": "info1_data"}, "info1_description")
	if err != nil {
		log.Printf("Error adding info1: %v", err)
		return
	}

	err = kb.LeaveHeaderNode("header1_link", "header1_name")
	if err != nil {
		log.Printf("Error leaving header1: %v", err)
		return
	}

	err = kb.AddHeaderNode("header2_link", "header2_name", map[string]interface{}{"data": "header2_data"}, "header2_description")
	if err != nil {
		log.Printf("Error adding header2: %v", err)
		return
	}

	err = kb.AddInfoNode("info2_link", "info2_name", map[string]interface{}{"data": "info2_data"}, "info2_description")
	if err != nil {
		log.Printf("Error adding info2: %v", err)
		return
	}

	err = kb.LeaveHeaderNode("header2_link", "header2_name")
	if err != nil {
		log.Printf("Error leaving header2: %v", err)
		return
	}

	// Test KB2
	err = kb.AddKB("kb2", "Second knowledge base")
	if err != nil {
		log.Printf("Error adding kb2: %v", err)
		return
	}

	err = kb.SelectKB("kb2")
	if err != nil {
		log.Printf("Error selecting kb2: %v", err)
		return
	}

	err = kb.AddHeaderNode("header1_link", "header1_name", map[string]interface{}{"data": "header1_data"}, "header1_description")
	if err != nil {
		log.Printf("Error adding header1 to kb2: %v", err)
		return
	}

	err = kb.AddInfoNode("info1_link", "info1_name", map[string]interface{}{"data": "info1_data"}, "info1_description")
	if err != nil {
		log.Printf("Error adding info1 to kb2: %v", err)
		return
	}

	err = kb.LeaveHeaderNode("header1_link", "header1_name")
	if err != nil {
		log.Printf("Error leaving header1 in kb2: %v", err)
		return
	}

	err = kb.AddHeaderNode("header2_link", "header2_name", map[string]interface{}{"data": "header2_data"}, "header2_description")
	if err != nil {
		log.Printf("Error adding header2 to kb2: %v", err)
		return
	}

	err = kb.AddInfoNode("info2_link", "info2_name", map[string]interface{}{"data": "info2_data"}, "info2_description")
	if err != nil {
		log.Printf("Error adding info2 to kb2: %v", err)
		return
	}

	err = kb.LeaveHeaderNode("header2_link", "header2_name")
	if err != nil {
		log.Printf("Error leaving header2 in kb2: %v", err)
		return
	}

	// Check installation
	err = kb.CheckInstallation()
	if err != nil {
		log.Printf("Error during installation check: %v", err)
		return
	}

	// Export and import from PostgreSQL
	exported, err := kb.ExportToPostgres("composite_memory_kb", true, true)
	if err != nil {
		log.Printf("Export error: %v", err)
	} else {
		fmt.Printf("Exported %d records\n", exported)
	}

	imported, err := kb.ImportFromPostgres("composite_memory_kb", "path", "data", "created_at", "updated_at")
	if err != nil {
		log.Printf("Import error: %v", err)
	} else {
		fmt.Printf("Imported %d records\n", imported)
	}

	fmt.Println("Ending unit test")
}



// Example usage function
func ExampleUsage() {
	// Initialize the enhanced tree storage system
	tree := km.NewBasicConstructDB("localhost", 5432, "knowledge_base", "gedgar", os.Getenv("POSTGRES_PASSWORD"), "knowledge_base")

	fmt.Println("=== Full ltree-Compatible Tree Storage System ===")

	// Sample data setup
	sampleData := []struct {
		path string
		data map[string]interface{}
	}{
		{"company", map[string]interface{}{"name": "TechCorp", "type": "corporation"}},
		{"company.engineering", map[string]interface{}{"name": "Engineering", "type": "department"}},
		{"company.engineering.backend", map[string]interface{}{"name": "Backend Team", "type": "team"}},
		{"company.engineering.backend.api", map[string]interface{}{"name": "API Service", "type": "service"}},
		{"company.engineering.backend.database", map[string]interface{}{"name": "Database Team", "type": "service"}},
		{"company.engineering.frontend", map[string]interface{}{"name": "Frontend Team", "type": "team"}},
		{"company.engineering.frontend.web", map[string]interface{}{"name": "Web App", "type": "service"}},
		{"company.engineering.frontend.mobile", map[string]interface{}{"name": "Mobile App", "type": "service"}},
		{"company.marketing", map[string]interface{}{"name": "Marketing", "type": "department"}},
		{"company.marketing.digital", map[string]interface{}{"name": "Digital Marketing", "type": "team"}},
		{"company.marketing.content", map[string]interface{}{"name": "Content Team", "type": "team"}},
		{"company.sales", map[string]interface{}{"name": "Sales", "type": "department"}},
		{"company.sales.enterprise", map[string]interface{}{"name": "Enterprise Sales", "type": "team"}},
		{"company.sales.smb", map[string]interface{}{"name": "SMB Sales", "type": "team"}},
	}

	// Store sample data
	for _, item := range sampleData {
		err := tree.Store(item.path, item.data, nil, nil)
		if err != nil {
			log.Printf("Error storing %s: %v", item.path, err)
		}
	}

	fmt.Printf("Stored %d nodes\n", len(sampleData))

	// Demonstrate full ltree query capabilities
	fmt.Println("\n=== Full ltree Query Demonstrations ===")

	// 1. Basic pattern matching
	fmt.Println("\n1. Basic pattern queries:")

	fmt.Println("  a) All direct children of engineering:")
	results := tree.Query("company.engineering.*")
	for _, r := range results {
		if dataMap, ok := r.Data.(map[string]interface{}); ok {
			fmt.Printf("    %s: %s\n", r.Path, dataMap["name"])
		}
	}

	fmt.Println("  b) All descendants of engineering:")
	results = tree.Query("company.engineering.**")
	for _, r := range results {
		if dataMap, ok := r.Data.(map[string]interface{}); ok {
			fmt.Printf("    %s: %s\n", r.Path, dataMap["name"])
		}
	}

	// 2. Ancestor tests (@>)
	fmt.Println("\n2. @ Operator Tests:")
	fmt.Println("  a) Ancestor relationships (@>):")
	testPairs := [][]string{
		{"company", "company.engineering.backend"},
		{"company.engineering", "company.engineering.backend.api"},
		{"company.sales", "company.engineering.backend"},
	}

	for _, pair := range testPairs {
		ancestor, descendant := pair[0], pair[1]
		isAncestor := tree.LtreeAncestor(ancestor, descendant)
		fmt.Printf("    '%s' @> '%s': %t\n", ancestor, descendant, isAncestor)
	}

	// Query using @> operator
	fmt.Println("  b) Find all descendants of 'company.engineering' using @> operator:")
	results = tree.QueryByOperator("@>", "company.engineering", "")
	for _, r := range results {
		if dataMap, ok := r.Data.(map[string]interface{}); ok {
			fmt.Printf("    %s: %s\n", r.Path, dataMap["name"])
		}
	}

	// 3. ltxtquery (@@ operator) demonstrations
	fmt.Println("\n3. ltxtquery (@@ operator) Tests:")

	fmt.Println("  a) Find paths containing 'engineering':")
	results = tree.QueryLtxtquery("engineering")
	for _, r := range results {
		if dataMap, ok := r.Data.(map[string]interface{}); ok {
			fmt.Printf("    %s: %s\n", r.Path, dataMap["name"])
		}
	}

	// 4. ltree functions
	fmt.Println("\n4. ltree Function Tests:")

	testPath := "company.engineering.backend.api"
	fmt.Printf("  Path: %s\n", testPath)
	fmt.Printf("  nlevel(): %d\n", tree.Nlevel(testPath))
	fmt.Printf("  subpath(1, 2): %s\n", tree.SubpathFunc(testPath, 1, func() *int { i := 2; return &i }()))
	fmt.Printf("  subltree(1, 3): %s\n", tree.Subltree(testPath, 1, 3))
	fmt.Printf("  index('engineering'): %d\n", tree.IndexFunc(testPath, "engineering", 0))

	// LCA test
	fmt.Println("\n  Longest Common Ancestor (LCA):")
	testPaths := []string{
		"company.engineering.backend.api",
		"company.engineering.backend.database",
		"company.engineering.frontend.web",
	}
	lcaResult := tree.LCA(testPaths...)
	if lcaResult != nil {
		fmt.Printf("    LCA of %v: %s\n", testPaths, *lcaResult)
	} else {
		fmt.Printf("    LCA of %v: nil\n", testPaths)
	}

	// 5. Tree statistics
	fmt.Println("\n5. Tree Statistics:")
	stats := tree.GetStats()
	fmt.Printf("  total_nodes: %d\n", stats.TotalNodes)
	fmt.Printf("  max_depth: %d\n", stats.MaxDepth)
	fmt.Printf("  avg_depth: %.2f\n", stats.AvgDepth)
	fmt.Printf("  root_nodes: %d\n", stats.RootNodes)
	fmt.Printf("  leaf_nodes: %d\n", stats.LeafNodes)

	// 6. PostgreSQL integration example
	fmt.Println("\n6. PostgreSQL Integration Example:")

	// Export to PostgreSQL
	exported, err := tree.ExportToPostgres(tree.TableName, true, false)
	if err != nil {
		fmt.Printf("Export error: %v\n", err)
	} else {
		fmt.Printf("Exported %d records\n", exported)
	}

	// Import from PostgreSQL
	imported, err := tree.ImportFromPostgres(tree.TableName, "path", "data", "created_at", "updated_at")
	if err != nil {
		fmt.Printf("Import error: %v\n", err)
	} else {
		fmt.Printf("Imported %d records\n", imported)
	}

	// Bidirectional sync
	syncStats := tree.SyncWithPostgres("both")
	fmt.Printf("Sync stats: imported=%d, exported=%d\n", syncStats.Imported, syncStats.Exported)

	fmt.Printf("\n=== System Ready - %d nodes loaded ===\n", tree.Size())
}



func main() {
	ExampleUsage()
	ExampleConstructMemDBUsage()
	ExampleSearchMemDBUsage()
}
