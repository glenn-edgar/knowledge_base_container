package kb_construct_module

import (
	"database/sql"
	//"encoding/json"
	"fmt"

	_ "github.com/lib/pq"
)

// ConstructStatusTable manages status table operations
type ConstructStatusTable struct {
	conn        *sql.DB
	constructKB *ConstructKB // Reference to ConstructKB instance
	database    string
	tableName   string
}

// StatusFieldResult represents the result of adding a status field
type StatusFieldResult struct {
	Status     string                 `json:"status"`
	Message    string                 `json:"message"`
	Properties map[string]interface{} `json:"properties"`
	Data       map[string]interface{} `json:"data"`
}

// CheckInstallationResult represents the result of check installation
type CheckInstallationResult struct {
	MissingPathsAdded       int `json:"missing_paths_added"`
	NotSpecifiedPathsRemoved int `json:"not_specified_paths_removed"`
}

// NewConstructStatusTable creates a new instance of ConstructStatusTable
func NewConstructStatusTable(conn *sql.DB, constructKB *ConstructKB, database string) (*ConstructStatusTable, error) {
	cst := &ConstructStatusTable{
		conn:        conn,
		constructKB: constructKB,
		database:    database,
		tableName:   database + "_status",
	}

	fmt.Printf("database: %s\n", database)

	if err := cst.setupSchema(); err != nil {
		return nil, fmt.Errorf("error setting up schema: %w", err)
	}

	return cst, nil
}

// setupSchema sets up the database schema
func (cst *ConstructStatusTable) setupSchema() error {
	// Create ltree extension
	if _, err := cst.conn.Exec("CREATE EXTENSION IF NOT EXISTS ltree;"); err != nil {
		return fmt.Errorf("error creating ltree extension: %w", err)
	}

	// Drop existing table
	dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", cst.tableName)
	if _, err := cst.conn.Exec(dropQuery); err != nil {
		return fmt.Errorf("error dropping table: %w", err)
	}

	// Create the status table
	createTableQuery := fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			data JSON,
			path LTREE UNIQUE
		);`, cst.tableName)

	if _, err := cst.conn.Exec(createTableQuery); err != nil {
		return fmt.Errorf("error creating table: %w", err)
	}

	// Create indexes
	indexes := []string{
		// GIST index for ltree path operations
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_path_gist ON %s USING GIST (path);",
			cst.tableName, cst.tableName),

		// B-tree index on path for exact lookups
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_path_btree ON %s (path);",
			cst.tableName, cst.tableName),
	}

	for _, indexQuery := range indexes {
		if _, err := cst.conn.Exec(indexQuery); err != nil {
			return fmt.Errorf("error creating index: %w", err)
		}
	}

	fmt.Printf("Status table '%s' created with optimized indexes.\n", cst.tableName)
	return nil
}

// AddStatusField adds a new status field to the knowledge base
func (cst *ConstructStatusTable) AddStatusField(statusKey string, properties map[string]interface{}, description string, initialData map[string]interface{}) (*StatusFieldResult, error) {
	// Type validation is implicit in Go's type system
	// but we can still check for nil maps
	if properties == nil {
		properties = make(map[string]interface{})
	}
	if initialData == nil {
		initialData = make(map[string]interface{})
	}

	fmt.Printf("Added status field '%s' with properties: %v and data: %v\n", statusKey, properties, initialData)

	// Add the node to the knowledge base
	if err := cst.constructKB.AddInfoNode("KB_STATUS_FIELD", statusKey, properties, initialData, description); err != nil {
		return nil, fmt.Errorf("error adding info node: %w", err)
	}

	result := &StatusFieldResult{
		Status:     "success",
		Message:    fmt.Sprintf("Status field '%s' added successfully", statusKey),
		Properties: properties,
		Data:       initialData,
	}

	return result, nil
}

// CheckInstallation synchronizes the knowledge_base and status_table based on paths
func (cst *ConstructStatusTable) CheckInstallation() (*CheckInstallationResult, error) {
	// Get all paths from status_table
	getPathsQuery := fmt.Sprintf("SELECT path FROM %s;", cst.tableName)
	rows, err := cst.conn.Query(getPathsQuery)
	if err != nil {
		return nil, fmt.Errorf("error querying status table paths: %w", err)
	}
	defer rows.Close()

	var allPaths []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, fmt.Errorf("error scanning path: %w", err)
		}
		allPaths = append(allPaths, path)
	}

	// Get specified paths (paths with label "KB_STATUS_FIELD") from knowledge_table
	specifiedPathsQuery := fmt.Sprintf(`
		SELECT path FROM %s 
		WHERE label = 'KB_STATUS_FIELD';`, cst.database)

	rows, err = cst.conn.Query(specifiedPathsQuery)
	if err != nil {
		return nil, fmt.Errorf("error querying knowledge base paths: %w", err)
	}
	defer rows.Close()

	var specifiedPaths []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, fmt.Errorf("error scanning specified path: %w", err)
		}
		specifiedPaths = append(specifiedPaths, path)
	}

	fmt.Printf("specified_paths: %v\n", specifiedPaths)

	// Find missing paths (in specified_paths but not in all_paths)
	missingPaths := findDifference(specifiedPaths, allPaths)
	fmt.Printf("missing_paths: %v\n", missingPaths)

	// Find not specified paths (in all_paths but not in specified_paths)
	notSpecifiedPaths := findDifference(allPaths, specifiedPaths)
	fmt.Printf("not_specified_paths: %v\n", notSpecifiedPaths)

	// Begin transaction for consistency
	tx, err := cst.conn.Begin()
	if err != nil {
		return nil, fmt.Errorf("error beginning transaction: %w", err)
	}
	defer tx.Rollback()

	// Process not_specified_paths: remove entries from status_table
	deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE path = $1;", cst.tableName)
	deleteStmt, err := tx.Prepare(deleteQuery)
	if err != nil {
		return nil, fmt.Errorf("error preparing delete statement: %w", err)
	}
	defer deleteStmt.Close()

	for _, path := range notSpecifiedPaths {
		fmt.Printf("deleting path: %s\n", path)
		if _, err := deleteStmt.Exec(path); err != nil {
			return nil, fmt.Errorf("error deleting path %s: %w", path, err)
		}
	}

	// Process missing_paths: add entries to status_table
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (data, path)
		VALUES ($1, $2);`, cst.tableName)
	insertStmt, err := tx.Prepare(insertQuery)
	if err != nil {
		return nil, fmt.Errorf("error preparing insert statement: %w", err)
	}
	defer insertStmt.Close()

	for _, path := range missingPaths {
		fmt.Printf("inserting path: %s\n", path)
		if _, err := insertStmt.Exec("{}", path); err != nil {
			return nil, fmt.Errorf("error inserting path %s: %w", path, err)
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("error committing transaction: %w", err)
	}

	result := &CheckInstallationResult{
		MissingPathsAdded:        len(missingPaths),
		NotSpecifiedPathsRemoved: len(notSpecifiedPaths),
	}

	return result, nil
}


