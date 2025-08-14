package kb_construct_module
import (
	"database/sql"
	"encoding/json"
	"fmt"
	//"time"

	_ "github.com/lib/pq"
)

// ConstructStreamTable manages stream table operations with header and info nodes
type ConstructStreamTable struct {
	conn        *sql.DB
	constructKB *ConstructKB 
	database    string
	tableName   string
}

// StreamFieldResult represents the result of adding a stream field
type StreamFieldResult struct {
	Stream     string                 `json:"stream"`
	Message    string                 `json:"message"`
	Properties map[string]interface{} `json:"properties"`
	Data       string                 `json:"data"`
}

// NewConstructStreamTable creates a new instance of ConstructStreamTable
func NewConstructStreamTable(conn *sql.DB, constructKB *ConstructKB, database string) (*ConstructStreamTable, error) {
	cst := &ConstructStreamTable{
		conn:        conn,
		constructKB: constructKB,
		database:    database,
		tableName:   database + "_stream",
	}

	if err := cst.setupSchema(); err != nil {
		return nil, fmt.Errorf("error setting up schema: %w", err)
	}

	return cst, nil
}

// setupSchema sets up the database schema (tables, functions, etc.)
func (cst *ConstructStreamTable) setupSchema() error {
	// Create ltree extension
	if _, err := cst.conn.Exec("CREATE EXTENSION IF NOT EXISTS ltree;"); err != nil {
		return fmt.Errorf("error creating ltree extension: %w", err)
	}

	// Drop existing table
	dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", cst.tableName)
	if _, err := cst.conn.Exec(dropQuery); err != nil {
		return fmt.Errorf("error dropping table: %w", err)
	}

	// Create the stream table
	createTableQuery := fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			path LTREE,
			recorded_at TIMESTAMPTZ DEFAULT NOW(),
			valid BOOLEAN DEFAULT FALSE,
			data JSONB
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
		
		// Index on recorded_at
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_recorded_at ON %s (recorded_at);", 
			cst.tableName, cst.tableName),
		
		// Descending index on recorded_at
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_recorded_at_desc ON %s (recorded_at DESC);", 
			cst.tableName, cst.tableName),
		
		// Composite index on path and recorded_at
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_path_recorded_at ON %s (path, recorded_at);", 
			cst.tableName, cst.tableName),
	}

	for _, indexQuery := range indexes {
		if _, err := cst.conn.Exec(indexQuery); err != nil {
			return fmt.Errorf("error creating index: %w", err)
		}
	}

	return nil
}

// AddStreamField adds a new stream field to the knowledge base
func (cst *ConstructStreamTable) AddStreamField(streamKey string, streamLength int, description string) (*StreamFieldResult, error) {
	properties := map[string]interface{}{
		"stream_length": streamLength,
	}

	// Add the node to the knowledge base
	// Note: This assumes constructKB has an AddInfoNode method similar to the Python version
	// You may need to adjust this based on your actual KnowledgeBaseManager implementation
	if err := cst.constructKB.AddInfoNode("KB_STREAM_FIELD", streamKey, properties, map[string]interface{}{}, description); err != nil {
		return nil, fmt.Errorf("error adding info node: %w", err)
	}

	result := &StreamFieldResult{
		Stream:     "success",
		Message:    fmt.Sprintf("stream field '%s' added successfully", streamKey),
		Properties: properties,
		Data:       description,
	}

	return result, nil
}

// removeInvalidStreamFields removes database entries with paths matching invalid_stream_paths
func (cst *ConstructStreamTable) removeInvalidStreamFields(invalidStreamPaths []string, chunkSize int) error {
	if len(invalidStreamPaths) == 0 {
		return nil
	}

	// Process in chunks
	for i := 0; i < len(invalidStreamPaths); i += chunkSize {
		end := i + chunkSize
		if end > len(invalidStreamPaths) {
			end = len(invalidStreamPaths)
		}
		chunk := invalidStreamPaths[i:end]

		// Build the IN clause placeholders
		placeholders := ""
		args := make([]interface{}, len(chunk))
		for j, path := range chunk {
			if j > 0 {
				placeholders += ", "
			}
			placeholders += fmt.Sprintf("$%d", j+1)
			args[j] = path
		}

		deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE path IN (%s);", cst.tableName, placeholders)
		if _, err := cst.conn.Exec(deleteQuery, args...); err != nil {
			return fmt.Errorf("error deleting invalid stream fields: %w", err)
		}
	}

	return nil
}

// manageStreamTable manages the number of records to match specified stream lengths
func (cst *ConstructStreamTable) manageStreamTable(specifiedStreamPaths []string, specifiedStreamLength []int) error {
	for i := 0; i < len(specifiedStreamPaths); i++ {
		path := specifiedStreamPaths[i]
		targetLength := specifiedStreamLength[i]

		// Count current records for this path
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE path = $1;", cst.tableName)
		var currentCount int
		err := cst.conn.QueryRow(countQuery, path).Scan(&currentCount)
		if err != nil {
			return fmt.Errorf("error counting records for path %s: %w", path, err)
		}

		diff := targetLength - currentCount

		if diff < 0 {
			// Need to remove records (oldest first)
			deleteQuery := fmt.Sprintf(`
				DELETE FROM %s
				WHERE path = $1 AND recorded_at IN (
					SELECT recorded_at 
					FROM %s
					WHERE path = $2
					ORDER BY recorded_at ASC 
					LIMIT $3
				);`, cst.tableName, cst.tableName)

			if _, err := cst.conn.Exec(deleteQuery, path, path, -diff); err != nil {
				return fmt.Errorf("error deleting excess records: %w", err)
			}

		} else if diff > 0 {
			// Need to add records
			insertQuery := fmt.Sprintf(`
				INSERT INTO %s (path, recorded_at, data, valid)
				VALUES ($1, CURRENT_TIMESTAMP, $2, FALSE);`, cst.tableName)

			for j := 0; j < diff; j++ {
				if _, err := cst.conn.Exec(insertQuery, path, "{}"); err != nil {
					return fmt.Errorf("error inserting new records: %w", err)
				}
			}
		}
	}

	return nil
}

// CheckInstallation synchronizes the knowledge_base and stream_table based on paths
func (cst *ConstructStreamTable) CheckInstallation() error {
	// Get all unique paths from stream_table
	streamPathsQuery := fmt.Sprintf("SELECT DISTINCT path::text FROM %s;", cst.tableName)
	rows, err := cst.conn.Query(streamPathsQuery)
	if err != nil {
		return fmt.Errorf("error querying stream paths: %w", err)
	}
	defer rows.Close()

	var uniqueStreamPaths []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return fmt.Errorf("error scanning stream path: %w", err)
		}
		uniqueStreamPaths = append(uniqueStreamPaths, path)
	}

	// Get specified paths from knowledge_table
	knowledgeQuery := fmt.Sprintf(`
		SELECT path, label, name, properties FROM %s 
		WHERE label = 'KB_STREAM_FIELD';`, cst.database)

	rows, err = cst.conn.Query(knowledgeQuery)
	if err != nil {
		return fmt.Errorf("error querying knowledge base: %w", err)
	}
	defer rows.Close()

	var specifiedStreamPaths []string
	var specifiedStreamLength []int

	for rows.Next() {
		var path, label, name string
		var propertiesJSON []byte
		
		if err := rows.Scan(&path, &label, &name, &propertiesJSON); err != nil {
			return fmt.Errorf("error scanning knowledge row: %w", err)
		}

		var properties map[string]interface{}
		if err := json.Unmarshal(propertiesJSON, &properties); err != nil {
			return fmt.Errorf("error unmarshaling properties: %w", err)
		}

		specifiedStreamPaths = append(specifiedStreamPaths, path)
		
		// Extract stream_length from properties
		if streamLength, ok := properties["stream_length"].(float64); ok {
			specifiedStreamLength = append(specifiedStreamLength, int(streamLength))
		} else {
			return fmt.Errorf("stream_length not found or invalid for path %s", path)
		}
	}

	fmt.Printf("specified_stream_paths: %v\n", specifiedStreamPaths)
	fmt.Printf("specified_stream_length: %v\n", specifiedStreamLength)

	// Find invalid paths (in stream_table but not in knowledge_base)
	invalidStreamPaths := findDifference(uniqueStreamPaths, specifiedStreamPaths)
	
	// Find missing paths (in knowledge_base but not in stream_table)
	missingStreamPaths := findDifference(specifiedStreamPaths, uniqueStreamPaths)

	fmt.Printf("invalid_stream_paths: %v\n", invalidStreamPaths)
	fmt.Printf("missing_stream_paths: %v\n", missingStreamPaths)

	// Remove invalid stream fields
	if err := cst.removeInvalidStreamFields(invalidStreamPaths, 500); err != nil {
		return fmt.Errorf("error removing invalid stream fields: %w", err)
	}

	// Manage stream table
	if err := cst.manageStreamTable(specifiedStreamPaths, specifiedStreamLength); err != nil {
		return fmt.Errorf("error managing stream table: %w", err)
	}

	return nil
}

// Helper function to find elements in slice1 that are not in slice2
func findDifference(slice1, slice2 []string) []string {
	m := make(map[string]bool)
	for _, item := range slice2 {
		m[item] = true
	}

	var diff []string
	for _, item := range slice1 {
		if !m[item] {
			diff = append(diff, item)
		}
	}
	return diff
}

// Note: The KnowledgeBaseManager would need an AddInfoNode method like this:
// func (kb *KnowledgeBaseManager) AddInfoNode(label, name string, properties, data map[string]interface{}, description string) error {
//     // Implementation would be similar to AddNode but with specific logic for info nodes
//     return nil
// }


