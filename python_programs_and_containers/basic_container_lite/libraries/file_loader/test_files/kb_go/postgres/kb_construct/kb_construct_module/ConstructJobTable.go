package kb_construct_module

import (
	"database/sql"
	"encoding/json"
	"fmt"

	_ "github.com/lib/pq"
)

// ConstructJobTable manages job table operations
type ConstructJobTable struct {
	conn        *sql.DB
	constructKB *ConstructKB
	database    string
	tableName   string
}

// JobFieldResult represents the result of adding a job field
type JobFieldResult struct {
	Job        string                 `json:"job"`
	Message    string                 `json:"message"`
	Properties map[string]interface{} `json:"properties"`
	Data       map[string]interface{} `json:"data"`
}

// NewConstructJobTable creates a new instance of ConstructJobTable
func NewConstructJobTable(conn *sql.DB, constructKB *ConstructKB, database string) (*ConstructJobTable, error) {
	cjt := &ConstructJobTable{
		conn:        conn,
		constructKB: constructKB,
		database:    database,
		tableName:   database + "_job",
	}

	if err := cjt.setupSchema(); err != nil {
		return nil, fmt.Errorf("error setting up schema: %w", err)
	}

	return cjt, nil
}

// setupSchema sets up the database schema
func (cjt *ConstructJobTable) setupSchema() error {
	// Create ltree extension
	if _, err := cjt.conn.Exec("CREATE EXTENSION IF NOT EXISTS ltree;"); err != nil {
		return fmt.Errorf("error creating ltree extension: %w", err)
	}

	// Drop existing table
	dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", cjt.tableName)
	if _, err := cjt.conn.Exec(dropQuery); err != nil {
		return fmt.Errorf("error dropping table: %w", err)
	}

	// Create the job table
	createTableQuery := fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			path LTREE,
			schedule_at TIMESTAMPTZ DEFAULT NOW(),
			started_at TIMESTAMPTZ DEFAULT NOW(),
			completed_at TIMESTAMPTZ DEFAULT NOW(),
			is_active BOOLEAN DEFAULT FALSE,
			valid BOOLEAN DEFAULT FALSE,
			data JSONB
		);`, cjt.tableName)

	if _, err := cjt.conn.Exec(createTableQuery); err != nil {
		return fmt.Errorf("error creating table: %w", err)
	}

	// Create indexes
	indexes := []string{
		// GIST index for ltree path operations
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_path_gist ON %s USING GIST (path);",
			cjt.tableName, cjt.tableName),

		// B-tree index on path for exact lookups
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_path_btree ON %s (path);",
			cjt.tableName, cjt.tableName),

		// Index on schedule_at for job scheduling
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_schedule_at ON %s (schedule_at);",
			cjt.tableName, cjt.tableName),

		// Index on is_active for filtering
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_is_active ON %s (is_active);",
			cjt.tableName, cjt.tableName),

		// Index on valid for filtering
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_valid ON %s (valid);",
			cjt.tableName, cjt.tableName),

		// Composite index on is_active and schedule_at
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_active_schedule ON %s (is_active, schedule_at);",
			cjt.tableName, cjt.tableName),

		// Index on started_at
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_started_at ON %s (started_at);",
			cjt.tableName, cjt.tableName),

		// Index on completed_at
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_completed_at ON %s (completed_at);",
			cjt.tableName, cjt.tableName),
	}

	for _, indexQuery := range indexes {
		if _, err := cjt.conn.Exec(indexQuery); err != nil {
			return fmt.Errorf("error creating index: %w", err)
		}
	}

	fmt.Printf("Job table '%s' created with optimized indexes.\n", cjt.tableName)
	return nil
}

// AddJobField adds a new job field to the knowledge base
func (cjt *ConstructJobTable) AddJobField(jobKey string, jobLength int, description string) (*JobFieldResult, error) {
	properties := map[string]interface{}{
		"job_length": jobLength,
	}
	data := make(map[string]interface{})

	// Add the node to the knowledge base
	if err := cjt.constructKB.AddInfoNode("KB_JOB_QUEUE", jobKey, properties, data, description); err != nil {
		return nil, fmt.Errorf("error adding info node: %w", err)
	}

	fmt.Printf("Added job field '%s' with properties: %v and data: %v\n", jobKey, properties, data)

	result := &JobFieldResult{
		Job:        "success",
		Message:    fmt.Sprintf("job field '%s' added successfully", jobKey),
		Properties: properties,
		Data:       data,
	}

	return result, nil
}

// manageJobTable manages the number of records to match specified job lengths
func (cjt *ConstructJobTable) manageJobTable(specifiedJobPaths []string, specifiedJobLength []int) error {
	fmt.Printf("specified_job_paths: %v\n", specifiedJobPaths)
	fmt.Printf("specified_job_length: %v\n", specifiedJobLength)

	// Begin transaction
	tx, err := cjt.conn.Begin()
	if err != nil {
		return fmt.Errorf("error beginning transaction: %w", err)
	}
	defer tx.Rollback()

	for i := 0; i < len(specifiedJobPaths); i++ {
		path := specifiedJobPaths[i]
		targetLength := specifiedJobLength[i]

		// Get current count for this path
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE path = $1;", cjt.tableName)
		var currentCount int
		if err := tx.QueryRow(countQuery, path).Scan(&currentCount); err != nil {
			return fmt.Errorf("error counting records for path %s: %w", path, err)
		}
		fmt.Printf("current_count: %d\n", currentCount)

		diff := targetLength - currentCount

		if diff < 0 {
			// Need to remove records (oldest first)
			deleteQuery := fmt.Sprintf(`
				DELETE FROM %s
				WHERE path = $1 AND completed_at IN (
					SELECT completed_at 
					FROM %s 
					WHERE path = $2
					ORDER BY completed_at ASC 
					LIMIT $3
				);`, cjt.tableName, cjt.tableName)

			if _, err := tx.Exec(deleteQuery, path, path, -diff); err != nil {
				return fmt.Errorf("error deleting excess records: %w", err)
			}

		} else if diff > 0 {
			// Need to add records
			insertQuery := fmt.Sprintf(`
				INSERT INTO %s (path, data)
				VALUES ($1, $2);`, cjt.tableName)

			for j := 0; j < diff; j++ {
				if _, err := tx.Exec(insertQuery, path, nil); err != nil {
					return fmt.Errorf("error inserting new records: %w", err)
				}
			}
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	fmt.Println("Job table management completed.")
	return nil
}

// removeInvalidJobFields removes database entries with invalid paths
func (cjt *ConstructJobTable) removeInvalidJobFields(invalidJobPaths []string, chunkSize int) error {
	if len(invalidJobPaths) == 0 {
		return nil
	}

	// Begin transaction
	tx, err := cjt.conn.Begin()
	if err != nil {
		return fmt.Errorf("error beginning transaction: %w", err)
	}
	defer tx.Rollback()

	// Process in chunks
	for i := 0; i < len(invalidJobPaths); i += chunkSize {
		end := i + chunkSize
		if end > len(invalidJobPaths) {
			end = len(invalidJobPaths)
		}
		chunk := invalidJobPaths[i:end]

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

		deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE path IN (%s);", cjt.tableName, placeholders)
		if _, err := tx.Exec(deleteQuery, args...); err != nil {
			return fmt.Errorf("error deleting invalid job fields: %w", err)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	return nil
}

// CheckInstallation synchronizes the knowledge_base and job_table
func (cjt *ConstructJobTable) CheckInstallation() error {
	// Get all unique paths from job_table
	uniquePathsQuery := fmt.Sprintf("SELECT DISTINCT path::text FROM %s", cjt.tableName)
	rows, err := cjt.conn.Query(uniquePathsQuery)
	if err != nil {
		return fmt.Errorf("error querying unique job paths: %w", err)
	}
	defer rows.Close()

	var uniqueJobPaths []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return fmt.Errorf("error scanning job path: %w", err)
		}
		uniqueJobPaths = append(uniqueJobPaths, path)
	}
	fmt.Printf("unique_job_paths: %v\n", uniqueJobPaths)

	// Get specified paths from knowledge_table
	knowledgeQuery := fmt.Sprintf(`
		SELECT path, label, name, properties FROM %s 
		WHERE label = 'KB_JOB_QUEUE';`, cjt.database)

	rows, err = cjt.conn.Query(knowledgeQuery)
	if err != nil {
		return fmt.Errorf("error querying knowledge base: %w", err)
	}
	defer rows.Close()

	var specifiedJobPaths []string
	var specifiedJobLength []int

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

		specifiedJobPaths = append(specifiedJobPaths, path)

		// Extract job_length from properties
		if jobLength, ok := properties["job_length"].(float64); ok {
			specifiedJobLength = append(specifiedJobLength, int(jobLength))
		} else {
			return fmt.Errorf("job_length not found or invalid for path %s", path)
		}
	}

	fmt.Printf("specified_job_paths: %v\n", specifiedJobPaths)
	fmt.Printf("specified_job_length: %v\n", specifiedJobLength)

	// Find invalid paths (in job_table but not in knowledge_base)
	invalidJobPaths := findDifference(uniqueJobPaths, specifiedJobPaths)

	// Remove invalid job fields
	if err := cjt.removeInvalidJobFields(invalidJobPaths, 500); err != nil {
		return fmt.Errorf("error removing invalid job fields: %w", err)
	}

	// Manage job table
	if err := cjt.manageJobTable(specifiedJobPaths, specifiedJobLength); err != nil {
		return fmt.Errorf("error managing job table: %w", err)
	}

	return nil
}


