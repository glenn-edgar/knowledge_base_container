package kb_construct_module

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

// ConstructRPCServerTable manages RPC server table operations
type ConstructRPCServerTable struct {
	conn        *sql.DB
	constructKB *ConstructKB
	database    string
	tableName   string
}

// RPCServerFieldResult represents the result of adding an RPC server field
type RPCServerFieldResult struct {
	Status     string                 `json:"status"`
	Message    string                 `json:"message"`
	Properties map[string]interface{} `json:"properties"`
	Data       string                 `json:"data"`
}

// QueueAdjustmentResult represents the result of adjusting a queue
type QueueAdjustmentResult struct {
	Action   string `json:"action"`
	Count    int    `json:"count"`
	NewTotal int    `json:"new_total"`
	Error    string `json:"error,omitempty"`
}

// NewConstructRPCServerTable creates a new instance of ConstructRPCServerTable
func NewConstructRPCServerTable(conn *sql.DB, constructKB *ConstructKB, database string) (*ConstructRPCServerTable, error) {
	crt := &ConstructRPCServerTable{
		conn:        conn,
		constructKB: constructKB,
		database:    database,
		tableName:   database + "_rpc_server",
	}

	if err := crt.setupSchema(); err != nil {
		return nil, fmt.Errorf("error setting up schema: %w", err)
	}

	return crt, nil
}

// setupSchema sets up the database schema
func (crt *ConstructRPCServerTable) setupSchema() error {
	// Drop existing table
	dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", crt.tableName)
	if _, err := crt.conn.Exec(dropQuery); err != nil {
		return fmt.Errorf("error dropping table: %w", err)
	}

	// Create the RPC server table
	createTableQuery := fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			server_path LTREE NOT NULL,
			
			-- Request information
			request_id UUID NOT NULL DEFAULT gen_random_uuid(),
			rpc_action TEXT NOT NULL DEFAULT 'none',
			request_payload JSONB NOT NULL,
			request_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			
			-- Tag to prevent duplicate transactions
			transaction_tag TEXT NOT NULL,
			
			-- Status tracking
			state TEXT NOT NULL DEFAULT 'empty'
				CHECK (state IN ('empty', 'new_job', 'processing')),
			
			-- Additional useful fields
			priority INTEGER NOT NULL DEFAULT 0,
			
			-- New fields as requested
			processing_timestamp TIMESTAMPTZ DEFAULT NULL,
			completed_timestamp TIMESTAMPTZ DEFAULT NULL,
			rpc_client_queue LTREE
		);`, crt.tableName)

	if _, err := crt.conn.Exec(createTableQuery); err != nil {
		return fmt.Errorf("error creating table: %w", err)
	}

	fmt.Println("rpc_server table created.")
	return nil
}

// AddRPCServerField adds a new RPC server field to the knowledge base
func (crt *ConstructRPCServerTable) AddRPCServerField(rpcServerKey string, queueDepth int, description string) (*RPCServerFieldResult, error) {
	properties := map[string]interface{}{
		"queue_depth": queueDepth,
	}
	data := make(map[string]interface{})

	// Add the node to the knowledge base
	if err := crt.constructKB.AddInfoNode("KB_RPC_SERVER_FIELD", rpcServerKey, properties, data, description); err != nil {
		return nil, fmt.Errorf("error adding info node: %w", err)
	}

	fmt.Printf("Added rpc_server field '%s' with properties: %v and data: %v\n", rpcServerKey, properties, data)

	result := &RPCServerFieldResult{
		Status:     "success",
		Message:    fmt.Sprintf("RPC server field '%s' added successfully", rpcServerKey),
		Properties: properties,
		Data:       description,
	}

	return result, nil
}

// RemoveUnspecifiedEntries removes entries from rpc_server_table where server_path is not in the specified list
func (crt *ConstructRPCServerTable) RemoveUnspecifiedEntries(specifiedServerPaths []string) (int, error) {
	if len(specifiedServerPaths) == 0 {
		fmt.Println("Warning: No server_paths specified. No entries will be removed.")
		return 0, nil
	}

	// Filter out empty strings and validate paths
	var validPaths []string
	for _, path := range specifiedServerPaths {
		if path != "" {
			validPaths = append(validPaths, path)
		}
	}

	if len(validPaths) == 0 {
		fmt.Println("Warning: No valid server_paths found after filtering. No entries will be removed.")
		return 0, nil
	}

	fmt.Printf("Processing %d valid server paths\n", len(validPaths))

	// Begin transaction
	tx, err := crt.conn.Begin()
	if err != nil {
		return 0, fmt.Errorf("error beginning transaction: %w", err)
	}
	defer tx.Rollback()

	// Create temporary table
	if _, err := tx.Exec("CREATE TEMP TABLE IF NOT EXISTS valid_server_paths (path text)"); err != nil {
		return 0, fmt.Errorf("error creating temp table: %w", err)
	}

	// Clear the temp table
	if _, err := tx.Exec("DELETE FROM valid_server_paths"); err != nil {
		return 0, fmt.Errorf("error clearing temp table: %w", err)
	}

	// Insert paths in batches
	batchSize := 1000
	insertStmt, err := tx.Prepare("INSERT INTO valid_server_paths VALUES ($1)")
	if err != nil {
		return 0, fmt.Errorf("error preparing insert statement: %w", err)
	}
	defer insertStmt.Close()

	for i := 0; i < len(validPaths); i += batchSize {
		end := i + batchSize
		if end > len(validPaths) {
			end = len(validPaths)
		}

		for j := i; j < end; j++ {
			if _, err := insertStmt.Exec(validPaths[j]); err != nil {
				return 0, fmt.Errorf("error inserting path %s: %w", validPaths[j], err)
			}
		}
	}

	// Set state to empty for remaining records
	updateQuery := fmt.Sprintf(`
		UPDATE %s
		SET state = 'empty'
		WHERE server_path::text IN (
			SELECT path FROM valid_server_paths
		)`, crt.tableName)

	if _, err := tx.Exec(updateQuery); err != nil {
		return 0, fmt.Errorf("error updating state: %w", err)
	}

	// Delete entries not in our temp table
	deleteQuery := fmt.Sprintf(`
		DELETE FROM %s
		WHERE server_path::text NOT IN (
			SELECT path FROM valid_server_paths
		)`, crt.tableName)

	result, err := tx.Exec(deleteQuery)
	if err != nil {
		return 0, fmt.Errorf("error deleting entries: %w", err)
	}

	deletedCount, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("error getting rows affected: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("error committing transaction: %w", err)
	}

	// Clean up temp table (best effort)
	crt.conn.Exec("DROP TABLE IF EXISTS valid_server_paths")

	fmt.Printf("Removed %d unspecified entries from %s\n", deletedCount, crt.tableName)
	return int(deletedCount), nil
}

// AdjustQueueLength adjusts the number of records for multiple server paths to match their specified queue lengths
func (crt *ConstructRPCServerTable) AdjustQueueLength(specifiedServerPaths []string, specifiedQueueLengths []int) (map[string]QueueAdjustmentResult, error) {
	results := make(map[string]QueueAdjustmentResult)

	if len(specifiedServerPaths) != len(specifiedQueueLengths) {
		return nil, fmt.Errorf("mismatch between paths and lengths lists")
	}

	// Begin transaction
	tx, err := crt.conn.Begin()
	if err != nil {
		return nil, fmt.Errorf("error beginning transaction: %w", err)
	}
	defer tx.Rollback()

	for i, serverPath := range specifiedServerPaths {
		targetLength := specifiedQueueLengths[i]

		// Get current count
		countQuery := fmt.Sprintf(`
			SELECT COUNT(*) FROM %s 
			WHERE server_path::text = $1`, crt.tableName)

		var currentCount int
		if err := tx.QueryRow(countQuery, serverPath).Scan(&currentCount); err != nil {
			results[serverPath] = QueueAdjustmentResult{Error: err.Error()}
			continue
		}

		// Set state to empty for all records with this server_path
		updateQuery := fmt.Sprintf(`
			UPDATE %s
			SET state = 'empty'
			WHERE server_path::text = $1`, crt.tableName)

		if _, err := tx.Exec(updateQuery, serverPath); err != nil {
			results[serverPath] = QueueAdjustmentResult{Error: err.Error()}
			continue
		}

		if currentCount > targetLength {
			// Remove excess records - oldest first
			deleteQuery := fmt.Sprintf(`
				DELETE FROM %s
				WHERE id IN (
					SELECT id FROM %s
					WHERE server_path::text = $1
					ORDER BY request_timestamp ASC
					LIMIT $2
				)`, crt.tableName, crt.tableName)

			if _, err := tx.Exec(deleteQuery, serverPath, currentCount-targetLength); err != nil {
				results[serverPath] = QueueAdjustmentResult{Error: err.Error()}
				continue
			}

			results[serverPath] = QueueAdjustmentResult{
				Action:   "removed",
				Count:    currentCount - targetLength,
				NewTotal: targetLength,
			}

		} else if currentCount < targetLength {
			// Add placeholder records
			recordsToAdd := targetLength - currentCount

			insertQuery := fmt.Sprintf(`
				INSERT INTO %s (
					server_path, request_payload, transaction_tag, state
				) VALUES (
					$1, $2, $3, 'empty'
				)`, crt.tableName)

			for j := 0; j < recordsToAdd; j++ {
				transactionTag := fmt.Sprintf("placeholder_%s", uuid.New().String())
				if _, err := tx.Exec(insertQuery, serverPath, "{}", transactionTag); err != nil {
					results[serverPath] = QueueAdjustmentResult{Error: err.Error()}
					break
				}
			}

			if results[serverPath].Error == "" {
				results[serverPath] = QueueAdjustmentResult{
					Action:   "added",
					Count:    recordsToAdd,
					NewTotal: targetLength,
				}
			}

		} else {
			results[serverPath] = QueueAdjustmentResult{
				Action:   "unchanged",
				Count:    0,
				NewTotal: currentCount,
			}
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("error committing transaction: %w", err)
	}

	return results, nil
}

// RestoreDefaultValues restores default values for all fields except server_path
func (crt *ConstructRPCServerTable) RestoreDefaultValues() (int, error) {
	updateQuery := fmt.Sprintf(`
		UPDATE %s
		SET 
			request_id = gen_random_uuid(),
			rpc_action = 'none',
			request_payload = '{}'::jsonb,
			request_timestamp = NOW(),
			transaction_tag = CONCAT('reset_', gen_random_uuid()::text),
			state = 'empty',
			priority = 0,
			processing_timestamp = NULL,
			completed_timestamp = NULL,
			rpc_client_queue = NULL
		RETURNING id`, crt.tableName)

	rows, err := crt.conn.Query(updateQuery)
	if err != nil {
		return 0, fmt.Errorf("error updating records: %w", err)
	}
	defer rows.Close()

	updatedCount := 0
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return 0, fmt.Errorf("error scanning id: %w", err)
		}
		updatedCount++
	}

	fmt.Printf("Restored default values for %d records\n", updatedCount)
	return updatedCount, nil
}

// CheckInstallation synchronizes the knowledge_base and rpc_server_table
func (crt *ConstructRPCServerTable) CheckInstallation() error {
	// Get specified paths from knowledge_table
	query := fmt.Sprintf(`
		SELECT path, properties FROM %s 
		WHERE label = 'KB_RPC_SERVER_FIELD';`, crt.database)

	rows, err := crt.conn.Query(query)
	if err != nil {
		return fmt.Errorf("error querying knowledge base: %w", err)
	}
	defer rows.Close()

	var paths []string
	var lengths []int

	for rows.Next() {
		var path string
		var propertiesJSON []byte

		if err := rows.Scan(&path, &propertiesJSON); err != nil {
			return fmt.Errorf("error scanning row: %w", err)
		}

		var properties map[string]interface{}
		if err := json.Unmarshal(propertiesJSON, &properties); err != nil {
			return fmt.Errorf("error unmarshaling properties: %w", err)
		}

		paths = append(paths, path)
		
		// Extract queue_depth
		if queueDepth, ok := properties["queue_depth"].(float64); ok {
			lengths = append(lengths, int(queueDepth))
		} else {
			return fmt.Errorf("queue_depth not found or invalid for path %s", path)
		}
	}

	fmt.Printf("paths: %v, lengths: %v\n", paths, lengths)

	// Execute the three operations
	if _, err := crt.RemoveUnspecifiedEntries(paths); err != nil {
		return fmt.Errorf("error removing unspecified entries: %w", err)
	}

	if _, err := crt.AdjustQueueLength(paths, lengths); err != nil {
		return fmt.Errorf("error adjusting queue length: %w", err)
	}

	if _, err := crt.RestoreDefaultValues(); err != nil {
		return fmt.Errorf("error restoring default values: %w", err)
	}

	return nil
}

