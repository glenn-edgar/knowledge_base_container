package kb_construct_module

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

// ConstructRPCClientTable manages RPC client table operations
type ConstructRPCClientTable struct {
	conn        *sql.DB
	constructKB *ConstructKB
	database    string
	tableName   string
}

// RPCClientFieldResult represents the result of adding an RPC client field
type RPCClientFieldResult struct {
	RPCClient  string                 `json:"rpc_client"`
	Message    string                 `json:"message"`
	Properties map[string]interface{} `json:"properties"`
	Data       string                 `json:"data"`
}

// QueueAdjustmentClientResult represents the result of queue adjustment for a client path
type QueueAdjustmentClientResult struct {
	Added   int    `json:"added"`
	Removed int    `json:"removed"`
	Error   string `json:"error,omitempty"`
}

// NewConstructRPCClientTable creates a new instance of ConstructRPCClientTable
func NewConstructRPCClientTable(conn *sql.DB, constructKB *ConstructKB, database string) (*ConstructRPCClientTable, error) {
	crt := &ConstructRPCClientTable{
		conn:        conn,
		constructKB: constructKB,
		database:    database,
		tableName:   database + "_rpc_client",
	}

	if err := crt.setupSchema(); err != nil {
		return nil, fmt.Errorf("error setting up schema: %w", err)
	}

	return crt, nil
}

// setupSchema sets up the database schema
func (crt *ConstructRPCClientTable) setupSchema() error {
	// Create ltree extension
	if _, err := crt.conn.Exec("CREATE EXTENSION IF NOT EXISTS ltree;"); err != nil {
		return fmt.Errorf("error creating ltree extension: %w", err)
	}

	// Drop existing table
	dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", crt.tableName)
	if _, err := crt.conn.Exec(dropQuery); err != nil {
		return fmt.Errorf("error dropping table: %w", err)
	}

	// Create the RPC client table
	createTableQuery := fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			
			-- Reference to the request
			request_id UUID NOT NULL,
			
			-- Path to identify the RPC client queue for routing responses
			client_path ltree NOT NULL,
			server_path ltree NOT NULL,
			
			-- Response information
			transaction_tag TEXT NOT NULL DEFAULT 'none',
			rpc_action TEXT NOT NULL DEFAULT 'none',

			response_payload JSONB NOT NULL,
			response_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- UTC timestamp
			
			-- Boolean to identify new/unprocessed results
			is_new_result BOOLEAN NOT NULL DEFAULT FALSE
		);`, crt.tableName)

	if _, err := crt.conn.Exec(createTableQuery); err != nil {
		return fmt.Errorf("error creating table: %w", err)
	}

	fmt.Println("rpc_client table created.")
	return nil
}

// AddRPCClientField adds a new RPC client field to the knowledge base
func (crt *ConstructRPCClientTable) AddRPCClientField(rpcClientKey string, queueDepth int, description string) (*RPCClientFieldResult, error) {
	properties := map[string]interface{}{
		"queue_depth": queueDepth,
	}

	// Add the node to the knowledge base
	if err := crt.constructKB.AddInfoNode("KB_RPC_CLIENT_FIELD", rpcClientKey, properties, map[string]interface{}{}, description); err != nil {
		return nil, fmt.Errorf("error adding info node: %w", err)
	}

	fmt.Printf("Added rpc_client field '%s' with properties: %v\n", rpcClientKey, properties)

	result := &RPCClientFieldResult{
		RPCClient:  "success",
		Message:    fmt.Sprintf("rpc_client field '%s' added successfully", rpcClientKey),
		Properties: properties,
		Data:       description,
	}

	return result, nil
}

// RemoveUnspecifiedEntries removes entries from rpc_client_table where client_path is not in the specified list
func (crt *ConstructRPCClientTable) RemoveUnspecifiedEntries(specifiedClientPaths []string) (int, error) {
	if len(specifiedClientPaths) == 0 {
		fmt.Println("Warning: No client_paths specified. No entries will be removed.")
		return 0, nil
	}

	// Filter out empty strings
	var validPaths []string
	for _, path := range specifiedClientPaths {
		if path != "" {
			validPaths = append(validPaths, path)
		}
	}

	if len(validPaths) == 0 {
		fmt.Println("Warning: No valid client_paths found after filtering. No entries will be removed.")
		return 0, nil
	}

	fmt.Printf("Processing %d valid client paths\n", len(validPaths))

	// Begin transaction
	tx, err := crt.conn.Begin()
	if err != nil {
		return 0, fmt.Errorf("error beginning transaction: %w", err)
	}
	defer tx.Rollback()

	// Create temporary table
	if _, err := tx.Exec("CREATE TEMP TABLE IF NOT EXISTS valid_client_paths (path text)"); err != nil {
		return 0, fmt.Errorf("error creating temp table: %w", err)
	}

	// Clear the temp table
	if _, err := tx.Exec("DELETE FROM valid_client_paths"); err != nil {
		return 0, fmt.Errorf("error clearing temp table: %w", err)
	}

	// Insert paths in batches
	batchSize := 1000
	insertStmt, err := tx.Prepare("INSERT INTO valid_client_paths VALUES ($1)")
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

	// Delete entries not in our temp table
	deleteQuery := fmt.Sprintf(`
		DELETE FROM %s
		WHERE client_path::text NOT IN (
			SELECT path FROM valid_client_paths
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
	crt.conn.Exec("DROP TABLE IF EXISTS valid_client_paths")

	fmt.Printf("Removed %d unspecified entries from %s\n", deletedCount, crt.tableName)
	return int(deletedCount), nil
}

// AdjustQueueLength adjusts the number of records for multiple client paths to match their specified queue lengths
func (crt *ConstructRPCClientTable) AdjustQueueLength(specifiedClientPaths []string, specifiedQueueLengths []int) (map[string]QueueAdjustmentClientResult, error) {
	if len(specifiedClientPaths) != len(specifiedQueueLengths) {
		return nil, fmt.Errorf("the specified_client_paths and specified_queue_lengths lists must be of equal length")
	}

	results := make(map[string]QueueAdjustmentClientResult)

	// Begin transaction
	tx, err := crt.conn.Begin()
	if err != nil {
		return nil, fmt.Errorf("error beginning transaction: %w", err)
	}
	defer tx.Rollback()

	for i, clientPath := range specifiedClientPaths {
		queueLength := specifiedQueueLengths[i]

		if queueLength < 0 {
			results[clientPath] = QueueAdjustmentClientResult{Error: "Invalid queue length (negative)"}
			continue
		}

		// Count current records
		countQuery := fmt.Sprintf(`
			SELECT COUNT(*) 
			FROM %s
			WHERE client_path = $1::ltree`, crt.tableName)

		var currentCount int
		if err := tx.QueryRow(countQuery, clientPath).Scan(&currentCount); err != nil {
			results[clientPath] = QueueAdjustmentClientResult{Error: err.Error()}
			continue
		}

		pathResult := QueueAdjustmentClientResult{Added: 0, Removed: 0}

		// Remove excess records
		if currentCount > queueLength {
			recordsToRemove := currentCount - queueLength
			deleteQuery := fmt.Sprintf(`
				DELETE FROM %s
				WHERE id IN (
					SELECT id
					FROM %s
					WHERE client_path = $1::ltree
					ORDER BY response_timestamp ASC
					LIMIT $2
				)
				RETURNING id`, crt.tableName, crt.tableName)

			rows, err := tx.Query(deleteQuery, clientPath, recordsToRemove)
			if err != nil {
				results[clientPath] = QueueAdjustmentClientResult{Error: err.Error()}
				continue
			}
			defer rows.Close()

			removedCount := 0
			for rows.Next() {
				var id int
				if err := rows.Scan(&id); err == nil {
					removedCount++
				}
			}
			pathResult.Removed = removedCount

		} else if currentCount < queueLength {
			// Add missing records
			recordsToAdd := queueLength - currentCount
			insertQuery := fmt.Sprintf(`
				INSERT INTO %s (
					request_id, client_path, server_path,
					transaction_tag, rpc_action,
					response_payload, response_timestamp, is_new_result
				)
				VALUES ($1, $2::ltree, $3::ltree, $4, $5, $6::jsonb, NOW(), FALSE)`, crt.tableName)

			for j := 0; j < recordsToAdd; j++ {
				requestID := uuid.New().String()
				emptyPayload := "{}"

				if _, err := tx.Exec(insertQuery,
					requestID,
					clientPath,
					clientPath, // default server_path = client_path
					"none",     // default transaction_tag
					"none",     // default rpc_action
					emptyPayload,
				); err != nil {
					results[clientPath] = QueueAdjustmentClientResult{Error: err.Error()}
					break
				}
				pathResult.Added++
			}
		}

		results[clientPath] = pathResult
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("error committing transaction: %w", err)
	}

	return results, nil
}

// RestoreDefaultValues restores default values for all fields except client_path
func (crt *ConstructRPCClientTable) RestoreDefaultValues() (int, error) {
	updateQuery := fmt.Sprintf(`
		UPDATE %s
		SET 
			request_id = (SELECT gen_random_uuid()),  -- Unique UUID per record
			server_path = client_path,  -- Set server_path to match client_path
			transaction_tag = 'none',
			rpc_action = 'none',
			response_payload = '{}'::jsonb,
			response_timestamp = NOW(),
			is_new_result = FALSE
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

	return updatedCount, nil
}

// CheckInstallation synchronizes the knowledge_base and rpc_client_table
func (crt *ConstructRPCClientTable) CheckInstallation() error {
	// Get specified paths from knowledge_table
	query := fmt.Sprintf(`
		SELECT path, properties FROM %s 
		WHERE label = 'KB_RPC_CLIENT_FIELD';`, crt.database)

	rows, err := crt.conn.Query(query)
	if err != nil {
		return fmt.Errorf("error retrieving knowledge base fields: %w", err)
	}
	defer rows.Close()

	var paths []string
	var lengths []int

	fmt.Println("specified_paths_data:")
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

		fmt.Printf("  path: %s, properties: %v\n", path, properties)
	}

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

