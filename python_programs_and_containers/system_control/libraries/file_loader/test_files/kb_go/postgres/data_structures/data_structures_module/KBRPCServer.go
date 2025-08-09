package data_structures_module

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

// NoMatchingRecordError represents when no matching record is found
type NoMatchingRecordError struct {
	Message string
}

func (e *NoMatchingRecordError) Error() string {
	return e.Message
}

// KBRPCServer handles RPC server operations for the knowledge base
type KBRPCServer struct {
	KBSearch  *KBSearch
	conn      *sql.DB
	BaseTable string
}

// RPCRecord represents a single RPC record
type RPCRecord struct {
	ID                  int                    `json:"id"`
	ServerPath          string                 `json:"server_path"`
	RequestID           string                 `json:"request_id"`
	RPCAction           string                 `json:"rpc_action"`
	RequestPayload      map[string]interface{} `json:"request_payload"`
	TransactionTag      string                 `json:"transaction_tag"`
	Priority            int                    `json:"priority"`
	RPCClientQueue      *string                `json:"rpc_client_queue"`
	State               string                 `json:"state"`
	RequestTimestamp    *time.Time             `json:"request_timestamp"`
	ProcessingTimestamp *time.Time             `json:"processing_timestamp"`
	CompletedTimestamp  *time.Time             `json:"completed_timestamp"`
}

// JobCounts represents counts of different job states
type JobCounts struct {
	EmptyJobs      int `json:"empty_jobs"`
	NewJobs        int `json:"new_jobs"`
	ProcessingJobs int `json:"processing_jobs"`
}

// NewKBRPCServer creates a new KBRPCServer instance
func NewKBRPCServer(kbSearch *KBSearch, database string) *KBRPCServer {
	return &KBRPCServer{
		KBSearch:  kbSearch,
		conn:      kbSearch.conn,
		BaseTable: fmt.Sprintf("%s_rpc_server", database),
	}
}

// FindRPCServerID finds a single RPC server id for given parameters
func (rpc *KBRPCServer) FindRPCServerID(kb *string, nodeName *string, properties map[string]interface{}, nodePath *string) (map[string]interface{}, error) {
	results, err := rpc.FindRPCServerIDs(kb, nodeName, properties, nodePath)
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("no node found matching path parameters: %v, %v, %v", nodeName, properties, nodePath)
	}
	if len(results) > 1 {
		return nil, fmt.Errorf("multiple nodes found matching path parameters: %v, %v, %v", nodeName, properties, nodePath)
	}

	return results[0], nil
}

// FindRPCServerIDs finds all RPC server ids matching the given parameters
func (rpc *KBRPCServer) FindRPCServerIDs(kb *string, nodeName *string, properties map[string]interface{}, nodePath *string) ([]map[string]interface{}, error) {
	rpc.KBSearch.ClearFilters()
	rpc.KBSearch.SearchLabel("KB_RPC_SERVER_FIELD")

	if kb != nil {
		rpc.KBSearch.SearchKB(*kb)
	}
	if nodeName != nil {
		rpc.KBSearch.SearchName(*nodeName)
	}
	if properties != nil {
		for key, value := range properties {
			rpc.KBSearch.SearchPropertyValue(key, value)
		}
	}
	if nodePath != nil {
		rpc.KBSearch.SearchPath(*nodePath)
	}

	nodeIDs, err := rpc.KBSearch.ExecuteQuery()
	if err != nil {
		return nil, err
	}

	if len(nodeIDs) == 0 {
		return nil, fmt.Errorf("no node found matching path parameters: %v, %v, %v", nodeName, properties, nodePath)
	}

	return nodeIDs, nil
}

// FindRPCServerTableKeys extracts path values from RPC server query results
func (rpc *KBRPCServer) FindRPCServerTableKeys(keyData []map[string]interface{}) []string {
	returnValues := []string{}
	for _, key := range keyData {
		if path, ok := key["path"].(string); ok {
			returnValues = append(returnValues, path)
		}
	}
	return returnValues
}

// ListJobsJobTypes lists records matching server path and state
func (rpc *KBRPCServer) ListJobsJobTypes(serverPath string, state string) ([]map[string]interface{}, error) {
	// Validate server_path
	if serverPath == "" || !rpc.isValidLTree(serverPath) {
		return nil, fmt.Errorf("server_path must be a non-empty valid ltree string (e.g., 'root.node1')")
	}

	// Validate state
	allowedStates := map[string]bool{"empty": true, "new_job": true, "processing": true}
	if !allowedStates[state] {
		return nil, fmt.Errorf("state must be one of: empty, new_job, processing")
	}

	query := fmt.Sprintf(`
		SELECT *
		FROM %s
		WHERE server_path = $1::ltree
		  AND state = $2
		ORDER BY priority DESC, request_timestamp ASC
	`, rpc.BaseTable)

	rows, err := rpc.conn.Query(query, serverPath, state)
	if err != nil {
		return nil, fmt.Errorf("database error in list_jobs_job_types: %v", err)
	}
	defer rows.Close()

	results, err := rowsToMaps(rows)
	if err != nil {
		return nil, err
	}

	return results, nil
}

// CountAllJobs counts all jobs by state for a server path
func (rpc *KBRPCServer) CountAllJobs(serverPath string) (*JobCounts, error) {
	emptyJobs, err := rpc.CountEmptyJobs(serverPath)
	if err != nil {
		return nil, err
	}

	newJobs, err := rpc.CountNewJobs(serverPath)
	if err != nil {
		return nil, err
	}

	processingJobs, err := rpc.CountProcessingJobs(serverPath)
	if err != nil {
		return nil, err
	}

	return &JobCounts{
		EmptyJobs:      emptyJobs,
		NewJobs:        newJobs,
		ProcessingJobs: processingJobs,
	}, nil
}

// CountProcessingJobs counts processing jobs for a server path
func (rpc *KBRPCServer) CountProcessingJobs(serverPath string) (int, error) {
	return rpc.CountJobsJobTypes(serverPath, "processing")
}

// CountNewJobs counts new jobs for a server path
func (rpc *KBRPCServer) CountNewJobs(serverPath string) (int, error) {
	return rpc.CountJobsJobTypes(serverPath, "new_job")
}

// CountEmptyJobs counts empty jobs for a server path
func (rpc *KBRPCServer) CountEmptyJobs(serverPath string) (int, error) {
	return rpc.CountJobsJobTypes(serverPath, "empty")
}

// CountJobsJobTypes counts jobs by type for a server path
func (rpc *KBRPCServer) CountJobsJobTypes(serverPath string, state string) (int, error) {
	if serverPath == "" || !rpc.isValidLTree(serverPath) {
		return 0, fmt.Errorf("server_path must be a valid ltree format (e.g., 'root.node1.node2')")
	}

	validStates := map[string]bool{"empty": true, "new_job": true, "processing": true, "completed_job": true}
	if !validStates[state] {
		return 0, fmt.Errorf("state must be one of: empty, new_job, processing, completed_job")
	}

	query := fmt.Sprintf(`
		SELECT COUNT(*) AS job_count
		FROM %s
		WHERE server_path = $1::ltree
		  AND state = $2
	`, rpc.BaseTable)

	var count int
	err := rpc.conn.QueryRow(query, serverPath, state).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("database error in count_jobs_job_types: %v", err)
	}

	return count, nil
}

// PushRPCQueue pushes a request to the RPC queue
func (rpc *KBRPCServer) PushRPCQueue(serverPath, requestID, rpcAction string, requestPayload map[string]interface{},
	transactionTag string, priority int, rpcClientQueue *string, maxRetries int, waitTime time.Duration) (map[string]interface{}, error) {

	// Validate server_path
	if serverPath == "" || !rpc.isValidLTree(serverPath) {
		return nil, fmt.Errorf("server_path must be a valid ltree format (e.g. 'root.node1.node2')")
	}

	// Validate request_id (UUID)
	if requestID == "" {
		requestID = uuid.New().String()
	} else {
		if _, err := uuid.Parse(requestID); err != nil {
			return nil, fmt.Errorf("request_id must be a valid UUID string or empty")
		}
	}

	// Validate rpc_action
	if rpcAction == "" {
		return nil, fmt.Errorf("rpc_action must be a non-empty string")
	}

	// Validate request_payload
	if requestPayload == nil {
		return nil, fmt.Errorf("request_payload cannot be nil")
	}

	// Validate transaction_tag
	if transactionTag == "" {
		return nil, fmt.Errorf("transaction_tag must be a non-empty string")
	}

	// Validate rpc_client_queue
	if rpcClientQueue != nil && (*rpcClientQueue == "" || !rpc.isValidLTree(*rpcClientQueue)) {
		return nil, fmt.Errorf("rpc_client_queue must be nil or a valid ltree format")
	}

	// Convert payload to JSON
	payloadJSON, err := json.Marshal(requestPayload)
	if err != nil {
		return nil, fmt.Errorf("request_payload must be JSON-serializable: %v", err)
	}

	if maxRetries <= 0 {
		maxRetries = 5
	}
	if waitTime <= 0 {
		waitTime = 500 * time.Millisecond
	}

	maxWait := 8 * time.Second
	attempt := 0

	for attempt < maxRetries {
		tx, err := rpc.conn.Begin()
		if err != nil {
			return nil, fmt.Errorf("failed to begin transaction: %v", err)
		}

		// Set isolation level
		_, err = tx.Exec("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
		if err != nil {
			tx.Rollback()
			return nil, err
		}

		// Acquire advisory lock
		h := fnv.New32a()
		h.Write([]byte(fmt.Sprintf("%s:%s", rpc.BaseTable, serverPath)))
		lockKey := int64(h.Sum32())
		
		_, err = tx.Exec("SELECT pg_advisory_xact_lock($1)", lockKey)
		if err != nil {
			tx.Rollback()
			return nil, err
		}

		// Find earliest empty record
		findQuery := fmt.Sprintf(`
			SELECT id FROM %s
			WHERE state = 'empty'
			ORDER BY priority DESC, request_timestamp ASC
			LIMIT 1
			FOR UPDATE
		`, rpc.BaseTable)

		var recordID int
		err = tx.QueryRow(findQuery).Scan(&recordID)
		if err != nil {
			tx.Rollback()
			if err == sql.ErrNoRows {
				return nil, &NoMatchingRecordError{Message: "No matching record found with state = 'empty'"}
			}
			return nil, err
		}

		// Update the record
		updateQuery := fmt.Sprintf(`
			UPDATE %s
			SET server_path = $1,
				request_id = $2,
				rpc_action = $3,
				request_payload = $4,
				transaction_tag = $5,
				priority = $6,
				rpc_client_queue = $7,
				state = 'new_job',
				request_timestamp = NOW() AT TIME ZONE 'UTC',
				completed_timestamp = NULL
			WHERE id = $8
			RETURNING *
		`, rpc.BaseTable)

		rows, err := tx.Query(updateQuery, serverPath, requestID, rpcAction, string(payloadJSON),
			transactionTag, priority, rpcClientQueue, recordID)
		if err != nil {
			tx.Rollback()
			if isSerializationError(err) && attempt < maxRetries-1 {
				attempt++
				sleepTime := minDuration(waitTime*time.Duration(1<<uint(attempt)), maxWait)
				time.Sleep(sleepTime)
				continue
			}
			return nil, fmt.Errorf("failed to update record: %v", err)
		}
		defer rows.Close()

		results, err := rowsToMaps(rows)
		if err != nil {
			tx.Rollback()
			return nil, err
		}

		if len(results) == 0 {
			tx.Rollback()
			return nil, fmt.Errorf("failed to update record in RPC queue")
		}

		if err := tx.Commit(); err != nil {
			if isSerializationError(err) && attempt < maxRetries-1 {
				attempt++
				sleepTime := minDuration(waitTime*time.Duration(1<<uint(attempt)), maxWait)
				time.Sleep(sleepTime)
				continue
			}
			return nil, err
		}

		return results[0], nil
	}

	return nil, fmt.Errorf("failed to push to RPC queue after %d retries", maxRetries)
}

// PeakServerQueue finds and processes one pending record from the server queue
func (rpc *KBRPCServer) PeakServerQueue(serverPath string, retries int, waitTime time.Duration) (map[string]interface{}, error) {
	if retries <= 0 {
		retries = 5
	}
	if waitTime <= 0 {
		waitTime = time.Second
	}

	attempt := 0
	for attempt < retries {
		tx, err := rpc.conn.Begin()
		if err != nil {
			return nil, fmt.Errorf("failed to begin transaction: %v", err)
		}

		_, err = tx.Exec("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
		if err != nil {
			tx.Rollback()
			return nil, err
		}

		// Select one pending job
		selectQuery := fmt.Sprintf(`
			SELECT *
			FROM %s
			WHERE server_path = $1
			  AND state = 'new_job'
			ORDER BY priority DESC, request_timestamp ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		`, rpc.BaseTable)

		rows, err := tx.Query(selectQuery, serverPath)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		defer rows.Close()

		results, err := rowsToMaps(rows)
		if err != nil {
			tx.Rollback()
			return nil, err
		}

		if len(results) == 0 {
			tx.Rollback()
			return nil, nil
		}

		record := results[0]
		recordID := record["id"]

		// Update the record status
		updateQuery := fmt.Sprintf(`
			UPDATE %s
			SET state = 'processing',
				processing_timestamp = NOW() AT TIME ZONE 'UTC'
			WHERE id = $1
			RETURNING id
		`, rpc.BaseTable)

		var updatedID int
		err = tx.QueryRow(updateQuery, recordID).Scan(&updatedID)
		if err != nil {
			tx.Rollback()
			if isSerializationError(err) && attempt < retries-1 {
				attempt++
				time.Sleep(waitTime * time.Duration(1<<uint(attempt)))
				continue
			}
			return nil, fmt.Errorf("failed to update state to 'processing': %v", err)
		}

		if err := tx.Commit(); err != nil {
			if isSerializationError(err) && attempt < retries-1 {
				attempt++
				time.Sleep(waitTime * time.Duration(1<<uint(attempt)))
				continue
			}
			return nil, err
		}

		return record, nil
	}

	return nil, fmt.Errorf("failed to peak server queue after %d attempts", retries)
}

// MarkJobCompletion marks a job as completed in the server queue
func (rpc *KBRPCServer) MarkJobCompletion(serverPath string, id int, retries int, waitTime time.Duration) (bool, error) {
	if retries <= 0 {
		retries = 5
	}
	if waitTime <= 0 {
		waitTime = time.Second
	}

	attempt := 0
	for attempt < retries {
		tx, err := rpc.conn.Begin()
		if err != nil {
			return false, fmt.Errorf("failed to begin transaction: %v", err)
		}

		_, err = tx.Exec("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
		if err != nil {
			tx.Rollback()
			return false, err
		}

		// Verify the record exists and is in processing state
		verifyQuery := fmt.Sprintf(`
			SELECT id FROM %s
			WHERE id = $1
			  AND server_path = $2
			  AND state = 'processing'
			FOR UPDATE
		`, rpc.BaseTable)

		var recordID int
		err = tx.QueryRow(verifyQuery, id, serverPath).Scan(&recordID)
		if err != nil {
			tx.Rollback()
			if err == sql.ErrNoRows {
				return false, nil
			}
			return false, err
		}

		// Update the record
		updateQuery := fmt.Sprintf(`
			UPDATE %s
			SET state = 'empty',
				completed_timestamp = NOW() AT TIME ZONE 'UTC'
			WHERE id = $1
			RETURNING id
		`, rpc.BaseTable)

		var updatedID int
		err = tx.QueryRow(updateQuery, id).Scan(&updatedID)
		if err != nil {
			tx.Rollback()
			if isSerializationError(err) && attempt < retries-1 {
				attempt++
				time.Sleep(waitTime * time.Duration(1<<uint(attempt)))
				continue
			}
			return false, err
		}

		if err := tx.Commit(); err != nil {
			if isSerializationError(err) && attempt < retries-1 {
				attempt++
				time.Sleep(waitTime * time.Duration(1<<uint(attempt)))
				continue
			}
			return false, err
		}

		return true, nil
	}

	return false, fmt.Errorf("failed to mark job as completed after %d attempts", retries)
}

// ClearServerQueue clears the reply queue by resetting records matching the specified server path
func (rpc *KBRPCServer) ClearServerQueue(serverPath string, maxRetries int, retryDelay time.Duration) (int, error) {
	if maxRetries <= 0 {
		maxRetries = 3
	}
	if retryDelay <= 0 {
		retryDelay = time.Second
	}

	retryCount := 0
	for retryCount < maxRetries {
		tx, err := rpc.conn.Begin()
		if err != nil {
			return 0, fmt.Errorf("failed to begin transaction: %v", err)
		}

		// Try to lock records
		lockQuery := fmt.Sprintf(`
			SELECT 1 FROM %s
			WHERE server_path = $1::ltree
			FOR UPDATE NOWAIT
		`, rpc.BaseTable)

		_, err = tx.Exec(lockQuery, serverPath)
		if err != nil {
			tx.Rollback()
			if isLockError(err) && retryCount < maxRetries-1 {
				retryCount++
				time.Sleep(retryDelay)
				continue
			}
			return 0, fmt.Errorf("failed to acquire lock: %v", err)
		}

		// Update records
		updateQuery := fmt.Sprintf(`
			UPDATE %s
			SET request_id = gen_random_uuid(),
				request_payload = '{}',
				completed_timestamp = CURRENT_TIMESTAMP AT TIME ZONE 'UTC',
				state = 'empty',
				rpc_client_queue = NULL
			WHERE server_path = $1::ltree
		`, rpc.BaseTable)

		result, err := tx.Exec(updateQuery, serverPath)
		if err != nil {
			tx.Rollback()
			return 0, fmt.Errorf("failed to clear reply queue: %v", err)
		}

		rowCount, err := result.RowsAffected()
		if err != nil {
			tx.Rollback()
			return 0, err
		}

		if err := tx.Commit(); err != nil {
			return 0, err
		}

		return int(rowCount), nil
	}

	return 0, fmt.Errorf("failed to acquire lock after %d attempts for server path: %s", maxRetries, serverPath)
}

// Helper methods

// isValidLTree validates if a string is a valid ltree path
func (rpc *KBRPCServer) isValidLTree(path string) bool {
	if path == "" {
		return false
	}

	parts := strings.Split(path, ".")
	if len(parts) == 0 {
		return false
	}

	// Basic ltree validation regex
	labelRegex := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

	for _, part := range parts {
		if !labelRegex.MatchString(part) {
			return false
		}
	}

	return true
}

// isSerializationError checks if error is a serialization failure
func isSerializationError(err error) bool {
	if err == nil {
		return false
	}
	pqErr, ok := err.(*pq.Error)
	if !ok {
		return false
	}
	return pqErr.Code == "40001" || pqErr.Code == "40P01" // serialization_failure or deadlock_detected
}

// minDuration returns the minimum of two durations
func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

