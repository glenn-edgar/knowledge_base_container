package data_structures_module


import (
	"database/sql"
	"encoding/json"
	"fmt"
	//"strings"
	"time"

	"github.com/google/uuid"
	//"github.com/lib/pq"
)

// KBRPCClient handles RPC client operations for the knowledge base
type KBRPCClient struct {
	KBSearch  *KBSearch
	conn      *sql.DB
	BaseTable string
}

// ReplyData represents a reply data record
type ReplyData struct {
	ID                int                    `json:"id"`
	RequestID         string                 `json:"request_id"`
	ClientPath        string                 `json:"client_path"`
	ServerPath        string                 `json:"server_path"`
	RPCAction         string                 `json:"rpc_action,omitempty"`
	TransactionTag    string                 `json:"transaction_tag,omitempty"`
	ResponsePayload   map[string]interface{} `json:"response_payload"`
	ResponseTimestamp time.Time              `json:"response_timestamp"`
	IsNewResult       bool                   `json:"is_new_result"`
}

// SlotCounts represents free and queued slot counts
type SlotCounts struct {
	TotalRecords int `json:"total_records"`
	FreeSlots    int `json:"free_slots"`
	QueuedSlots  int `json:"queued_slots"`
}

// NewKBRPCClient creates a new KBRPCClient instance
func NewKBRPCClient(kbSearch *KBSearch, database string) *KBRPCClient {
	return &KBRPCClient{
		KBSearch:  kbSearch,
		conn:      kbSearch.conn,
		BaseTable: fmt.Sprintf("%s_rpc_client", database),
	}
}

// FindRPCClientID finds a single RPC client id for given parameters
func (client *KBRPCClient) FindRPCClientID(kb *string, nodeName *string, properties map[string]interface{}, nodePath *string) (map[string]interface{}, error) {
	results, err := client.FindRPCClientIDs(kb, nodeName, properties, nodePath)
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

// FindRPCClientIDs finds all RPC client ids matching the given parameters
func (client *KBRPCClient) FindRPCClientIDs(kb *string, nodeName *string, properties map[string]interface{}, nodePath *string) ([]map[string]interface{}, error) {
	client.KBSearch.ClearFilters()
	client.KBSearch.SearchLabel("KB_RPC_CLIENT_FIELD")

	if kb != nil {
		client.KBSearch.SearchKB(*kb)
	}
	if nodeName != nil {
		client.KBSearch.SearchName(*nodeName)
	}
	if properties != nil {
		for key, value := range properties {
			client.KBSearch.SearchPropertyValue(key, value)
		}
	}
	if nodePath != nil {
		client.KBSearch.SearchPath(*nodePath)
	}

	nodeIDs, err := client.KBSearch.ExecuteQuery()
	if err != nil {
		return nil, err
	}

	if len(nodeIDs) == 0 {
		return nil, fmt.Errorf("no node found matching path parameters: %v, %v, %v", nodeName, properties, nodePath)
	}

	return nodeIDs, nil
}

// FindRPCClientKeys extracts path values from RPC client query results
func (client *KBRPCClient) FindRPCClientKeys(keyData []map[string]interface{}) []string {
	returnValues := []string{}
	for _, key := range keyData {
		if path, ok := key["path"].(string); ok {
			returnValues = append(returnValues, path)
		}
	}
	return returnValues
}

// FindFreeSlots finds the number of free slots for a given client path
func (client *KBRPCClient) FindFreeSlots(clientPath string) (int, error) {
	query := fmt.Sprintf(`
		SELECT 
			COUNT(*) as total_records,
			COUNT(*) FILTER (WHERE is_new_result = FALSE) as free_slots
		FROM %s 
		WHERE client_path = $1
	`, client.BaseTable)

	var totalRecords, freeSlots int
	err := client.conn.QueryRow(query, clientPath).Scan(&totalRecords, &freeSlots)
	if err != nil {
		return 0, fmt.Errorf("database error when finding free slots: %v", err)
	}

	if totalRecords == 0 {
		return 0, fmt.Errorf("no records found for client_path: %s", clientPath)
	}

	return freeSlots, nil
}

// FindQueuedSlots finds the number of queued slots for a given client path
func (client *KBRPCClient) FindQueuedSlots(clientPath string) (int, error) {
	query := fmt.Sprintf(`
		SELECT 
			COUNT(*) as total_records,
			COUNT(*) FILTER (WHERE is_new_result = TRUE) as queued_slots
		FROM %s 
		WHERE client_path = $1
	`, client.BaseTable)

	var totalRecords, queuedSlots int
	err := client.conn.QueryRow(query, clientPath).Scan(&totalRecords, &queuedSlots)
	if err != nil {
		return 0, fmt.Errorf("database error when finding queued slots: %v", err)
	}

	if totalRecords == 0 {
		return 0, fmt.Errorf("no records found for client_path: %s", clientPath)
	}

	return queuedSlots, nil
}

// PeakAndClaimReplyData atomically fetches and marks the next available reply as processed
func (client *KBRPCClient) PeakAndClaimReplyData(clientPath string, maxRetries int, retryDelay time.Duration) (*ReplyData, error) {
	if maxRetries <= 0 {
		maxRetries = 3
	}
	if retryDelay <= 0 {
		retryDelay = time.Second
	}

	attempt := 0
	for attempt < maxRetries {
		tx, err := client.conn.Begin()
		if err != nil {
			return nil, fmt.Errorf("failed to begin transaction: %v", err)
		}

		updateQuery := fmt.Sprintf(`
			UPDATE %s
			SET is_new_result = FALSE
			WHERE id = (
				SELECT id
				FROM %s
				WHERE client_path = $1
				AND is_new_result = TRUE
				ORDER BY response_timestamp ASC
				FOR UPDATE SKIP LOCKED
				LIMIT 1
			)
			RETURNING *
		`, client.BaseTable, client.BaseTable)

		rows, err := tx.Query(updateQuery, clientPath)
		if err != nil {
			tx.Rollback()
			if isLockError(err) && attempt < maxRetries-1 {
				attempt++
				time.Sleep(retryDelay)
				continue
			}
			return nil, err
		}
		defer rows.Close()

		if !rows.Next() {
			// Check if any matching unclaimed rows exist
			checkQuery := fmt.Sprintf(`
				SELECT EXISTS (
					SELECT 1 FROM %s
					WHERE client_path = $1 AND is_new_result = TRUE
				)
			`, client.BaseTable)

			var exists bool
			err = tx.QueryRow(checkQuery, clientPath).Scan(&exists)
			if err != nil {
				tx.Rollback()
				return nil, err
			}

			tx.Rollback()
			if !exists {
				return nil, nil
			}

			attempt++
			time.Sleep(retryDelay)
			continue
		}

		// Scan the row
		columns, err := rows.Columns()
		if err != nil {
			tx.Rollback()
			return nil, err
		}

		values := make([]interface{}, len(columns))
		valuePointers := make([]interface{}, len(columns))
		for i := range columns {
			valuePointers[i] = &values[i]
		}

		if err := rows.Scan(valuePointers...); err != nil {
			tx.Rollback()
			return nil, err
		}

		// Build result map
		result := make(map[string]interface{})
		for i, col := range columns {
			result[col] = values[i]
		}

		if err := tx.Commit(); err != nil {
			return nil, err
		}

		return mapToReplyData(result), nil
	}

	return nil, fmt.Errorf("could not lock a new-reply row after %d attempts", maxRetries)
}

// ClearReplyQueue clears the reply queue by resetting records matching the specified client path
func (client *KBRPCClient) ClearReplyQueue(clientPath string, maxRetries int, retryDelay time.Duration) (int, error) {
	if maxRetries <= 0 {
		maxRetries = 3
	}
	if retryDelay <= 0 {
		retryDelay = time.Second
	}

	attempt := 0
	for attempt < maxRetries {
		tx, err := client.conn.Begin()
		if err != nil {
			return 0, fmt.Errorf("failed to begin transaction: %v", err)
		}

		// Select and lock records
		selectQuery := fmt.Sprintf(`
			SELECT id
			FROM %s
			WHERE client_path = $1
			FOR UPDATE NOWAIT
		`, client.BaseTable)

		rows, err := tx.Query(selectQuery, clientPath)
		if err != nil {
			tx.Rollback()
			if isLockError(err) && attempt < maxRetries-1 {
				attempt++
				time.Sleep(retryDelay)
				continue
			}
			return 0, err
		}

		var ids []int
		for rows.Next() {
			var id int
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				tx.Rollback()
				return 0, err
			}
			ids = append(ids, id)
		}
		rows.Close()

		if len(ids) == 0 {
			tx.Commit()
			return 0, nil
		}

		// Update each record
		updated := 0
		updateQuery := fmt.Sprintf(`
			UPDATE %s
			SET
				request_id         = $1,
				server_path        = $2,
				response_payload   = $3,
				response_timestamp = NOW(),
				is_new_result      = FALSE
			WHERE id = $4
		`, client.BaseTable)

		for _, id := range ids {
			newUUID := uuid.New().String()
			emptyJSON, _ := json.Marshal(map[string]interface{}{})
			
			result, err := tx.Exec(updateQuery, newUUID, clientPath, string(emptyJSON), id)
			if err != nil {
				tx.Rollback()
				return 0, err
			}
			
			rowsAffected, _ := result.RowsAffected()
			updated += int(rowsAffected)
		}

		if err := tx.Commit(); err != nil {
			return 0, err
		}

		return updated, nil
	}

	return 0, fmt.Errorf("could not acquire lock after %d retries", maxRetries)
}

// PushAndClaimReplyData atomically claims and updates the earliest matching record
func (client *KBRPCClient) PushAndClaimReplyData(clientPath, requestUUID, serverPath, rpcAction, 
	transactionTag string, replyData map[string]interface{}, maxRetries int, retryDelay time.Duration) error {
	
	if maxRetries <= 0 {
		maxRetries = 3
	}
	if retryDelay <= 0 {
		retryDelay = time.Second
	}

	replyJSON, err := json.Marshal(replyData)
	if err != nil {
		return fmt.Errorf("failed to marshal reply data: %v", err)
	}

	var lastError error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		tx, err := client.conn.Begin()
		if err != nil {
			lastError = err
			continue
		}

		query := fmt.Sprintf(`
			WITH candidate AS (
				SELECT id
				FROM %s
				WHERE client_path = $1
				AND is_new_result = FALSE
				ORDER BY response_timestamp ASC
				FOR UPDATE SKIP LOCKED
				LIMIT 1
			)
			UPDATE %s
			SET request_id        = $2,
				server_path       = $3,
				rpc_action        = $4,
				transaction_tag   = $5,
				response_payload  = $6,
				is_new_result     = TRUE,
				response_timestamp = CURRENT_TIMESTAMP
			FROM candidate
			WHERE %s.id = candidate.id
			RETURNING %s.id
		`, client.BaseTable, client.BaseTable, client.BaseTable, client.BaseTable)

		var id int
		err = tx.QueryRow(query, clientPath, requestUUID, serverPath, rpcAction, 
			transactionTag, string(replyJSON)).Scan(&id)
		
		if err != nil {
			tx.Rollback()
			if err == sql.ErrNoRows {
				lastError = fmt.Errorf("no available record with is_new_result=FALSE found")
			} else {
				lastError = err
			}
			
			if attempt < maxRetries {
				time.Sleep(retryDelay)
				continue
			}
			break
		}

		if err := tx.Commit(); err != nil {
			lastError = err
			if attempt < maxRetries {
				time.Sleep(retryDelay)
				continue
			}
			break
		}

		return nil
	}

	return fmt.Errorf("failed after %d retries: %v", maxRetries, lastError)
}

// ListWaitingJobs lists all rows where is_new_result is TRUE
func (client *KBRPCClient) ListWaitingJobs(clientPath *string) ([]ReplyData, error) {
	var query string
	var args []interface{}

	if clientPath == nil {
		query = fmt.Sprintf(`
			SELECT id, request_id, client_path, server_path, rpc_action, transaction_tag,
				response_payload, response_timestamp, is_new_result
			FROM %s
			WHERE is_new_result = TRUE
			ORDER BY response_timestamp ASC
		`, client.BaseTable)
	} else {
		query = fmt.Sprintf(`
			SELECT id, request_id, client_path, server_path, rpc_action, transaction_tag,
				response_payload, response_timestamp, is_new_result
			FROM %s
			WHERE is_new_result = TRUE AND client_path = $1
			ORDER BY response_timestamp ASC
		`, client.BaseTable)
		args = append(args, *clientPath)
	}

	rows, err := client.conn.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("database error when listing waiting jobs: %v", err)
	}
	defer rows.Close()

	var results []ReplyData
	for rows.Next() {
		var rd ReplyData
		var requestID sql.NullString
		var rpcAction sql.NullString
		var transactionTag sql.NullString
		var payloadStr string

		err := rows.Scan(&rd.ID, &requestID, &rd.ClientPath, &rd.ServerPath, 
			&rpcAction, &transactionTag, &payloadStr, &rd.ResponseTimestamp, &rd.IsNewResult)
		if err != nil {
			return nil, err
		}

		if requestID.Valid {
			rd.RequestID = requestID.String
		}
		if rpcAction.Valid {
			rd.RPCAction = rpcAction.String
		}
		if transactionTag.Valid {
			rd.TransactionTag = transactionTag.String
		}

		// Parse JSON payload
		if err := json.Unmarshal([]byte(payloadStr), &rd.ResponsePayload); err != nil {
			// If JSON parsing fails, store as raw string
			rd.ResponsePayload = map[string]interface{}{"raw": payloadStr}
		}

		results = append(results, rd)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// Helper functions

// mapToReplyData converts a map to ReplyData struct
func mapToReplyData(m map[string]interface{}) *ReplyData {
	rd := &ReplyData{}

	if id, ok := m["id"].(int64); ok {
		rd.ID = int(id)
	}
	if requestID, ok := m["request_id"].(string); ok {
		rd.RequestID = requestID
	}
	if clientPath, ok := m["client_path"].(string); ok {
		rd.ClientPath = clientPath
	}
	if serverPath, ok := m["server_path"].(string); ok {
		rd.ServerPath = serverPath
	}
	if rpcAction, ok := m["rpc_action"].(string); ok {
		rd.RPCAction = rpcAction
	}
	if transactionTag, ok := m["transaction_tag"].(string); ok {
		rd.TransactionTag = transactionTag
	}
	if timestamp, ok := m["response_timestamp"].(time.Time); ok {
		rd.ResponseTimestamp = timestamp
	}
	if isNew, ok := m["is_new_result"].(bool); ok {
		rd.IsNewResult = isNew
	}

	// Handle response payload
	if payloadStr, ok := m["response_payload"].(string); ok {
		var payload map[string]interface{}
		if err := json.Unmarshal([]byte(payloadStr), &payload); err == nil {
			rd.ResponsePayload = payload
		}
	}

	return rd
}

/*
// isLockError checks if an error is a lock-related error
func isLockError(err error) bool {
	if err == nil {
		return false
	}
	pqErr, ok := err.(*pq.Error)
	if !ok {
		return false
	}
	// 55P03 = lock_not_available
	return pqErr.Code == "55P03"
}
*/

