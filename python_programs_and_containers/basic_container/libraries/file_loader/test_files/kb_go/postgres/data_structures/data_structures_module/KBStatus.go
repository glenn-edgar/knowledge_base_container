package data_structures_module

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
	"strings"
)

// KBStatusData handles the status data for the knowledge base
type KBStatusData struct {
	KBSearch  *KBSearch
	BaseTable string
}

// StatusDataResult represents the result of status data operations
type StatusDataResult struct {
	Success bool
	Message string
	Data    interface{}
}

// MultipleStatusResult represents results for multiple status operations
type MultipleStatusResult struct {
	Success bool
	Message string
	Results map[string]string
}

// NewKBStatusData creates a new KBStatusData instance
func NewKBStatusData(kbSearch *KBSearch, database string) *KBStatusData {
	return &KBStatusData{
		KBSearch:  kbSearch,
		BaseTable: fmt.Sprintf("%s_status", database),
	}
}

// FindNodeID finds a single node id for given parameters
func (ksd *KBStatusData) FindNodeID(kb, nodeName *string, properties map[string]interface{}, nodePath *string) (map[string]interface{}, error) {
	results, err := ksd.FindNodeIDs(kb, nodeName, properties, nodePath)
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("no node found matching parameters: kb=%v, name=%v, properties=%v, path=%v",
			kb, nodeName, properties, nodePath)
	}
	if len(results) > 1 {
		return nil, fmt.Errorf("multiple nodes (%d) found matching parameters: kb=%v, name=%v, properties=%v, path=%v",
			len(results), kb, nodeName, properties, nodePath)
	}

	return results[0], nil
}

// FindNodeIDs finds all node ids matching the given parameters
func (ksd *KBStatusData) FindNodeIDs(kb, nodeName *string, properties map[string]interface{}, nodePath *string) ([]map[string]interface{}, error) {
	// Clear previous filters and build new query
	ksd.KBSearch.ClearFilters()
	ksd.KBSearch.SearchLabel("KB_STATUS_FIELD")

	if kb != nil {
		ksd.KBSearch.SearchKB(*kb)
	}

	if nodeName != nil {
		ksd.KBSearch.SearchName(*nodeName)
	}

	if properties != nil {
		for key, value := range properties {
			ksd.KBSearch.SearchPropertyValue(key, value)
		}
	}

	if nodePath != nil {
		ksd.KBSearch.SearchPath(*nodePath)
	}

	// Execute query and get results
	nodeIDs, err := ksd.KBSearch.ExecuteQuery()
	if err != nil {
		return nil, fmt.Errorf("error finding node IDs: %v", err)
	}

	if len(nodeIDs) == 0 {
		return nil, fmt.Errorf("no nodes found matching parameters: kb=%v, name=%v, properties=%v, path=%v",
			kb, nodeName, properties, nodePath)
	}

	return nodeIDs, nil
}

// GetStatusData retrieves status data for a given path
func (ksd *KBStatusData) GetStatusData(path string) (map[string]interface{}, string, error) {
	if path == "" {
		return nil, "", fmt.Errorf("path cannot be empty")
	}

	query := fmt.Sprintf(`
		SELECT data, path
		FROM %s
		WHERE path = $1
		LIMIT 1
	`, ksd.BaseTable)

	row := ksd.KBSearch.conn.QueryRow(query, path)

	var dataStr string
	var pathValue string
	err := row.Scan(&dataStr, &pathValue)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, "", fmt.Errorf("no data found for path: %s", path)
		}
		return nil, "", fmt.Errorf("error retrieving status data for path '%s': %v", path, err)
	}

	// Parse JSON data
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(dataStr), &data); err != nil {
		return nil, "", fmt.Errorf("failed to decode JSON data for path '%s': %v", path, err)
	}

	return data, pathValue, nil
}

// GetMultipleStatusData retrieves status data for multiple paths in a single query
func (ksd *KBStatusData) GetMultipleStatusData(paths []string) (map[string]map[string]interface{}, error) {
	if len(paths) == 0 {
		return map[string]map[string]interface{}{}, nil
	}

	// Build query with placeholders
	placeholders := make([]string, len(paths))
	args := make([]interface{}, len(paths))
	for i, path := range paths {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = path
	}

	query := fmt.Sprintf(`
		SELECT data, path
		FROM %s
		WHERE path IN (%s)
	`, ksd.BaseTable, joinStrings(placeholders, ","))

	rows, err := ksd.KBSearch.conn.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("error retrieving multiple status data: %v", err)
	}
	defer rows.Close()

	dataDict := make(map[string]map[string]interface{})

	for rows.Next() {
		var dataStr string
		var pathValue string
		if err := rows.Scan(&dataStr, &pathValue); err != nil {
			continue
		}

		// Parse JSON data
		var data map[string]interface{}
		if err := json.Unmarshal([]byte(dataStr), &data); err != nil {
			// Log warning but continue
			fmt.Printf("Warning: Failed to decode JSON for path '%s'\n", pathValue)
			continue
		}

		dataDict[pathValue] = data
	}

	return dataDict, nil
}

// SetStatusData updates status data for a given path with retry logic
func (ksd *KBStatusData) SetStatusData(path string, data map[string]interface{}, retryCount int, retryDelay time.Duration) (bool, string, error) {
	// Input validation
	if path == "" {
		return false, "", fmt.Errorf("path cannot be empty")
	}
	if data == nil {
		return false, "", fmt.Errorf("data must be a valid map")
	}
	if retryCount < 0 {
		return false, "", fmt.Errorf("retry count must be non-negative")
	}
	if retryDelay < 0 {
		return false, "", fmt.Errorf("retry delay must be non-negative")
	}

	// Convert data to JSON once
	jsonData, err := json.Marshal(data)
	if err != nil {
		return false, "", fmt.Errorf("failed to marshal data to JSON: %v", err)
	}

	// Prepare the UPSERT query
	upsertQuery := fmt.Sprintf(`
		INSERT INTO %s (path, data)
		VALUES ($1, $2)
		ON CONFLICT (path)
		DO UPDATE SET data = EXCLUDED.data
		RETURNING path, (xmax = 0) AS was_inserted
	`, ksd.BaseTable)

	var lastError error
	attempt := 0

	for attempt <= retryCount {
		// Start transaction
		tx, err := ksd.KBSearch.conn.Begin()
		if err != nil {
			lastError = err
			if attempt < retryCount {
				time.Sleep(retryDelay)
				attempt++
				continue
			}
			break
		}

		// Execute query
		var returnedPath string
		var wasInserted bool
		err = tx.QueryRow(upsertQuery, path, string(jsonData)).Scan(&returnedPath, &wasInserted)
		
		if err != nil {
			tx.Rollback()
			lastError = err
			
			// Check if it's a transient error
			if isTransientError(err) && attempt < retryCount {
				time.Sleep(retryDelay)
				attempt++
				continue
			}
			
			return false, "", fmt.Errorf("error setting status data for path '%s': %v", path, err)
		}

		// Commit transaction
		if err := tx.Commit(); err != nil {
			lastError = err
			if attempt < retryCount {
				time.Sleep(retryDelay)
				attempt++
				continue
			}
			break
		}

		operation := "updated"
		if wasInserted {
			operation = "inserted"
		}
		return true, fmt.Sprintf("Successfully %s data for path: %s", operation, returnedPath), nil
	}

	// If we've exhausted all retries
	errorMsg := fmt.Sprintf("Failed to set status data for path '%s' after %d attempts", path, retryCount+1)
	if lastError != nil {
		errorMsg += fmt.Sprintf(": %v", lastError)
	}
	return false, "", fmt.Errorf(errorMsg)
}

// SetMultipleStatusData updates multiple path-data pairs in a single transaction
func (ksd *KBStatusData) SetMultipleStatusData(pathDataPairs map[string]map[string]interface{}, retryCount int, retryDelay time.Duration) (bool, string, map[string]string, error) {
	if len(pathDataPairs) == 0 {
		return false, "", nil, fmt.Errorf("pathDataPairs cannot be empty")
	}
	if retryCount < 0 {
		return false, "", nil, fmt.Errorf("retry count must be non-negative")
	}
	if retryDelay < 0 {
		return false, "", nil, fmt.Errorf("retry delay must be non-negative")
	}

	// Validate and prepare JSON data
	jsonPairs := make(map[string]string)
	for path, data := range pathDataPairs {
		if path == "" {
			return false, "", nil, fmt.Errorf("path cannot be empty")
		}
		if data == nil {
			return false, "", nil, fmt.Errorf("data for path '%s' must be a valid map", path)
		}

		jsonData, err := json.Marshal(data)
		if err != nil {
			return false, "", nil, fmt.Errorf("failed to marshal data for path '%s': %v", path, err)
		}
		jsonPairs[path] = string(jsonData)
	}

	// Prepare the UPSERT query
	upsertQuery := fmt.Sprintf(`
		INSERT INTO %s (path, data)
		VALUES ($1, $2)
		ON CONFLICT (path)
		DO UPDATE SET data = EXCLUDED.data
		RETURNING path, (xmax = 0) AS was_inserted
	`, ksd.BaseTable)

	var lastError error
	attempt := 0

	for attempt <= retryCount {
		results := make(map[string]string)
		
		// Start transaction
		tx, err := ksd.KBSearch.conn.Begin()
		if err != nil {
			lastError = err
			if attempt < retryCount {
				time.Sleep(retryDelay)
				attempt++
				continue
			}
			break
		}

		// Execute all updates
		allSuccess := true
		for path, jsonData := range jsonPairs {
			var returnedPath string
			var wasInserted bool
			err := tx.QueryRow(upsertQuery, path, jsonData).Scan(&returnedPath, &wasInserted)
			
			if err != nil {
				results[path] = "failed"
				allSuccess = false
			} else {
				if wasInserted {
					results[returnedPath] = "inserted"
				} else {
					results[returnedPath] = "updated"
				}
			}
		}

		// If any operation failed, rollback
		if !allSuccess {
			tx.Rollback()
			lastError = fmt.Errorf("some operations failed")
			if attempt < retryCount {
				time.Sleep(retryDelay)
				attempt++
				continue
			}
			break
		}

		// Commit transaction
		if err := tx.Commit(); err != nil {
			lastError = err
			if attempt < retryCount {
				time.Sleep(retryDelay)
				attempt++
				continue
			}
			break
		}

		successCount := 0
		for _, status := range results {
			if status != "failed" {
				successCount++
			}
		}

		return true, fmt.Sprintf("Successfully processed %d/%d records", successCount, len(pathDataPairs)), results, nil
	}

	// If we've exhausted all retries
	errorMsg := fmt.Sprintf("Failed to set multiple status data after %d attempts", retryCount+1)
	if lastError != nil {
		errorMsg += fmt.Sprintf(": %v", lastError)
	}
	return false, "", nil, fmt.Errorf(errorMsg)
}

// SetMultipleStatusDataList is an alternative method that accepts a list of path-data pairs
func (ksd *KBStatusData) SetMultipleStatusDataList(pathDataPairs []struct {
	Path string
	Data map[string]interface{}
}, retryCount int, retryDelay time.Duration) (bool, string, map[string]string, error) {
	// Convert list to map
	pairsMap := make(map[string]map[string]interface{})
	for _, pair := range pathDataPairs {
		pairsMap[pair.Path] = pair.Data
	}
	
	return ksd.SetMultipleStatusData(pairsMap, retryCount, retryDelay)
}

// Helper functions

// joinStrings joins strings with a separator
func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	result := strs[0]
	for i := 1; i < len(strs); i++ {
		result += sep + strs[i]
	}
	return result
}

// isTransientError checks if an error is likely transient and worth retrying
func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	
	// Check for common transient error patterns
	errStr := err.Error()
	transientPatterns := []string{
		"connection",
		"timeout",
		"temporary",
		"EOF",
		"broken pipe",
		"reset by peer",
	}
	
	for _, pattern := range transientPatterns {
		if containsIgnoreCase(errStr, pattern) {
			return true
		}
	}
	
	return false
}

// containsIgnoreCase checks if a string contains a substring (case-insensitive)
func containsIgnoreCase(s, substr string) bool {
	s = strings.ToLower(s)
	substr = strings.ToLower(substr)
	return strings.Contains(s, substr)
}

