package data_structures_module

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// KBStream handles stream data for the knowledge base
type KBStream struct {
	KBSearch  *KBSearch
	conn      *sql.DB
	BaseTable string
}

// StreamRecord represents a single stream record
type StreamRecord struct {
	ID         int                    `json:"id"`
	Path       string                 `json:"path"`
	RecordedAt time.Time              `json:"recorded_at"`
	Data       map[string]interface{} `json:"data"`
	Valid      bool                   `json:"valid"`
}

// StreamPushResult represents the result of pushing stream data
type StreamPushResult struct {
	ID                  int                    `json:"id"`
	Path                string                 `json:"path"`
	RecordedAt          time.Time              `json:"recorded_at"`
	Data                map[string]interface{} `json:"data"`
	Valid               bool                   `json:"valid"`
	PreviousRecordedAt  time.Time              `json:"previous_recorded_at"`
	WasPreviouslyValid  bool                   `json:"was_previously_valid"`
	Operation           string                 `json:"operation"`
}

// ClearResult represents the result of clearing stream data
type ClearResult struct {
	Success        bool                     `json:"success"`
	ClearedCount   int                      `json:"cleared_count"`
	ClearedRecords []map[string]interface{} `json:"cleared_records,omitempty"`
	Path           string                   `json:"path"`
	Operation      string                   `json:"operation,omitempty"`
	Error          string                   `json:"error,omitempty"`
}

// StreamStatistics represents statistics for stream data
type StreamStatistics struct {
	TotalRecords            int           `json:"total_records,omitempty"`
	ValidRecords            int           `json:"valid_records"`
	InvalidRecords          int           `json:"invalid_records,omitempty"`
	EarliestValidRecorded   *time.Time    `json:"earliest_valid_recorded,omitempty"`
	LatestValidRecorded     *time.Time    `json:"latest_valid_recorded,omitempty"`
	EarliestRecordedOverall *time.Time    `json:"earliest_recorded_overall,omitempty"`
	LatestRecordedOverall   *time.Time    `json:"latest_recorded_overall,omitempty"`
	AvgIntervalSecondsAll   *float64      `json:"avg_interval_seconds_all,omitempty"`
	AvgIntervalSecondsValid *float64      `json:"avg_interval_seconds_valid,omitempty"`
	EarliestRecorded        *time.Time    `json:"earliest_recorded,omitempty"`
	LatestRecorded          *time.Time    `json:"latest_recorded,omitempty"`
	AvgIntervalSeconds      *float64      `json:"avg_interval_seconds,omitempty"`
}

// NewKBStream creates a new KBStream instance
func NewKBStream(kbSearch *KBSearch, database string) *KBStream {
	return &KBStream{
		KBSearch:  kbSearch,
		conn:      kbSearch.conn,
		BaseTable: fmt.Sprintf("%s_stream", database),
	}
}

// executeQuery executes a query and returns results as slice of maps
func (ks *KBStream) executeQuery(query string, params ...interface{}) ([]map[string]interface{}, error) {
	rows, err := ks.conn.Query(query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return rowsToMaps(rows)
}

// executeSingle executes a query and returns a single result as a map
func (ks *KBStream) executeSingle(query string, params ...interface{}) (map[string]interface{}, error) {
	rows, err := ks.conn.Query(query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, nil
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]interface{}, len(columns))
	valuePointers := make([]interface{}, len(columns))
	for i := range columns {
		valuePointers[i] = &values[i]
	}

	if err := rows.Scan(valuePointers...); err != nil {
		return nil, err
	}

	result := make(map[string]interface{})
	for i, col := range columns {
		var v interface{}
		val := values[i]
		b, ok := val.([]byte)
		if ok {
			v = string(b)
		} else {
			v = val
		}
		result[col] = v
	}

	return result, nil
}

// FindStreamID finds a single stream node id for given parameters
func (ks *KBStream) FindStreamID(kb *string, nodeName *string, properties map[string]interface{}, nodePath *string) (map[string]interface{}, error) {
	results, err := ks.FindStreamIDs(kb, nodeName, properties, nodePath)
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("no stream node found matching parameters: name=%v, properties=%v, path=%v", 
			nodeName, properties, nodePath)
	}
	if len(results) > 1 {
		return nil, fmt.Errorf("multiple stream nodes (%d) found matching parameters: name=%v, properties=%v, path=%v", 
			len(results), nodeName, properties, nodePath)
	}

	return results[0], nil
}

// FindStreamIDs finds all stream node ids matching the given parameters
func (ks *KBStream) FindStreamIDs(kb *string, nodeName *string, properties map[string]interface{}, nodePath *string) ([]map[string]interface{}, error) {
	// Clear previous filters
	ks.KBSearch.ClearFilters()
	ks.KBSearch.SearchLabel("KB_STREAM_FIELD")

	if kb != nil {
		ks.KBSearch.SearchKB(*kb)
	}

	if nodeName != nil {
		ks.KBSearch.SearchName(*nodeName)
	}

	if properties != nil {
		for key, value := range properties {
			ks.KBSearch.SearchPropertyValue(key, value)
		}
	}

	if nodePath != nil {
		ks.KBSearch.SearchPath(*nodePath)
	}

	// Execute query
	nodeIDs, err := ks.KBSearch.ExecuteQuery()
	if err != nil {
		return nil, fmt.Errorf("error finding stream node IDs: %v", err)
	}

	if len(nodeIDs) == 0 {
		return nil, fmt.Errorf("no stream nodes found matching parameters: name=%v, properties=%v, path=%v", 
			nodeName, properties, nodePath)
	}

	return nodeIDs, nil
}

// FindStreamTableKeys extracts path values from stream query results
func (ks *KBStream) FindStreamTableKeys(keyData []map[string]interface{}) []string {
	if len(keyData) == 0 {
		return []string{}
	}

	returnValues := []string{}
	for _, row := range keyData {
		if path, ok := row["path"].(string); ok {
			returnValues = append(returnValues, path)
		}
	}

	return returnValues
}

// PushStreamData finds the oldest record for the given path and updates it with new data
func (ks *KBStream) PushStreamData(path string, data map[string]interface{}, maxRetries int, retryDelay time.Duration) (*StreamPushResult, error) {
	if path == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}
	if data == nil {
		return nil, fmt.Errorf("data must be a valid map")
	}

	if maxRetries <= 0 {
		maxRetries = 3
	}
	if retryDelay <= 0 {
		retryDelay = time.Second
	}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Check if records exist
		countQuery := fmt.Sprintf(`
			SELECT COUNT(*) as count
			FROM %s
			WHERE path = $1
		`, ks.BaseTable)

		countResult, err := ks.executeSingle(countQuery, path)
		if err != nil {
			return nil, err
		}

		count := 0
		if val, ok := countResult["count"].(int64); ok {
			count = int(val)
		}

		if count == 0 {
			return nil, fmt.Errorf("no records found for path='%s'. Records must be pre-allocated for stream tables", path)
		}

		// Try to lock the oldest record
		selectQuery := fmt.Sprintf(`
			SELECT id, recorded_at, valid
			FROM %s
			WHERE path = $1
			ORDER BY recorded_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		`, ks.BaseTable)

		row, err := ks.executeSingle(selectQuery, path)
		if err != nil {
			return nil, err
		}

		if row == nil {
			// All rows are locked
			if attempt < maxRetries {
				time.Sleep(retryDelay)
				continue
			}
			return nil, fmt.Errorf("could not lock any row for path='%s' after %d attempts", path, maxRetries)
		}

		recordID := row["id"].(int64)
		oldRecordedAt := row["recorded_at"].(time.Time)
		wasValid := row["valid"].(bool)

		// Update the record
		jsonData, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("error marshaling data: %v", err)
		}

		updateQuery := fmt.Sprintf(`
			UPDATE %s
			SET data = $1,
			    recorded_at = NOW(),
			    valid = TRUE
			WHERE id = $2
			RETURNING id, path, recorded_at, data, valid
		`, ks.BaseTable)

		updatedRow, err := ks.executeSingle(updateQuery, string(jsonData), recordID)
		if err != nil {
			return nil, err
		}

		if updatedRow == nil {
			return nil, fmt.Errorf("failed to update record id=%d", recordID)
		}

		// Parse the returned data
		var returnedData map[string]interface{}
		if dataStr, ok := updatedRow["data"].(string); ok {
			json.Unmarshal([]byte(dataStr), &returnedData)
		}

		result := &StreamPushResult{
			ID:                 int(updatedRow["id"].(int64)),
			Path:               updatedRow["path"].(string),
			RecordedAt:         updatedRow["recorded_at"].(time.Time),
			Data:               returnedData,
			Valid:              updatedRow["valid"].(bool),
			PreviousRecordedAt: oldRecordedAt,
			WasPreviouslyValid: wasValid,
			Operation:          "circular_buffer_replace",
		}

		return result, nil
	}

	return nil, fmt.Errorf("unexpected error in push_stream_data")
}

// GetLatestStreamData gets the most recent valid stream data for a given path
func (ks *KBStream) GetLatestStreamData(path string) (*StreamRecord, error) {
	if path == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}

	query := fmt.Sprintf(`
		SELECT id, path, recorded_at, data, valid
		FROM %s
		WHERE path = $1 AND valid = TRUE
		ORDER BY recorded_at DESC
		LIMIT 1
	`, ks.BaseTable)

	result, err := ks.executeSingle(query, path)
	if err != nil {
		return nil, fmt.Errorf("error getting latest stream data for path '%s': %v", path, err)
	}

	if result == nil {
		return nil, nil
	}

	return mapToStreamRecord(result), nil
}

// GetStreamDataCount counts the number of stream entries for a given path
func (ks *KBStream) GetStreamDataCount(path string, includeInvalid bool) (int, error) {
	if path == "" {
		return 0, fmt.Errorf("path cannot be empty")
	}

	var query string
	if includeInvalid {
		query = fmt.Sprintf(`
			SELECT COUNT(*) as count
			FROM %s
			WHERE path = $1
		`, ks.BaseTable)
	} else {
		query = fmt.Sprintf(`
			SELECT COUNT(*) as count
			FROM %s
			WHERE path = $1 AND valid = TRUE
		`, ks.BaseTable)
	}

	result, err := ks.executeSingle(query, path)
	if err != nil {
		return 0, fmt.Errorf("error counting stream data for path '%s': %v", path, err)
	}

	if result == nil {
		return 0, nil
	}

	if count, ok := result["count"].(int64); ok {
		return int(count), nil
	}

	return 0, nil
}

// ClearStreamData clears stream data for a given path by setting valid to FALSE
func (ks *KBStream) ClearStreamData(path string, olderThan *time.Time) *ClearResult {
	if path == "" {
		return &ClearResult{
			Success: false,
			Error:   "path cannot be empty",
			Path:    path,
		}
	}

	var updateQuery string
	var params []interface{}
	var operationDesc string

	if olderThan != nil {
		updateQuery = fmt.Sprintf(`
			UPDATE %s
			SET valid = FALSE
			WHERE path = $1
			AND recorded_at < $2
			AND valid = TRUE
			RETURNING id, recorded_at
		`, ks.BaseTable)
		params = []interface{}{path, *olderThan}
		operationDesc = fmt.Sprintf("older than %v", *olderThan)
	} else {
		updateQuery = fmt.Sprintf(`
			UPDATE %s
			SET valid = FALSE
			WHERE path = $1
			AND valid = TRUE
			RETURNING id, recorded_at
		`, ks.BaseTable)
		params = []interface{}{path}
		operationDesc = "all records"
	}

	clearedRecords, err := ks.executeQuery(updateQuery, params...)
	if err != nil {
		return &ClearResult{
			Success:      false,
			ClearedCount: 0,
			Error:        fmt.Sprintf("error clearing stream data for path '%s': %v", path, err),
			Path:         path,
		}
	}

	return &ClearResult{
		Success:        true,
		ClearedCount:   len(clearedRecords),
		ClearedRecords: clearedRecords,
		Path:           path,
		Operation:      fmt.Sprintf("Cleared %s", operationDesc),
	}
}

// ListStreamData lists valid stream data for a given path with filtering and pagination
func (ks *KBStream) ListStreamData(path string, limit *int, offset int, recordedAfter, recordedBefore *time.Time, order string) ([]StreamRecord, error) {
	if path == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}

	if order != "ASC" && order != "DESC" {
		return nil, fmt.Errorf("order must be 'ASC' or 'DESC'")
	}

	query := fmt.Sprintf(`
		SELECT id, path, recorded_at, data, valid
		FROM %s
		WHERE path = $1 AND valid = TRUE
	`, ks.BaseTable)

	params := []interface{}{path}
	paramCount := 1

	if recordedAfter != nil {
		paramCount++
		query += fmt.Sprintf(" AND recorded_at >= $%d", paramCount)
		params = append(params, *recordedAfter)
	}

	if recordedBefore != nil {
		paramCount++
		query += fmt.Sprintf(" AND recorded_at <= $%d", paramCount)
		params = append(params, *recordedBefore)
	}

	query += fmt.Sprintf(" ORDER BY recorded_at %s", order)

	if limit != nil && *limit > 0 {
		paramCount++
		query += fmt.Sprintf(" LIMIT $%d", paramCount)
		params = append(params, *limit)
	}

	if offset > 0 {
		paramCount++
		query += fmt.Sprintf(" OFFSET $%d", paramCount)
		params = append(params, offset)
	}

	rows, err := ks.executeQuery(query, params...)
	if err != nil {
		return nil, fmt.Errorf("error listing stream data for path '%s': %v", path, err)
	}

	results := []StreamRecord{}
	for _, row := range rows {
		results = append(results, *mapToStreamRecord(row))
	}

	return results, nil
}

// GetStreamDataRange gets valid stream data within a specific time range
func (ks *KBStream) GetStreamDataRange(path string, startTime, endTime time.Time) ([]StreamRecord, error) {
	if path == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}
	if startTime.IsZero() || endTime.IsZero() {
		return nil, fmt.Errorf("both start_time and end_time must be provided")
	}
	if !startTime.Before(endTime) {
		return nil, fmt.Errorf("start_time must be before end_time")
	}

	query := fmt.Sprintf(`
		SELECT id, path, recorded_at, data, valid
		FROM %s
		WHERE path = $1
		AND recorded_at >= $2
		AND recorded_at <= $3
		AND valid = TRUE
		ORDER BY recorded_at ASC
	`, ks.BaseTable)

	rows, err := ks.executeQuery(query, path, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("error getting stream data range for path '%s': %v", path, err)
	}

	results := []StreamRecord{}
	for _, row := range rows {
		results = append(results, *mapToStreamRecord(row))
	}

	return results, nil
}

// GetStreamStatistics gets comprehensive statistics for stream data at a given path
func (ks *KBStream) GetStreamStatistics(path string, includeInvalid bool) (*StreamStatistics, error) {
	if path == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}

	var query string
	if includeInvalid {
		query = fmt.Sprintf(`
			SELECT 
				COUNT(*) as total_records,
				COUNT(CASE WHEN valid = TRUE THEN 1 END) as valid_records,
				COUNT(CASE WHEN valid = FALSE THEN 1 END) as invalid_records,
				MIN(CASE WHEN valid = TRUE THEN recorded_at END) as earliest_valid_recorded,
				MAX(CASE WHEN valid = TRUE THEN recorded_at END) as latest_valid_recorded,
				MIN(recorded_at) as earliest_recorded_overall,
				MAX(recorded_at) as latest_recorded_overall,
				AVG(EXTRACT(EPOCH FROM (recorded_at - LAG(recorded_at) OVER (ORDER BY recorded_at)))) as avg_interval_seconds_all,
				AVG(CASE WHEN valid = TRUE THEN EXTRACT(EPOCH FROM (recorded_at - LAG(recorded_at) OVER (ORDER BY recorded_at))) END) as avg_interval_seconds_valid
			FROM %s
			WHERE path = $1
		`, ks.BaseTable)
	} else {
		query = fmt.Sprintf(`
			SELECT 
				COUNT(*) as valid_records,
				MIN(recorded_at) as earliest_recorded,
				MAX(recorded_at) as latest_recorded,
				AVG(EXTRACT(EPOCH FROM (recorded_at - LAG(recorded_at) OVER (ORDER BY recorded_at)))) as avg_interval_seconds
			FROM %s
			WHERE path = $1 AND valid = TRUE
		`, ks.BaseTable)
	}

	result, err := ks.executeSingle(query, path)
	if err != nil {
		return nil, fmt.Errorf("error getting stream statistics for path '%s': %v", path, err)
	}

	if result == nil {
		if includeInvalid {
			return &StreamStatistics{
				TotalRecords:   0,
				ValidRecords:   0,
				InvalidRecords: 0,
			}, nil
		}
		return &StreamStatistics{
			ValidRecords: 0,
		}, nil
	}

	return mapToStreamStatistics(result, includeInvalid), nil
}

// GetStreamDataByID retrieves a specific stream record by its ID
func (ks *KBStream) GetStreamDataByID(recordID int) (*StreamRecord, error) {
	if recordID <= 0 {
		return nil, fmt.Errorf("record_id must be a valid positive integer")
	}

	query := fmt.Sprintf(`
		SELECT id, path, recorded_at, data, valid
		FROM %s
		WHERE id = $1
	`, ks.BaseTable)

	result, err := ks.executeSingle(query, recordID)
	if err != nil {
		return nil, fmt.Errorf("error retrieving stream record with id %d: %v", recordID, err)
	}

	if result == nil {
		return nil, nil
	}

	return mapToStreamRecord(result), nil
}

// Helper functions

// rowsToMaps converts SQL rows to slice of maps (reused from KBSearch)
func rowsToMaps(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	results := []map[string]interface{}{}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePointers := make([]interface{}, len(columns))

		for i := range columns {
			valuePointers[i] = &values[i]
		}

		if err := rows.Scan(valuePointers...); err != nil {
			return nil, err
		}

		entry := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			entry[col] = v
		}

		results = append(results, entry)
	}

	return results, nil
}

// mapToStreamRecord converts a map to StreamRecord
func mapToStreamRecord(m map[string]interface{}) *StreamRecord {
	record := &StreamRecord{}

	if id, ok := m["id"].(int64); ok {
		record.ID = int(id)
	}
	if path, ok := m["path"].(string); ok {
		record.Path = path
	}
	if recordedAt, ok := m["recorded_at"].(time.Time); ok {
		record.RecordedAt = recordedAt
	}
	if valid, ok := m["valid"].(bool); ok {
		record.Valid = valid
	}

	// Handle data field
	if dataStr, ok := m["data"].(string); ok {
		var data map[string]interface{}
		if err := json.Unmarshal([]byte(dataStr), &data); err == nil {
			record.Data = data
		}
	}

	return record
}

// mapToStreamStatistics converts a map to StreamStatistics
func mapToStreamStatistics(m map[string]interface{}, includeInvalid bool) *StreamStatistics {
	stats := &StreamStatistics{}

	if includeInvalid {
		if total, ok := m["total_records"].(int64); ok {
			stats.TotalRecords = int(total)
		}
		if valid, ok := m["valid_records"].(int64); ok {
			stats.ValidRecords = int(valid)
		}
		if invalid, ok := m["invalid_records"].(int64); ok {
			stats.InvalidRecords = int(invalid)
		}
		if t, ok := m["earliest_valid_recorded"].(time.Time); ok {
			stats.EarliestValidRecorded = &t
		}
		if t, ok := m["latest_valid_recorded"].(time.Time); ok {
			stats.LatestValidRecorded = &t
		}
		if t, ok := m["earliest_recorded_overall"].(time.Time); ok {
			stats.EarliestRecordedOverall = &t
		}
		if t, ok := m["latest_recorded_overall"].(time.Time); ok {
			stats.LatestRecordedOverall = &t
		}
		if avg, ok := m["avg_interval_seconds_all"].(float64); ok {
			stats.AvgIntervalSecondsAll = &avg
		}
		if avg, ok := m["avg_interval_seconds_valid"].(float64); ok {
			stats.AvgIntervalSecondsValid = &avg
		}
	} else {
		if valid, ok := m["valid_records"].(int64); ok {
			stats.ValidRecords = int(valid)
		}
		if t, ok := m["earliest_recorded"].(time.Time); ok {
			stats.EarliestRecorded = &t
		}
		if t, ok := m["latest_recorded"].(time.Time); ok {
			stats.LatestRecorded = &t
		}
		if avg, ok := m["avg_interval_seconds"].(float64); ok {
			stats.AvgIntervalSeconds = &avg
		}
	}

	return stats
}

