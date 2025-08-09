package data_structures_module


import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// KBJobQueue handles job queue operations for the knowledge base
type KBJobQueue struct {
	KBSearch  *KBSearch
	conn      *sql.DB
	BaseTable string
}

// JobRecord represents a single job record
type JobRecord struct {
	ID          int                    `json:"id"`
	Path        string                 `json:"path"`
	ScheduleAt  *time.Time             `json:"schedule_at"`
	StartedAt   *time.Time             `json:"started_at"`
	CompletedAt *time.Time             `json:"completed_at"`
	IsActive    bool                   `json:"is_active"`
	Valid       bool                   `json:"valid"`
	Data        map[string]interface{} `json:"data"`
}

// PeakJobResult represents the result of peeking at a job
type PeakJobResult struct {
	ID         int                    `json:"id"`
	Data       map[string]interface{} `json:"data"`
	ScheduleAt *time.Time             `json:"schedule_at"`
	StartedAt  *time.Time             `json:"started_at"`
}

// JobCompletionResult represents the result of marking a job as completed
type JobCompletionResult struct {
	Success     bool       `json:"success"`
	JobID       int        `json:"job_id"`
	CompletedAt *time.Time `json:"completed_at"`
}

// PushJobResult represents the result of pushing a job
type PushJobResult struct {
	JobID      int                    `json:"job_id"`
	ScheduleAt *time.Time             `json:"schedule_at"`
	Data       map[string]interface{} `json:"data"`
}

// ClearQueueResult represents the result of clearing the job queue
type ClearQueueResult struct {
	Success      bool                     `json:"success"`
	ClearedCount int                      `json:"cleared_count"`
	ClearedJobs  []map[string]interface{} `json:"cleared_jobs"`
}

// JobStatistics represents job queue statistics
type JobStatistics struct {
	TotalJobs                int        `json:"total_jobs"`
	PendingJobs              int        `json:"pending_jobs"`
	ActiveJobs               int        `json:"active_jobs"`
	CompletedJobs            int        `json:"completed_jobs"`
	EarliestScheduled        *time.Time `json:"earliest_scheduled"`
	LatestCompleted          *time.Time `json:"latest_completed"`
	AvgProcessingTimeSeconds *float64   `json:"avg_processing_time_seconds"`
}

// NewKBJobQueue creates a new KBJobQueue instance
func NewKBJobQueue(kbSearch *KBSearch, database string) *KBJobQueue {
	return &KBJobQueue{
		KBSearch:  kbSearch,
		conn:      kbSearch.conn,
		BaseTable: fmt.Sprintf("%s_job", database),
	}
}

// executeQuery executes a query and returns results as slice of maps
func (jq *KBJobQueue) executeQuery(query string, params ...interface{}) ([]map[string]interface{}, error) {
	rows, err := jq.conn.Query(query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return rowsToMaps(rows)
}

// executeSingle executes a query and returns a single result as a map
func (jq *KBJobQueue) executeSingle(query string, params ...interface{}) (map[string]interface{}, error) {
	rows, err := jq.conn.Query(query, params...)
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

// FindJobID finds a single job id for given parameters
func (jq *KBJobQueue) FindJobID(kb *string, nodeName *string, properties map[string]interface{}, nodePath *string) (map[string]interface{}, error) {
	results, err := jq.FindJobIDs(kb, nodeName, properties, nodePath)
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("no job found matching parameters: name=%v, properties=%v, path=%v",
			nodeName, properties, nodePath)
	}
	if len(results) > 1 {
		return nil, fmt.Errorf("multiple jobs (%d) found matching parameters: name=%v, properties=%v, path=%v",
			len(results), nodeName, properties, nodePath)
	}

	return results[0], nil
}

// FindJobIDs finds all job ids matching the given parameters
func (jq *KBJobQueue) FindJobIDs(kb *string, nodeName *string, properties map[string]interface{}, nodePath *string) ([]map[string]interface{}, error) {
	// Clear previous filters and build new query
	jq.KBSearch.ClearFilters()
	jq.KBSearch.SearchLabel("KB_JOB_QUEUE")

	if kb != nil {
		jq.KBSearch.SearchKB(*kb)
	}

	if nodeName != nil {
		jq.KBSearch.SearchName(*nodeName)
	}

	if properties != nil {
		for key, value := range properties {
			jq.KBSearch.SearchPropertyValue(key, value)
		}
	}

	if nodePath != nil {
		jq.KBSearch.SearchPath(*nodePath)
	}

	// Execute query
	nodeIDs, err := jq.KBSearch.ExecuteQuery()
	if err != nil {
		return nil, fmt.Errorf("error finding job IDs: %v", err)
	}

	if len(nodeIDs) == 0 {
		return nil, fmt.Errorf("no jobs found matching parameters: name=%v, properties=%v, path=%v",
			nodeName, properties, nodePath)
	}

	return nodeIDs, nil
}

// FindJobPaths extracts path values from job query results
func (jq *KBJobQueue) FindJobPaths(tableDictRows []map[string]interface{}) []string {
	if len(tableDictRows) == 0 {
		return []string{}
	}

	returnValues := []string{}
	for _, row := range tableDictRows {
		if path, ok := row["path"].(string); ok {
			returnValues = append(returnValues, path)
		}
	}

	return returnValues
}

// GetQueuedNumber counts the number of valid job entries for a given path
func (jq *KBJobQueue) GetQueuedNumber(path string) (int, error) {
	if path == "" {
		return 0, fmt.Errorf("path cannot be empty")
	}

	query := fmt.Sprintf(`
		SELECT COUNT(*) as count
		FROM %s
		WHERE path = $1
		AND valid = TRUE
	`, jq.BaseTable)

	result, err := jq.executeSingle(query, path)
	if err != nil {
		return 0, fmt.Errorf("error counting queued jobs for path '%s': %v", path, err)
	}

	if result == nil {
		return 0, nil
	}

	if count, ok := result["count"].(int64); ok {
		return int(count), nil
	}

	return 0, nil
}

// GetFreeNumber counts the number of invalid job entries for a given path
func (jq *KBJobQueue) GetFreeNumber(path string) (int, error) {
	if path == "" {
		return 0, fmt.Errorf("path cannot be empty")
	}

	query := fmt.Sprintf(`
		SELECT COUNT(*) as count
		FROM %s
		WHERE path = $1
		AND valid = FALSE
	`, jq.BaseTable)

	result, err := jq.executeSingle(query, path)
	if err != nil {
		return 0, fmt.Errorf("error counting free jobs for path '%s': %v", path, err)
	}

	if result == nil {
		return 0, nil
	}

	if count, ok := result["count"].(int64); ok {
		return int(count), nil
	}

	return 0, nil
}

// PeakJobData finds and claims the earliest scheduled job for a path
func (jq *KBJobQueue) PeakJobData(path string, maxRetries int, retryDelay time.Duration) (*PeakJobResult, error) {
	if path == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}

	if maxRetries <= 0 {
		maxRetries = 3
	}
	if retryDelay <= 0 {
		retryDelay = time.Second
	}

	for attempt := 0; attempt < maxRetries; attempt++ {
		// Start transaction
		tx, err := jq.conn.Begin()
		if err != nil {
			if attempt < maxRetries-1 {
				time.Sleep(retryDelay)
				continue
			}
			return nil, fmt.Errorf("database error peeking job data for path '%s': %v", path, err)
		}

		// Find query
		findQuery := fmt.Sprintf(`
			SELECT id, data, schedule_at
			FROM %s
			WHERE path = $1
				AND valid = TRUE
				AND is_active = FALSE
				AND (schedule_at IS NULL OR schedule_at <= NOW())
			ORDER BY schedule_at ASC NULLS FIRST
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		`, jq.BaseTable)

		var jobID int64
		var dataStr string
		var scheduleAt sql.NullTime

		err = tx.QueryRow(findQuery, path).Scan(&jobID, &dataStr, &scheduleAt)
		if err != nil {
			tx.Rollback()
			if err == sql.ErrNoRows {
				return nil, nil
			}
			if attempt < maxRetries-1 {
				time.Sleep(retryDelay)
				continue
			}
			return nil, err
		}

		// Update query
		updateQuery := fmt.Sprintf(`
			UPDATE %s
			SET started_at = NOW(),
				is_active = TRUE
			WHERE id = $1
				AND is_active = FALSE
				AND valid = TRUE
			RETURNING started_at
		`, jq.BaseTable)

		var startedAt time.Time
		err = tx.QueryRow(updateQuery, jobID).Scan(&startedAt)
		if err != nil {
			tx.Rollback()
			if attempt < maxRetries-1 {
				time.Sleep(time.Duration(float64(retryDelay) * 1.5))
				continue
			}
			return nil, err
		}

		// Commit transaction
		if err := tx.Commit(); err != nil {
			if attempt < maxRetries-1 {
				time.Sleep(retryDelay)
				continue
			}
			return nil, err
		}

		// Parse JSON data
		var data map[string]interface{}
		if err := json.Unmarshal([]byte(dataStr), &data); err != nil {
			return nil, fmt.Errorf("error parsing job data: %v", err)
		}

		result := &PeakJobResult{
			ID:        int(jobID),
			Data:      data,
			StartedAt: &startedAt,
		}

		if scheduleAt.Valid {
			result.ScheduleAt = &scheduleAt.Time
		}

		return result, nil
	}

	return nil, fmt.Errorf("could not lock and claim a job for path='%s' after %d retries", path, maxRetries)
}

// MarkJobCompleted marks a job as completed
func (jq *KBJobQueue) MarkJobCompleted(jobID int, maxRetries int, retryDelay time.Duration) (*JobCompletionResult, error) {
	if jobID <= 0 {
		return nil, fmt.Errorf("job_id must be a valid positive integer")
	}

	if maxRetries <= 0 {
		maxRetries = 3
	}
	if retryDelay <= 0 {
		retryDelay = time.Second
	}

	for attempt := 0; attempt < maxRetries; attempt++ {
		// Start transaction
		tx, err := jq.conn.Begin()
		if err != nil {
			if attempt < maxRetries-1 {
				time.Sleep(retryDelay)
				continue
			}
			return nil, err
		}

		// Lock query
		lockQuery := fmt.Sprintf(`
			SELECT id
			FROM %s
			WHERE id = $1
			FOR UPDATE NOWAIT
		`, jq.BaseTable)

		var lockedID int
		err = tx.QueryRow(lockQuery, jobID).Scan(&lockedID)
		if err != nil {
			tx.Rollback()
			if err == sql.ErrNoRows {
				return nil, fmt.Errorf("no job found with id=%d", jobID)
			}
			if isLockError(err) && attempt < maxRetries-1 {
				time.Sleep(retryDelay)
				continue
			}
			return nil, err
		}

		// Update query
		updateQuery := fmt.Sprintf(`
			UPDATE %s
			SET completed_at = NOW(),
				valid = FALSE,
				is_active = FALSE
			WHERE id = $1
			RETURNING id, completed_at
		`, jq.BaseTable)

		var completedAt time.Time
		err = tx.QueryRow(updateQuery, jobID).Scan(&lockedID, &completedAt)
		if err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("failed to mark job %d as completed", jobID)
		}

		// Commit transaction
		if err := tx.Commit(); err != nil {
			if attempt < maxRetries-1 {
				time.Sleep(retryDelay)
				continue
			}
			return nil, err
		}

		return &JobCompletionResult{
			Success:     true,
			JobID:       lockedID,
			CompletedAt: &completedAt,
		}, nil
	}

	return nil, fmt.Errorf("could not lock job id=%d after %d attempts", jobID, maxRetries)
}

// PushJobData pushes new job data to an available slot
func (jq *KBJobQueue) PushJobData(path string, data map[string]interface{}, maxRetries int, retryDelay time.Duration) (*PushJobResult, error) {
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

	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal data: %v", err)
	}

	selectSQL := fmt.Sprintf(`
		SELECT id
		FROM %s
		WHERE path = $1
		AND valid = FALSE
		ORDER BY completed_at ASC
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`, jq.BaseTable)

	updateSQL := fmt.Sprintf(`
		UPDATE %s
		SET data = $1,
			schedule_at = timezone('UTC', now()),
			started_at = timezone('UTC', now()),
			completed_at = timezone('UTC', now()),
			valid = TRUE,
			is_active = FALSE
		WHERE id = $2
		RETURNING id, schedule_at, data
	`, jq.BaseTable)

	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Start transaction
		tx, err := jq.conn.Begin()
		if err != nil {
			if attempt < maxRetries {
				time.Sleep(retryDelay)
				continue
			}
			return nil, err
		}

		// Select available slot
		var jobID int64
		err = tx.QueryRow(selectSQL, path).Scan(&jobID)
		if err != nil {
			tx.Rollback()
			if err == sql.ErrNoRows {
				return nil, fmt.Errorf("no available job slot for path '%s'", path)
			}
			if isLockError(err) && attempt < maxRetries {
				time.Sleep(retryDelay)
				continue
			}
			return nil, fmt.Errorf("error finding available job slot: %v", err)
		}

		// Update the slot
		var scheduleAt time.Time
		var returnedData string
		err = tx.QueryRow(updateSQL, string(jsonData), jobID).Scan(&jobID, &scheduleAt, &returnedData)
		if err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("failed to update job slot for path '%s'", path)
		}

		// Commit transaction
		if err := tx.Commit(); err != nil {
			if attempt < maxRetries {
				time.Sleep(retryDelay)
				continue
			}
			return nil, err
		}

		// Parse returned data
		var parsedData map[string]interface{}
		if err := json.Unmarshal([]byte(returnedData), &parsedData); err != nil {
			parsedData = data // Fallback to original data
		}

		return &PushJobResult{
			JobID:      int(jobID),
			ScheduleAt: &scheduleAt,
			Data:       parsedData,
		}, nil
	}

	return nil, fmt.Errorf("could not acquire lock for path '%s' after %d attempts", path, maxRetries)
}

// ListPendingJobs lists all pending jobs for a path
func (jq *KBJobQueue) ListPendingJobs(path string, limit *int, offset int) ([]JobRecord, error) {
	if path == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}

	query := fmt.Sprintf(`
		SELECT id, path, schedule_at, started_at, completed_at, is_active, valid, data
		FROM %s
		WHERE path = $1
		AND valid = TRUE
		AND is_active = FALSE
		ORDER BY schedule_at ASC
	`, jq.BaseTable)

	params := []interface{}{path}
	paramCount := 1

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

	rows, err := jq.executeQuery(query, params...)
	if err != nil {
		return nil, fmt.Errorf("error listing pending jobs for path '%s': %v", path, err)
	}

	return mapToJobRecords(rows), nil
}

// ListActiveJobs lists all active jobs for a path
func (jq *KBJobQueue) ListActiveJobs(path string, limit *int, offset int) ([]JobRecord, error) {
	if path == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}

	query := fmt.Sprintf(`
		SELECT id, path, schedule_at, started_at, completed_at, is_active, valid, data
		FROM %s
		WHERE path = $1
		AND valid = TRUE
		AND is_active = TRUE
		ORDER BY started_at ASC
	`, jq.BaseTable)

	params := []interface{}{path}
	paramCount := 1

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

	rows, err := jq.executeQuery(query, params...)
	if err != nil {
		return nil, fmt.Errorf("error listing active jobs for path '%s': %v", path, err)
	}

	return mapToJobRecords(rows), nil
}

// ClearJobQueue clears all jobs for a given path
func (jq *KBJobQueue) ClearJobQueue(path string) (*ClearQueueResult, error) {
	if path == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}

	// Start transaction
	tx, err := jq.conn.Begin()
	if err != nil {
		return nil, err
	}

	// Lock the table
	_, err = tx.Exec(fmt.Sprintf("LOCK TABLE %s IN EXCLUSIVE MODE", jq.BaseTable))
	if err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("error acquiring table lock: %v", err)
	}

	// Update query
	updateQuery := fmt.Sprintf(`
		UPDATE %s
		SET schedule_at = NOW(),
			started_at = NOW(),
			completed_at = NOW(),
			is_active = $1,
			valid = $2,
			data = $3
		WHERE path = $4
		RETURNING id, completed_at
	`, jq.BaseTable)

	rows, err := tx.Query(updateQuery, false, false, "{}", path)
	if err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("error clearing jobs: %v", err)
	}
	defer rows.Close()

	clearedJobs, err := rowsToMaps(rows)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &ClearQueueResult{
		Success:      true,
		ClearedCount: len(clearedJobs),
		ClearedJobs:  clearedJobs,
	}, nil
}

// GetJobStatistics gets comprehensive statistics for jobs at a given path
func (jq *KBJobQueue) GetJobStatistics(path string) (*JobStatistics, error) {
	if path == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}

	query := fmt.Sprintf(`
		SELECT 
			COUNT(*) as total_jobs,
			COUNT(CASE WHEN valid = TRUE AND is_active = FALSE THEN 1 END) as pending_jobs,
			COUNT(CASE WHEN valid = TRUE AND is_active = TRUE THEN 1 END) as active_jobs,
			COUNT(CASE WHEN valid = FALSE THEN 1 END) as completed_jobs,
			MIN(schedule_at) as earliest_scheduled,
			MAX(completed_at) as latest_completed,
			AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) as avg_processing_time_seconds
		FROM %s
		WHERE path = $1
	`, jq.BaseTable)

	result, err := jq.executeSingle(query, path)
	if err != nil {
		return nil, fmt.Errorf("error getting job statistics for path '%s': %v", path, err)
	}

	if result == nil {
		return &JobStatistics{}, nil
	}

	return mapToJobStatistics(result), nil
}

// GetJobByID retrieves a specific job by its ID
func (jq *KBJobQueue) GetJobByID(jobID int) (*JobRecord, error) {
	if jobID <= 0 {
		return nil, fmt.Errorf("job_id must be a valid positive integer")
	}

	query := fmt.Sprintf(`
		SELECT id, path, schedule_at, started_at, completed_at, is_active, valid, data
		FROM %s
		WHERE id = $1
	`, jq.BaseTable)

	result, err := jq.executeSingle(query, jobID)
	if err != nil {
		return nil, fmt.Errorf("error retrieving job with id %d: %v", jobID, err)
	}

	if result == nil {
		return nil, nil
	}

	records := mapToJobRecords([]map[string]interface{}{result})
	if len(records) > 0 {
		return &records[0], nil
	}

	return nil, nil
}

// Helper functions

// mapToJobRecords converts maps to JobRecord slice
func mapToJobRecords(rows []map[string]interface{}) []JobRecord {
	records := []JobRecord{}

	for _, row := range rows {
		record := JobRecord{}

		if id, ok := row["id"].(int64); ok {
			record.ID = int(id)
		}
		if path, ok := row["path"].(string); ok {
			record.Path = path
		}
		if scheduleAt, ok := row["schedule_at"].(time.Time); ok {
			record.ScheduleAt = &scheduleAt
		}
		if startedAt, ok := row["started_at"].(time.Time); ok {
			record.StartedAt = &startedAt
		}
		if completedAt, ok := row["completed_at"].(time.Time); ok {
			record.CompletedAt = &completedAt
		}
		if isActive, ok := row["is_active"].(bool); ok {
			record.IsActive = isActive
		}
		if valid, ok := row["valid"].(bool); ok {
			record.Valid = valid
		}

		// Handle data field
		if dataStr, ok := row["data"].(string); ok {
			var data map[string]interface{}
			if err := json.Unmarshal([]byte(dataStr), &data); err == nil {
				record.Data = data
			}
		}

		records = append(records, record)
	}

	return records
}

// mapToJobStatistics converts a map to JobStatistics
func mapToJobStatistics(m map[string]interface{}) *JobStatistics {
	stats := &JobStatistics{}

	if total, ok := m["total_jobs"].(int64); ok {
		stats.TotalJobs = int(total)
	}
	if pending, ok := m["pending_jobs"].(int64); ok {
		stats.PendingJobs = int(pending)
	}
	if active, ok := m["active_jobs"].(int64); ok {
		stats.ActiveJobs = int(active)
	}
	if completed, ok := m["completed_jobs"].(int64); ok {
		stats.CompletedJobs = int(completed)
	}
	if t, ok := m["earliest_scheduled"].(time.Time); ok {
		stats.EarliestScheduled = &t
	}
	if t, ok := m["latest_completed"].(time.Time); ok {
		stats.LatestCompleted = &t
	}
	if avg, ok := m["avg_processing_time_seconds"].(float64); ok {
		stats.AvgProcessingTimeSeconds = &avg
	}

	return stats
}

// isLockError checks if an error is a lock-related error
func isLockError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "lock") || strings.Contains(errStr, "busy")
}

