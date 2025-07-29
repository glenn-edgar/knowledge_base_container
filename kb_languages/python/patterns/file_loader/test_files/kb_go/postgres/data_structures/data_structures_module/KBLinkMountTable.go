package data_structures_module

import (
	"database/sql"
	"fmt"
	//"log"

	_ "github.com/lib/pq" // PostgreSQL driver
)

// KBLinkMountTable represents the link mount table operations
type KBLinkMountTable struct {
	conn      *sql.DB
	baseTable string
}

// NewKBLinkMountTable creates a new instance of KBLinkMountTable
// Equivalent to Python's __init__ method
func NewKBLinkMountTable(conn *sql.DB, database string) *KBLinkMountTable {
	return &KBLinkMountTable{
		conn:      conn,
		baseTable: database + "_link_mount",
	}
}

// FindRecordsByLinkName finds records by link_name, optionally filtered by parent_node_kb
func (kmt *KBLinkMountTable) FindRecordsByLinkName(linkName string, kb *string) ([]map[string]interface{}, error) {
	var query string
	var args []interface{}

	if kb == nil {
		query = fmt.Sprintf(`
			SELECT *
			FROM %s
			WHERE link_name = $1
		`, kmt.baseTable)
		args = []interface{}{linkName}
	} else {
		query = fmt.Sprintf(`
			SELECT *
			FROM %s
			WHERE link_name = $1 AND knowledge_base = $2
		`, kmt.baseTable)
		args = []interface{}{linkName, *kb}
	}

	rows, err := kmt.conn.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Get column names from cursor description and convert rows to dictionaries
	return kmt.fetchAllRowsAsDictionaries(rows)
}

// FindRecordsByMountPath finds records by mount_path, optionally filtered by knowledge_base
func (kmt *KBLinkMountTable) FindRecordsByMountPath(mountPath string, kb *string) ([]map[string]interface{}, error) {
	var query string
	var args []interface{}

	if kb == nil {
		query = fmt.Sprintf(`
			SELECT *
			FROM %s
			WHERE mount_path = $1
		`, kmt.baseTable)
		args = []interface{}{mountPath}
	} else {
		query = fmt.Sprintf(`
			SELECT *
			FROM %s
			WHERE mount_path = $1 AND knowledge_base = $2
		`, kmt.baseTable)
		args = []interface{}{mountPath, *kb}
	}

	rows, err := kmt.conn.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Convert rows to dictionaries (equivalent to cursor.fetchall())
	return kmt.fetchAllRows(rows)
}

// FindAllLinkNames gets all unique link names from the table
func (kmt *KBLinkMountTable) FindAllLinkNames() ([]string, error) {
	query := fmt.Sprintf("SELECT DISTINCT link_name FROM %s ORDER BY link_name", kmt.baseTable)
	
	rows, err := kmt.conn.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Get all rows first (equivalent to cursor.fetchall())
	allRows, err := kmt.fetchAllRows(rows)
	if err != nil {
		return nil, err
	}

	// Extract link_name from each row (equivalent to [row["link_name"] for row in cursor.fetchall()])
	var linkNames []string
	for _, row := range allRows {
		if linkName, ok := row["link_name"].(string); ok {
			linkNames = append(linkNames, linkName)
		}
	}

	return linkNames, nil
}

// FindAllMountPaths gets all unique mount paths from the table
func (kmt *KBLinkMountTable) FindAllMountPaths() ([]string, error) {
	query := fmt.Sprintf("SELECT DISTINCT mount_path FROM %s ORDER BY mount_path", kmt.baseTable)
	
	rows, err := kmt.conn.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Get all rows first (equivalent to cursor.fetchall())
	allRows, err := kmt.fetchAllRows(rows)
	if err != nil {
		return nil, err
	}

	// Extract mount_path from each row (equivalent to [row["mount_path"] for row in cursor.fetchall()])
	var mountPaths []string
	for _, row := range allRows {
		if mountPath, ok := row["mount_path"].(string); ok {
			mountPaths = append(mountPaths, mountPath)
		}
	}

	return mountPaths, nil
}

// fetchAllRowsAsDictionaries converts sql.Rows to dictionaries (matches Python's dict(zip(columns, row)))
func (kmt *KBLinkMountTable) fetchAllRowsAsDictionaries(rows *sql.Rows) ([]map[string]interface{}, error) {
	// Get column names from cursor description
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	var results []map[string]interface{}
	
	for rows.Next() {
		// Create a slice to hold the values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert rows to dictionaries (equivalent to dict(zip(columns, row)))
		rowMap := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			// Convert []byte to string for text fields
			if b, ok := val.([]byte); ok {
				val = string(b)
			}
			rowMap[col] = val
		}

		results = append(results, rowMap)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return results, nil
}

// fetchAllRows converts sql.Rows to a slice of maps (equivalent to cursor.fetchall())
func (kmt *KBLinkMountTable) fetchAllRows(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	var results []map[string]interface{}
	
	for rows.Next() {
		// Create a slice to hold the values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Create a map for this row (equivalent to cursor returning dictionaries)
		rowMap := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			// Convert []byte to string for text fields
			if b, ok := val.([]byte); ok {
				val = string(b)
			}
			rowMap[col] = val
		}

		results = append(results, rowMap)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return results, nil
}

