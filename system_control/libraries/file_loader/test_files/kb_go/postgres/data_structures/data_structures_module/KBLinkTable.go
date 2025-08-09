package data_structures_module

import (
	"database/sql"
	"fmt"
	//"log"

	_ "github.com/lib/pq" // PostgreSQL driver
)

// KBLinkTable represents the link table operations
type KBLinkTable struct {
	conn      *sql.DB
	baseTable string
}

// NewKBLinkTable creates a new instance of KBLinkTable
// Equivalent to Python's __init__ method
func NewKBLinkTable(conn *sql.DB, baseTable string) *KBLinkTable {
	return &KBLinkTable{
		conn:      conn,
		baseTable: baseTable + "_link",
	}
}

// FindRecordsByLinkName finds records by link_name, optionally filtered by knowledge_base
func (kt *KBLinkTable) FindRecordsByLinkName(linkName string, kb *string) ([]map[string]interface{}, error) {
	var query string
	var args []interface{}

	if kb == nil {
		query = fmt.Sprintf(`
			SELECT *
			FROM %s
			WHERE link_name = $1
		`, kt.baseTable)
		args = []interface{}{linkName}
	} else {
		query = fmt.Sprintf(`
			SELECT *
			FROM %s
			WHERE link_name = $1 AND parent_node_kb = $2
		`, kt.baseTable)
		args = []interface{}{linkName, *kb}
	}

	rows, err := kt.conn.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	return kt.fetchAllRows(rows)
}

// FindRecordsByNodePath finds records by node_path, optionally filtered by knowledge_base
func (kt *KBLinkTable) FindRecordsByNodePath(nodePath string, kb *string) ([]map[string]interface{}, error) {
	var query string
	var args []interface{}

	if kb == nil {
		query = fmt.Sprintf(`
			SELECT *
			FROM %s
			WHERE parent_path = $1
		`, kt.baseTable)
		args = []interface{}{nodePath}
	} else {
		query = fmt.Sprintf(`
			SELECT *
			FROM %s
			WHERE parent_path = $1 AND parent_node_kb = $2
		`, kt.baseTable)
		args = []interface{}{nodePath, *kb}
	}

	rows, err := kt.conn.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	return kt.fetchAllRows(rows)
}

// FindAllLinkNames gets all unique link names from the table
func (kt *KBLinkTable) FindAllLinkNames() ([]string, error) {
	query := fmt.Sprintf("SELECT DISTINCT link_name FROM %s ORDER BY link_name", kt.baseTable)
	
	rows, err := kt.conn.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Get all rows first (equivalent to cursor.fetchall())
	allRows, err := kt.fetchAllRows(rows)
	if err != nil {
		return nil, err
	}

	// Extract link_name from each row (equivalent to row["link_name"])
	var returnValue []string
	for _, row := range allRows {
		if linkName, ok := row["link_name"].(string); ok {
			returnValue = append(returnValue, linkName)
		}
	}

	return returnValue, nil
}

// FindAllNodeNames gets all unique node paths from the table
func (kt *KBLinkTable) FindAllNodeNames() ([]string, error) {
	query := fmt.Sprintf("SELECT DISTINCT parent_path FROM %s ORDER BY parent_path", kt.baseTable)
	
	rows, err := kt.conn.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Get all rows first (equivalent to cursor.fetchall())
	allRows, err := kt.fetchAllRows(rows)
	if err != nil {
		return nil, err
	}

	// Extract parent_path from each row (equivalent to [row["parent_path"] for row in cursor.fetchall()])
	var nodePaths []string
	for _, row := range allRows {
		if parentPath, ok := row["parent_path"].(string); ok {
			nodePaths = append(nodePaths, parentPath)
		}
	}

	return nodePaths, nil
}

// fetchAllRows converts sql.Rows to a slice of maps (equivalent to cursor.fetchall())
func (kt *KBLinkTable) fetchAllRows(rows *sql.Rows) ([]map[string]interface{}, error) {
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

