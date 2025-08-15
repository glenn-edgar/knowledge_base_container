package data_structures_module

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	//"github.com/lib/pq"
	//_ "github.com/lib/pq"
)

// Filter represents a query filter with condition and parameters
type Filter struct {
	Condition string
	Params    map[string]interface{}
}

// KBSearch handles SQL filtering for the knowledge_base table
type KBSearch struct {
	Path           []string
	Host           string
	Port           string
	DBName         string
	User           string
	Password       string
	BaseTable      string
	LinkTable      string
	LinkMountTable string
	Filters        []Filter
	Results        []map[string]interface{}
	PathValues     map[string]interface{}
	conn           *sql.DB
}

// NewKBSearch creates a new KBSearch instance and connects to the database
func NewKBSearch(host, port, dbname, user, password, database string) (*KBSearch, error) {
	kb := &KBSearch{
		Path:           []string{},
		Host:           host,
		Port:           port,
		DBName:         dbname,
		User:           user,
		Password:       password,
		BaseTable:      database,
		LinkTable:      database + "_link",
		LinkMountTable: database + "_link_mount",
		Filters:        []Filter{},
		PathValues:     make(map[string]interface{}),
	}

	if err := kb.connect(); err != nil {
		return nil, err
	}

	return kb, nil
}

// connect establishes a connection to the PostgreSQL database
func (kb *KBSearch) connect() error {
	connStr := fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=disable",
		kb.Host, kb.Port, kb.DBName, kb.User, kb.Password)

	conn, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("error connecting to database: %v", err)
	}

	// Test the connection
	if err := conn.Ping(); err != nil {
		return fmt.Errorf("error pinging database: %v", err)
	}

	kb.conn = conn
	return nil
}

// Disconnect closes the database connection
func (kb *KBSearch) Disconnect() error {
	if kb.conn != nil {
		return kb.conn.Close()
	}
	return nil
}

// GetConnAndCursor returns the database connection
func (kb *KBSearch) GetConnAndCursor() (*sql.DB, error) {
	if kb.conn == nil {
		return nil, fmt.Errorf("not connected to database. Call connect() first")
	}
	return kb.conn, nil
}

// ClearFilters clears all filters and resets the query state
func (kb *KBSearch) ClearFilters() {
	kb.Filters = []Filter{}
	kb.Results = nil
}

// SearchKB adds a filter to search for rows matching the specified knowledge_base
func (kb *KBSearch) SearchKB(knowledgeBase string) {
	kb.Filters = append(kb.Filters, Filter{
		Condition: "knowledge_base = $knowledge_base",
		Params:    map[string]interface{}{"knowledge_base": knowledgeBase},
	})
}

// SearchLabel adds a filter to search for rows matching the specified label
func (kb *KBSearch) SearchLabel(label string) {
	kb.Filters = append(kb.Filters, Filter{
		Condition: "label = $label",
		Params:    map[string]interface{}{"label": label},
	})
}

// SearchName adds a filter to search for rows matching the specified name
func (kb *KBSearch) SearchName(name string) {
	kb.Filters = append(kb.Filters, Filter{
		Condition: "name = $name",
		Params:    map[string]interface{}{"name": name},
	})
}

// SearchPropertyKey adds a filter to search for rows where properties contains the key
func (kb *KBSearch) SearchPropertyKey(key string) {
	kb.Filters = append(kb.Filters, Filter{
		Condition: "properties::jsonb ? $property_key",
		Params:    map[string]interface{}{"property_key": key},
	})
}

// SearchPropertyValue adds a filter to search for rows where properties contains the key-value pair
func (kb *KBSearch) SearchPropertyValue(key string, value interface{}) {
	jsonObject := map[string]interface{}{key: value}
	jsonBytes, _ := json.Marshal(jsonObject)

	kb.Filters = append(kb.Filters, Filter{
		Condition: "properties::jsonb @> $json_object::jsonb",
		Params:    map[string]interface{}{"json_object": string(jsonBytes)},
	})
}

// SearchStartingPath adds a filter to search for descendants of the specified path
func (kb *KBSearch) SearchStartingPath(startingPath string) {
	kb.Filters = append(kb.Filters, Filter{
		Condition: "path <@ $starting_path",
		Params:    map[string]interface{}{"starting_path": startingPath},
	})
}

// SearchPath adds a filter to search for rows matching the LTREE path expression
func (kb *KBSearch) SearchPath(pathExpression string) {
	kb.Filters = append(kb.Filters, Filter{
		Condition: "path ~ $path_expr",
		Params:    map[string]interface{}{"path_expr": pathExpression},
	})
}

// SearchHasLink adds a filter to search for rows where has_link is TRUE
func (kb *KBSearch) SearchHasLink() {
	kb.Filters = append(kb.Filters, Filter{
		Condition: "has_link = TRUE",
		Params:    map[string]interface{}{},
	})
}

// SearchHasLinkMount adds a filter to search for rows where has_link_mount is TRUE
func (kb *KBSearch) SearchHasLinkMount() {
	kb.Filters = append(kb.Filters, Filter{
		Condition: "has_link_mount = TRUE",
		Params:    map[string]interface{}{},
	})
}

// ExecuteQuery executes the progressive query with all added filters using CTEs
func (kb *KBSearch) ExecuteQuery() ([]map[string]interface{}, error) {
	if kb.conn == nil {
		return nil, fmt.Errorf("not connected to database")
	}

	columnStr := "*"

	// If no filters, execute simple query
	if len(kb.Filters) == 0 {
		query := fmt.Sprintf("SELECT %s FROM %s", columnStr, kb.BaseTable)
		rows, err := kb.conn.Query(query)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		results, err := kb.rowsToMaps(rows)
		if err != nil {
			return nil, err
		}
		kb.Results = results
		return results, nil
	}

	// Build CTE query
	cteParts := []string{}
	paramSlice := []interface{}{}
	paramCounter := 1

	// Initial CTE
	cteParts = append(cteParts, fmt.Sprintf("base_data AS (SELECT %s FROM %s)", columnStr, kb.BaseTable))

	// Process each filter
	for i, filter := range kb.Filters {
		condition := filter.Condition
		params := filter.Params

		// Replace parameter placeholders with positional parameters
		for paramName, paramValue := range params {
			placeholder := "$" + paramName
			newPlaceholder := fmt.Sprintf("$%d", paramCounter)
			condition = strings.Replace(condition, placeholder, newPlaceholder, -1)
			paramSlice = append(paramSlice, paramValue)
			paramCounter++
		}

		cteName := fmt.Sprintf("filter_%d", i)
		prevCTE := "base_data"
		if i > 0 {
			prevCTE = fmt.Sprintf("filter_%d", i-1)
		}

		var cteQuery string
		if condition != "" && len(params) > 0 {
			cteQuery = fmt.Sprintf("%s AS (SELECT %s FROM %s WHERE %s)", cteName, columnStr, prevCTE, condition)
		} else if condition != "" {
			cteQuery = fmt.Sprintf("%s AS (SELECT %s FROM %s WHERE %s)", cteName, columnStr, prevCTE, condition)
		} else {
			cteQuery = fmt.Sprintf("%s AS (SELECT %s FROM %s)", cteName, columnStr, prevCTE)
		}

		cteParts = append(cteParts, cteQuery)
	}

	// Build final query
	withClause := "WITH " + strings.Join(cteParts, ",\n")
	finalSelect := fmt.Sprintf("SELECT %s FROM filter_%d", columnStr, len(kb.Filters)-1)
	finalQuery := fmt.Sprintf("%s\n%s", withClause, finalSelect)

	// Execute query
	rows, err := kb.conn.Query(finalQuery, paramSlice...)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %v\nQuery: %s\nParams: %v", err, finalQuery, paramSlice)
	}
	defer rows.Close()

	results, err := kb.rowsToMaps(rows)
	if err != nil {
		return nil, err
	}

	kb.Results = results
	return results, nil
}

// rowsToMaps converts SQL rows to slice of maps
func (kb *KBSearch) rowsToMaps(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	results := []map[string]interface{}{}

	for rows.Next() {
		// Create a slice of interface{} to hold the values
		values := make([]interface{}, len(columns))
		valuePointers := make([]interface{}, len(columns))

		for i := range columns {
			valuePointers[i] = &values[i]
		}

		if err := rows.Scan(valuePointers...); err != nil {
			return nil, err
		}

		// Create a map for this row
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

// FindPathValues extracts path values from query results
func (kb *KBSearch) FindPathValues(keyData []map[string]interface{}) []string {
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

// GetResults returns the results of the last executed query
func (kb *KBSearch) GetResults() []map[string]interface{} {
	if kb.Results == nil {
		return []map[string]interface{}{}
	}
	return kb.Results
}




// FindDescription extracts description from properties field of a single query result
func (kb *KBSearch) FindDescription(row map[string]interface{}) map[string]string {
	description := ""
	path := ""

	// Extract path
	if p, ok := row["path"].(string); ok {
		path = p
	}

	// Extract description from properties
	if props, ok := row["properties"]; ok {
		// Handle properties as JSON
		var propsMap map[string]interface{}
		switch p := props.(type) {
		case string:
			json.Unmarshal([]byte(p), &propsMap)
		case map[string]interface{}:
			propsMap = p
		}

		if desc, ok := propsMap["description"].(string); ok {
			description = desc
		}
	}

	return map[string]string{path: description}
}

// FindDescriptions extracts descriptions from properties field of multiple query results
func (kb *KBSearch) FindDescriptions(dataSlice []map[string]interface{}) []map[string]string {
	returnValues := []map[string]string{}

	for _, row := range dataSlice {
		description := ""
		path := ""

		// Extract path
		if p, ok := row["path"].(string); ok {
			path = p
		}

		// Extract description from properties
		if props, ok := row["properties"]; ok {
			// Handle properties as JSON
			var propsMap map[string]interface{}
			switch p := props.(type) {
			case string:
				json.Unmarshal([]byte(p), &propsMap)
			case map[string]interface{}:
				propsMap = p
			}

			if desc, ok := propsMap["description"].(string); ok {
				description = desc
			}
		}

		returnValues = append(returnValues, map[string]string{path: description})
	}

	return returnValues
}

// FindDescriptionPath finds data for a single specified path in the knowledge base
func (kb *KBSearch) FindDescriptionPath(path string) (map[string]interface{}, error) {
	if path == "" {
		return map[string]interface{}{}, nil
	}

	returnValues := make(map[string]interface{})

	query := fmt.Sprintf("SELECT path, data FROM %s WHERE path = $1", kb.BaseTable)
	rows, err := kb.conn.Query(query, path)
	if err != nil {
		return nil, fmt.Errorf("error retrieving data for path: %v", err)
	}
	defer rows.Close()

	found := false
	for rows.Next() {
		var dbPath string
		var data interface{}
		if err := rows.Scan(&dbPath, &data); err != nil {
			return nil, err
		}
		returnValues[dbPath] = data
		found = true
	}

	// Add nil for path not found
	if !found {
		returnValues[path] = nil
	}

	return returnValues, nil
}

// FindDescriptionPaths finds data for multiple specified paths in the knowledge base
func (kb *KBSearch) FindDescriptionPaths(paths []string) ([]map[string]interface{}, error) {
	if len(paths) == 0 {
		return []map[string]interface{}{}, nil
	}

	returnValues := []map[string]interface{}{}

	var rows *sql.Rows
	var err error

	if len(paths) == 1 {
		// Single path optimization
		query := fmt.Sprintf("SELECT path, data FROM %s WHERE path = $1", kb.BaseTable)
		rows, err = kb.conn.Query(query, paths[0])
	} else {
		// Build query with multiple placeholders
		placeholders := make([]string, len(paths))
		args := make([]interface{}, len(paths))
		for i, path := range paths {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args[i] = path
		}

		query := fmt.Sprintf("SELECT path, data FROM %s WHERE path IN (%s)",
			kb.BaseTable, strings.Join(placeholders, ","))
		rows, err = kb.conn.Query(query, args...)
	}

	if err != nil {
		return nil, fmt.Errorf("error retrieving data for paths: %v", err)
	}
	defer rows.Close()

	foundPaths := make(map[string]bool)

	for rows.Next() {
		var path string
		var data interface{}
		if err := rows.Scan(&path, &data); err != nil {
			return nil, err
		}
		returnValues = append(returnValues, map[string]interface{}{path: data})
		foundPaths[path] = true
	}

	// Add nil for paths not found
	for _, path := range paths {
		if !foundPaths[path] {
			returnValues = append(returnValues, map[string]interface{}{path: nil})
		}
	}

	return returnValues, nil
}

// DecodeLinkNodes decodes an ltree path into knowledge base name and node link/name pairs
func (kb *KBSearch) DecodeLinkNodes(path string) (string, [][]string, error) {
	if path == "" {
		return "", nil, fmt.Errorf("path must be a non-empty string")
	}

	pathParts := strings.Split(path, ".")

	// Need at least kb.link.name
	if len(pathParts) < 3 {
		return "", nil, fmt.Errorf("path must have at least 3 elements (kb.link.name), got %d", len(pathParts))
	}

	// After removing kb, remaining elements should be even
	remainingParts := len(pathParts) - 1
	if remainingParts%2 != 0 {
		return "", nil, fmt.Errorf("bad path format: after kb identifier, must have even number of elements (link/name pairs), got %d elements", remainingParts)
	}

	// Extract knowledge base name
	kbName := pathParts[0]

	// Process pairs of (node_link, node_name)
	result := [][]string{}
	for i := 1; i < len(pathParts); i += 2 {
		nodeLink := pathParts[i]
		nodeName := pathParts[i+1]
		result = append(result, []string{nodeLink, nodeName})
	}

	return kbName, result, nil
}

