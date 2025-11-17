package kb_memory_module

import (
	//"context"
	"database/sql"
	"encoding/json"
	"fmt"
	
	"log"
	"regexp"
	"sort"
	//"strconv"
	"strings"
	//"time"

	//"github.com/lib/pq"
	//_ "github.com/lib/pq"
)

// TreeNode represents a node in the tree with metadata
type TreeNode struct {
	Path      string      `json:"path"`
	Data      interface{} `json:"data"`
	CreatedAt *string     `json:"created_at,omitempty"`
	UpdatedAt *string     `json:"updated_at,omitempty"`
}

// BasicConstructDB is a comprehensive system for storing and querying tree-structured data with full ltree compatibility
type BasicConstructDB struct {
	data             map[string]*TreeNode
	kbDict           map[string]map[string]interface{}
	host             string
	port             int
	dbname           string
	user             string
	password         string
	TableName        string
	connectionParams map[string]interface{}
}

// QueryResult represents a query result
type QueryResult struct {
	Path      string      `json:"path"`
	Data      interface{} `json:"data"`
	CreatedAt *string     `json:"created_at,omitempty"`
	UpdatedAt *string     `json:"updated_at,omitempty"`
}

// TreeStats represents tree statistics
type TreeStats struct {
	TotalNodes int     `json:"total_nodes"`
	MaxDepth   int     `json:"max_depth"`
	AvgDepth   float64 `json:"avg_depth"`
	RootNodes  int     `json:"root_nodes"`
	LeafNodes  int     `json:"leaf_nodes"`
}

// SyncStats represents synchronization statistics
type SyncStats struct {
	Imported int `json:"imported"`
	Exported int `json:"exported"`
}

// NewBasicConstructDB creates a new BasicConstructDB instance
func NewBasicConstructDB(host string, port int, dbname, user, password, TableName string) *BasicConstructDB {
	return &BasicConstructDB{
		data:      make(map[string]*TreeNode),
		kbDict:    make(map[string]map[string]interface{}),
		host:      host,
		port:      port,
		dbname:    dbname,
		user:      user,
		password:  password,
		TableName: TableName,
		connectionParams: map[string]interface{}{
			"host":     host,
			"port":     port,
			"dbname":   dbname,
			"user":     user,
			"password": password,
		},
	}
}

// AddKB adds a knowledge base
func (db *BasicConstructDB) AddKB(kbName, description string) error {
	if _, exists := db.kbDict[kbName]; exists {
		return fmt.Errorf("knowledge base %s already exists", kbName)
	}
	db.kbDict[kbName] = map[string]interface{}{
		"description": description,
	}
	return nil
}

// ValidatePath validates that a path conforms to ltree format
func (db *BasicConstructDB) ValidatePath(path string) bool {
	if path == "" {
		return false
	}

	// ltree labels must start with letter or underscore, then alphanumeric and underscores
	pattern := `^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$`
	matched, err := regexp.MatchString(pattern, path)
	if err != nil || !matched {
		return false
	}

	// Check each label length
	labels := strings.Split(path, ".")
	for _, label := range labels {
		if len(label) < 1 || len(label) > 256 {
			return false
		}
	}
	return true
}

// PathDepth gets the depth (number of levels) of a path
func (db *BasicConstructDB) PathDepth(path string) int {
	return len(strings.Split(path, "."))
}

// PathLabels gets the labels of a path as a slice
func (db *BasicConstructDB) PathLabels(path string) []string {
	return strings.Split(path, ".")
}

// Subpath extracts a subpath from a path
func (db *BasicConstructDB) Subpath(path string, start int, length *int) string {
	labels := db.PathLabels(path)
	if start < 0 {
		start = len(labels) + start
	}

	if length == nil {
		return strings.Join(labels[start:], ".")
	}
	end := start + *length
	if end > len(labels) {
		end = len(labels)
	}
	return strings.Join(labels[start:end], ".")
}

// ConvertLtreeQueryToRegex converts full ltree query syntax to regex
func (db *BasicConstructDB) ConvertLtreeQueryToRegex(query string) string {
	// Handle ltxtquery format (word1@word2@word3)
	if strings.Contains(query, "@") && !strings.HasPrefix(query, "@") && !strings.HasSuffix(query, "@") {
		return db.convertSimplePattern(strings.ReplaceAll(query, "@", "."))
	}
	return db.convertLqueryPattern(query)
}

func (db *BasicConstructDB) convertLqueryPattern(pattern string) string {
	// Escape special regex characters first
	result := regexp.QuoteMeta(pattern)

	// Convert ltree-specific patterns
	// *{n,m} - between n and m levels
	re1 := regexp.MustCompile(`\\*\\\{(\d+),(\d+)\\\}`)
	result = re1.ReplaceAllStringFunc(result, func(match string) string {
		groups := re1.FindStringSubmatch(match)
		return fmt.Sprintf("([^.]+\\.){%s,%s}", groups[1], groups[2])
	})

	// *{n,} - n or more levels
	re2 := regexp.MustCompile(`\\*\\\{(\d+),\\\}`)
	result = re2.ReplaceAllStringFunc(result, func(match string) string {
		groups := re2.FindStringSubmatch(match)
		return fmt.Sprintf("([^.]+\\.){%s,}", groups[1])
	})

	// *{,m} - up to m levels
	re3 := regexp.MustCompile(`\\*\\\{,(\d+)\\\}`)
	result = re3.ReplaceAllStringFunc(result, func(match string) string {
		groups := re3.FindStringSubmatch(match)
		return fmt.Sprintf("([^.]+\\.){{0,%s}}", groups[1])
	})

	// *{n} - exactly n levels
	re4 := regexp.MustCompile(`\\*\\\{(\d+)\\\}`)
	result = re4.ReplaceAllStringFunc(result, func(match string) string {
		groups := re4.FindStringSubmatch(match)
		return fmt.Sprintf("([^.]+\\.){%s}", groups[1])
	})

	// ** - any number of levels (including zero)
	result = strings.ReplaceAll(result, "\\*\\*", ".*")

	// * - exactly one level
	result = strings.ReplaceAll(result, "\\*", "[^.]+")

	// {a,b,c} - choice between alternatives
	re5 := regexp.MustCompile(`\\{([^}]+)\\}`)
	result = re5.ReplaceAllStringFunc(result, func(match string) string {
		groups := re5.FindStringSubmatch(match)
		return fmt.Sprintf("(%s)", strings.ReplaceAll(groups[1], ",", "|"))
	})

	// Remove trailing dots from quantified patterns
	re6 := regexp.MustCompile(`\\\.\)\{([^}]+)\}`)
	result = re6.ReplaceAllString(result, "){$1}[^.]*")

	return fmt.Sprintf("^%s$", result)
}

func (db *BasicConstructDB) convertSimplePattern(pattern string) string {
	parts := strings.Split(pattern, ".*")
	escapedParts := make([]string, len(parts))
	for i, part := range parts {
		escapedParts[i] = regexp.QuoteMeta(part)
	}
	result := strings.Join(escapedParts, ".*")

	result = strings.ReplaceAll(result, "\\*\\*", ".*")
	result = strings.ReplaceAll(result, "\\*", "[^.]+")

	re := regexp.MustCompile(`\\{([^}]+)\\}`)
	result = re.ReplaceAllStringFunc(result, func(match string) string {
		groups := re.FindStringSubmatch(match)
		return fmt.Sprintf("(%s)", strings.ReplaceAll(groups[1], ",", "|"))
	})

	return fmt.Sprintf("^%s$", result)
}

// LtreeMatch checks if path matches ltree query using ~ operator
func (db *BasicConstructDB) LtreeMatch(path, query string) bool {
	regexPattern := db.ConvertLtreeQueryToRegex(query)
	matched, err := regexp.MatchString(regexPattern, path)
	return err == nil && matched
}

// LtxtqueryMatch checks if path matches ltxtquery using @@ operator
func (db *BasicConstructDB) LtxtqueryMatch(path, ltxtquery string) bool {
	pathWords := make(map[string]bool)
	for _, word := range strings.Split(path, ".") {
		pathWords[word] = true
	}

	query := strings.TrimSpace(ltxtquery)

	// Handle simple cases first
	if !strings.Contains(query, "&") && !strings.Contains(query, "|") && !strings.Contains(query, "!") {
		return pathWords[strings.TrimSpace(query)]
	}

	// This is a simplified implementation for basic boolean operations
	// A full implementation would require a proper expression parser
	if strings.Contains(query, "&") {
		words := strings.Split(query, "&")
		for _, word := range words {
			if !pathWords[strings.TrimSpace(word)] {
				return false
			}
		}
		return true
	}

	if strings.Contains(query, "|") {
		words := strings.Split(query, "|")
		for _, word := range words {
			if pathWords[strings.TrimSpace(word)] {
				return true
			}
		}
		return false
	}

	return false
}

// LtreeAncestor checks if ancestor @> descendant (ancestor-of relationship)
func (db *BasicConstructDB) LtreeAncestor(ancestor, descendant string) bool {
	if ancestor == descendant {
		return false
	}
	return strings.HasPrefix(descendant, ancestor+".")
}

// LtreeDescendant checks if descendant <@ ancestor (descendant-of relationship)
func (db *BasicConstructDB) LtreeDescendant(descendant, ancestor string) bool {
	return db.LtreeAncestor(ancestor, descendant)
}

// LtreeAncestorOrEqual checks if ancestor @> descendant or ancestor = descendant
func (db *BasicConstructDB) LtreeAncestorOrEqual(ancestor, descendant string) bool {
	return ancestor == descendant || db.LtreeAncestor(ancestor, descendant)
}

// LtreeDescendantOrEqual checks if descendant <@ ancestor or descendant = ancestor
func (db *BasicConstructDB) LtreeDescendantOrEqual(descendant, ancestor string) bool {
	return descendant == ancestor || db.LtreeDescendant(descendant, ancestor)
}

// LtreeConcatenate concatenates two ltree paths using || operator
func (db *BasicConstructDB) LtreeConcatenate(path1, path2 string) string {
	if path1 == "" {
		return path2
	}
	if path2 == "" {
		return path1
	}
	return fmt.Sprintf("%s.%s", path1, path2)
}

// Nlevel returns the number of labels in the path (ltree nlevel function)
func (db *BasicConstructDB) Nlevel(path string) int {
	return len(strings.Split(path, "."))
}

// Subltree extracts a subtree from start to end position (ltree subltree function)
func (db *BasicConstructDB) Subltree(path string, start, end int) string {
	labels := strings.Split(path, ".")
	if start >= len(labels) {
		return ""
	}
	if end > len(labels) {
		end = len(labels)
	}
	return strings.Join(labels[start:end], ".")
}

// SubpathFunc extracts subpath (ltree subpath function)
func (db *BasicConstructDB) SubpathFunc(path string, offset int, length *int) string {
	return db.Subpath(path, offset, length)
}

// IndexFunc finds the position of subpath in path (ltree index function)
func (db *BasicConstructDB) IndexFunc(path, subpath string, offset int) int {
	labels := strings.Split(path, ".")
	subLabels := strings.Split(subpath, ".")

	for i := offset; i <= len(labels)-len(subLabels); i++ {
		match := true
		for j := 0; j < len(subLabels); j++ {
			if labels[i+j] != subLabels[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

// Text2ltree converts text to ltree format (basic validation and normalization)
func (db *BasicConstructDB) Text2ltree(text string) (string, error) {
	if db.ValidatePath(text) {
		return text, nil
	}
	return "", fmt.Errorf("cannot convert '%s' to valid ltree format", text)
}

// Ltree2text converts ltree to text (identity function for valid paths)
func (db *BasicConstructDB) Ltree2text(ltreePath string) string {
	return ltreePath
}

// LCA finds the longest common ancestor of multiple paths (ltree lca function)
func (db *BasicConstructDB) LCA(paths ...string) *string {
	if len(paths) == 0 {
		return nil
	}

	if len(paths) == 1 {
		return &paths[0]
	}

	// Split all paths into labels
	allLabels := make([][]string, len(paths))
	minLength := len(strings.Split(paths[0], "."))

	for i, path := range paths {
		allLabels[i] = strings.Split(path, ".")
		if len(allLabels[i]) < minLength {
			minLength = len(allLabels[i])
		}
	}

	// Find common prefix
	var commonLabels []string
	for i := 0; i < minLength; i++ {
		currentLabel := allLabels[0][i]
		match := true
		for j := 1; j < len(allLabels); j++ {
			if allLabels[j][i] != currentLabel {
				match = false
				break
			}
		}
		if match {
			commonLabels = append(commonLabels, currentLabel)
		} else {
			break
		}
	}

	if len(commonLabels) == 0 {
		return nil
	}

	result := strings.Join(commonLabels, ".")
	return &result
}

// Store stores data at a specific path in the tree
func (db *BasicConstructDB) Store(path string, data interface{}, createdAt, updatedAt *string) error {
	if !db.ValidatePath(path) {
		return fmt.Errorf("invalid ltree path: %s", path)
	}

	db.data[path] = &TreeNode{
		Path:      path,
		Data:      data,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}
	return nil
}

// Get retrieves data from a specific path
func (db *BasicConstructDB) Get(path string) (interface{}, error) {
	if !db.ValidatePath(path) {
		return nil, fmt.Errorf("invalid ltree path: %s", path)
	}

	node, exists := db.data[path]
	if !exists {
		return nil, nil
	}
	return node.Data, nil
}

// GetNode retrieves the full node (with metadata) from a specific path
func (db *BasicConstructDB) GetNode(path string) (*TreeNode, error) {
	if !db.ValidatePath(path) {
		return nil, fmt.Errorf("invalid ltree path: %s", path)
	}

	node, exists := db.data[path]
	if !exists {
		return nil, nil
	}

	// Create a copy
	return &TreeNode{
		Path:      node.Path,
		Data:      node.Data,
		CreatedAt: node.CreatedAt,
		UpdatedAt: node.UpdatedAt,
	}, nil
}

// Query queries using ltree pattern matching (~)
func (db *BasicConstructDB) Query(pattern string) []QueryResult {
	var results []QueryResult

	for path, node := range db.data {
		if db.LtreeMatch(path, pattern) {
			results = append(results, QueryResult{
				Path:      path,
				Data:      node.Data,
				CreatedAt: node.CreatedAt,
				UpdatedAt: node.UpdatedAt,
			})
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Path < results[j].Path
	})

	return results
}

// QueryLtxtquery queries using ltxtquery pattern matching (@@)
func (db *BasicConstructDB) QueryLtxtquery(ltxtquery string) []QueryResult {
	var results []QueryResult

	for path, node := range db.data {
		if db.LtxtqueryMatch(path, ltxtquery) {
			results = append(results, QueryResult{
				Path:      path,
				Data:      node.Data,
				CreatedAt: node.CreatedAt,
				UpdatedAt: node.UpdatedAt,
			})
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Path < results[j].Path
	})

	return results
}

// QueryByOperator queries using specific ltree operators
func (db *BasicConstructDB) QueryByOperator(operator, path1, path2 string) []QueryResult {
	var results []QueryResult

	switch operator {
	case "@>": // ancestor-of
		for path, node := range db.data {
			if db.LtreeAncestor(path1, path) {
				results = append(results, QueryResult{
					Path:      path,
					Data:      node.Data,
					CreatedAt: node.CreatedAt,
					UpdatedAt: node.UpdatedAt,
				})
			}
		}
	case "<@": // descendant-of
		for path, node := range db.data {
			if db.LtreeDescendant(path, path1) {
				results = append(results, QueryResult{
					Path:      path,
					Data:      node.Data,
					CreatedAt: node.CreatedAt,
					UpdatedAt: node.UpdatedAt,
				})
			}
		}
	case "~": // lquery match
		return db.Query(path1)
	case "@@": // ltxtquery match
		return db.QueryLtxtquery(path1)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Path < results[j].Path
	})

	return results
}

// QueryAncestors gets all ancestors using @> operator
func (db *BasicConstructDB) QueryAncestors(path string) ([]QueryResult, error) {
	if !db.ValidatePath(path) {
		return nil, fmt.Errorf("invalid ltree path: %s", path)
	}

	var results []QueryResult
	for storedPath, node := range db.data {
		if db.LtreeAncestor(storedPath, path) {
			results = append(results, QueryResult{
				Path:      storedPath,
				Data:      node.Data,
				CreatedAt: node.CreatedAt,
				UpdatedAt: node.UpdatedAt,
			})
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return len(strings.Split(results[i].Path, ".")) < len(strings.Split(results[j].Path, "."))
	})

	return results, nil
}

// QueryDescendants gets all descendants using <@ operator
func (db *BasicConstructDB) QueryDescendants(path string) ([]QueryResult, error) {
	if !db.ValidatePath(path) {
		return nil, fmt.Errorf("invalid ltree path: %s", path)
	}

	var results []QueryResult
	for storedPath, node := range db.data {
		if db.LtreeDescendant(storedPath, path) {
			results = append(results, QueryResult{
				Path:      storedPath,
				Data:      node.Data,
				CreatedAt: node.CreatedAt,
				UpdatedAt: node.UpdatedAt,
			})
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Path < results[j].Path
	})

	return results, nil
}

// QuerySubtree gets node and all its descendants
func (db *BasicConstructDB) QuerySubtree(path string) ([]QueryResult, error) {
	var results []QueryResult

	// Add the node itself if it exists
	if db.Exists(path) {
		node := db.data[path]
		results = append(results, QueryResult{
			Path:      path,
			Data:      node.Data,
			CreatedAt: node.CreatedAt,
			UpdatedAt: node.UpdatedAt,
		})
	}

	// Add all descendants
	descendants, err := db.QueryDescendants(path)
	if err != nil {
		return nil, err
	}
	results = append(results, descendants...)

	sort.Slice(results, func(i, j int) bool {
		return results[i].Path < results[j].Path
	})

	return results, nil
}

// Exists checks if a path exists
func (db *BasicConstructDB) Exists(path string) bool {
	_, exists := db.data[path]
	return exists && db.ValidatePath(path)
}

// Delete deletes a specific node
func (db *BasicConstructDB) Delete(path string) bool {
	if _, exists := db.data[path]; exists {
		delete(db.data, path)
		return true
	}
	return false
}

// AddSubtree adds a subtree to a specific path
func (db *BasicConstructDB) AddSubtree(path string, subtree []QueryResult) error {
	if !db.ValidatePath(path) {
		return fmt.Errorf("invalid ltree path: %s", path)
	}
	if !db.Exists(path) {
		return fmt.Errorf("path %s does not exist", path)
	}

	for _, node := range subtree {
		newPath := fmt.Sprintf("%s.%s", path, node.Path)
		err := db.Store(newPath, node.Data, node.CreatedAt, node.UpdatedAt)
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteSubtree deletes a node and all its descendants
func (db *BasicConstructDB) DeleteSubtree(path string) int {
	var toDelete []string

	if _, exists := db.data[path]; exists {
		toDelete = append(toDelete, path)
	}

	// Find all descendants
	for storedPath := range db.data {
		if db.LtreeDescendant(storedPath, path) {
			toDelete = append(toDelete, storedPath)
		}
	}

	// Delete them
	for _, deletePath := range toDelete {
		delete(db.data, deletePath)
	}

	return len(toDelete)
}

// getDBConnection creates a database connection
func (db *BasicConstructDB) getDBConnection() (*sql.DB, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		db.host, db.port, db.user, db.password, db.dbname)
	return sql.Open("postgres", connStr)
}

// ImportFromPostgres imports data from a PostgreSQL table with ltree column
func (db *BasicConstructDB) ImportFromPostgres(tableName, pathColumn, dataColumn, createdAtColumn, updatedAtColumn string) (int, error) {
	conn, err := db.getDBConnection()
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	// Check if table exists
	var exists bool
	err = conn.QueryRow("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1)", tableName).Scan(&exists)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, fmt.Errorf("table '%s' does not exist", tableName)
	}

	// Import data
	query := fmt.Sprintf(`
		SELECT 
			%s::text as path,
			%s,
			%s::text as created_at,
			%s::text as updated_at
		FROM %s
		ORDER BY %s`,
		pathColumn, dataColumn, createdAtColumn, updatedAtColumn, tableName, pathColumn)

	rows, err := conn.Query(query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	importedCount := 0
	for rows.Next() {
		var path string
		var dataBytes []byte
		var createdAt, updatedAt sql.NullString

		err := rows.Scan(&path, &dataBytes, &createdAt, &updatedAt)
		if err != nil {
			continue
		}

		var data interface{}
		if len(dataBytes) > 0 {
			json.Unmarshal(dataBytes, &data)
		}

		var createdAtPtr, updatedAtPtr *string
		if createdAt.Valid {
			createdAtPtr = &createdAt.String
		}
		if updatedAt.Valid {
			updatedAtPtr = &updatedAt.String
		}

		db.Store(path, data, createdAtPtr, updatedAtPtr)
		importedCount++
	}

	return importedCount, nil
}

// ExportToPostgres exports data to a PostgreSQL table with ltree support
func (db *BasicConstructDB) ExportToPostgres(tableName string, createTable, clearExisting bool) (int, error) {
	conn, err := db.getDBConnection()
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	// Enable ltree extension
	_, err = conn.Exec("CREATE EXTENSION IF NOT EXISTS ltree")
	if err != nil {
		return 0, err
	}

	if createTable {
		// Create table with ltree support
		createTableQuery := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				id SERIAL PRIMARY KEY,
				path LTREE UNIQUE NOT NULL,
				data JSONB,
				created_at TIMESTAMP,
				updated_at TIMESTAMP
			)`, tableName)
		_, err = conn.Exec(createTableQuery)
		if err != nil {
			return 0, err
		}

		// Create indexes
		_, err = conn.Exec(fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_path_idx ON %s USING GIST (path)", tableName, tableName))
		if err != nil {
			return 0, err
		}
		_, err = conn.Exec(fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_data_idx ON %s USING GIN (data)", tableName, tableName))
		if err != nil {
			return 0, err
		}
	}

	if clearExisting {
		_, err = conn.Exec(fmt.Sprintf("TRUNCATE TABLE %s", tableName))
		if err != nil {
			return 0, err
		}
	}

	// Export data
	exportedCount := 0
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (path, data, created_at, updated_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (path) 
		DO UPDATE SET 
			data = EXCLUDED.data,
			updated_at = EXCLUDED.updated_at`, tableName)

	for path, node := range db.data {
		dataBytes, err := json.Marshal(node.Data)
		if err != nil {
			continue
		}

		var createdAt, updatedAt interface{}
		if node.CreatedAt != nil {
			createdAt = *node.CreatedAt
		}
		if node.UpdatedAt != nil {
			updatedAt = *node.UpdatedAt
		}

		_, err = conn.Exec(insertQuery, path, dataBytes, createdAt, updatedAt)
		if err != nil {
			log.Printf("Error exporting path %s: %v", path, err)
			continue
		}
		exportedCount++
	}

	return exportedCount, nil
}

// SyncWithPostgres synchronizes data with PostgreSQL table
func (db *BasicConstructDB) SyncWithPostgres(direction string) SyncStats {
	stats := SyncStats{}

	if direction == "import" || direction == "both" {
		imported, err := db.ImportFromPostgres(db.TableName, "path", "data", "created_at", "updated_at")
		if err != nil {
			log.Printf("Import failed: %v", err)
		} else {
			stats.Imported = imported
		}
	}

	if direction == "export" || direction == "both" {
		exported, err := db.ExportToPostgres(db.TableName, true, false)
		if err != nil {
			log.Printf("Export failed: %v", err)
		} else {
			stats.Exported = exported
		}
	}

	return stats
}

// GetStats gets comprehensive tree statistics
func (db *BasicConstructDB) GetStats() TreeStats {
	if len(db.data) == 0 {
		return TreeStats{}
	}

	var depths []int
	rootNodes := 0

	for path := range db.data {
		depth := db.Nlevel(path)
		depths = append(depths, depth)
		if depth == 1 {
			rootNodes++
		}
	}

	// Calculate max depth
	maxDepth := 0
	totalDepth := 0
	for _, depth := range depths {
		if depth > maxDepth {
			maxDepth = depth
		}
		totalDepth += depth
	}

	// Count leaf nodes (nodes with no children)
	leafNodes := 0
	for path := range db.data {
		hasChildren := false
		for otherPath := range db.data {
			if db.LtreeAncestor(path, otherPath) {
				hasChildren = true
				break
			}
		}
		if !hasChildren {
			leafNodes++
		}
	}

	avgDepth := float64(totalDepth) / float64(len(depths))

	return TreeStats{
		TotalNodes: len(db.data),
		MaxDepth:   maxDepth,
		AvgDepth:   avgDepth,
		RootNodes:  rootNodes,
		LeafNodes:  leafNodes,
	}
}

// Clear clears all data
func (db *BasicConstructDB) Clear() {
	db.data = make(map[string]*TreeNode)
}

// Size gets the number of nodes
func (db *BasicConstructDB) Size() int {
	return len(db.data)
}

// GetAllPaths gets all paths sorted
func (db *BasicConstructDB) GetAllPaths() []string {
	paths := make([]string, 0, len(db.data))
	for path := range db.data {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

