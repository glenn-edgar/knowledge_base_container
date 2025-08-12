package kb_construct_module

import (
	"database/sql"
	"encoding/json"
	"fmt"
	//"log"
	//"os"
	//"strings"

	_ "github.com/lib/pq"
)

// KnowledgeBaseManager manages knowledge base operations
type KnowledgeBaseManager struct {
	conn      *sql.DB
	tableName string
}

// ConnectionParams holds database connection parameters
type ConnectionParams struct {
	Host     string
	Database string
	User     string
	Password string
	Port     int
}

// NewKnowledgeBaseManager creates a new instance of KnowledgeBaseManager
func NewKnowledgeBaseManager(tableName string, connParams ConnectionParams) (*KnowledgeBaseManager, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		connParams.Host, connParams.Port, connParams.User, connParams.Password, connParams.Database)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("error pinging database: %w", err)
	}

	kb := &KnowledgeBaseManager{
		conn:      db,
		tableName: tableName,
	}

	// Enable ltree extension
	if _, err := kb.conn.Exec("CREATE EXTENSION IF NOT EXISTS ltree;"); err != nil {
		return nil, fmt.Errorf("error creating ltree extension: %w", err)
	}

	// Create tables
	if err := kb.createTables(); err != nil {
		return nil, fmt.Errorf("error creating tables: %w", err)
	}

	return kb, nil
}

// Disconnect closes the database connection
func (kb *KnowledgeBaseManager) Disconnect() error {
	if kb.conn != nil {
		return kb.conn.Close()
	}
	return nil
}

// deleteTable deletes a specified table
func (kb *KnowledgeBaseManager) deleteTable(tableName string, schema string) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s CASCADE;", schema, tableName)
	_, err := kb.conn.Exec(query)
	if err != nil {
		return fmt.Errorf("error deleting table %s.%s: %w", schema, tableName, err)
	}
	return nil
}

// createTables creates all necessary tables
func (kb *KnowledgeBaseManager) createTables() error {
	// Delete existing tables
	tables := []string{
		kb.tableName,
		kb.tableName + "_info",
		kb.tableName + "_link",
		kb.tableName + "_link_mount",
	}
	for _, table := range tables {
		//fmt.Println("deleting table", table)
		if err := kb.deleteTable(table, "public"); err != nil {
			return err
		}
	}
	

	// Create main knowledge base table
	kbTableQuery := fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			knowledge_base VARCHAR NOT NULL,
			label VARCHAR NOT NULL,
			name VARCHAR NOT NULL,
			properties JSON,
			data JSON,
			has_link BOOLEAN DEFAULT FALSE,
			has_link_mount BOOLEAN DEFAULT FALSE,
			path LTREE UNIQUE
		)`, kb.tableName)

	if _, err := kb.conn.Exec(kbTableQuery); err != nil {
		return fmt.Errorf("error creating knowledge base table: %w", err)
	}

	// Create info table
	infoTableQuery := fmt.Sprintf(`
		CREATE TABLE %s_info (
			id SERIAL PRIMARY KEY,
			knowledge_base VARCHAR NOT NULL UNIQUE,
			description VARCHAR
		)`, kb.tableName)

	if _, err := kb.conn.Exec(infoTableQuery); err != nil {
		return fmt.Errorf("error creating info table: %w", err)
	}

	// Create link table
	linkTableQuery := fmt.Sprintf(`
		CREATE TABLE %s_link (
			id SERIAL PRIMARY KEY,
			link_name VARCHAR NOT NULL,
			parent_node_kb VARCHAR NOT NULL,
			parent_path LTREE NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(link_name, parent_node_kb, parent_path)
		)`, kb.tableName)

	if _, err := kb.conn.Exec(linkTableQuery); err != nil {
		return fmt.Errorf("error creating link table: %w", err)
	}

	// Create link mount table
	linkMountTableQuery := fmt.Sprintf(`
		CREATE TABLE %s_link_mount (
			id SERIAL PRIMARY KEY,
			link_name VARCHAR NOT NULL UNIQUE,
			knowledge_base VARCHAR NOT NULL,
			mount_path LTREE NOT NULL,
			description VARCHAR,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(knowledge_base, mount_path)
		)`, kb.tableName)

	if _, err := kb.conn.Exec(linkMountTableQuery); err != nil {
		return fmt.Errorf("error creating link mount table: %w", err)
	}

	// Create all indexes
	return kb.createIndexes()
}

// createIndexes creates all necessary indexes
func (kb *KnowledgeBaseManager) createIndexes() error {
	indexes := []string{
		// Main table indexes
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_kb ON %s (knowledge_base)", kb.tableName, kb.tableName),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_path ON %s USING GIST (path)", kb.tableName, kb.tableName),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_label ON %s (label)", kb.tableName, kb.tableName),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_name ON %s (name)", kb.tableName, kb.tableName),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_has_link ON %s (has_link)", kb.tableName, kb.tableName),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_has_link_mount ON %s (has_link_mount)", kb.tableName, kb.tableName),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_kb_path ON %s (knowledge_base, path)", kb.tableName, kb.tableName),

		// Info table indexes
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_info_kb ON %s_info (knowledge_base)", kb.tableName, kb.tableName),

		// Link table indexes
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_link_name ON %s_link (link_name)", kb.tableName, kb.tableName),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_link_parent_kb ON %s_link (parent_node_kb)", kb.tableName, kb.tableName),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_link_parent_path ON %s_link USING GIST (parent_path)", kb.tableName, kb.tableName),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_link_created ON %s_link (created_at)", kb.tableName, kb.tableName),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_link_composite ON %s_link (link_name, parent_node_kb)", kb.tableName, kb.tableName),

		// Mount table indexes
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_mount_link_name ON %s_link_mount (link_name)", kb.tableName, kb.tableName),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_mount_kb ON %s_link_mount (knowledge_base)", kb.tableName, kb.tableName),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_mount_path ON %s_link_mount USING GIST (mount_path)", kb.tableName, kb.tableName),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_mount_created ON %s_link_mount (created_at)", kb.tableName, kb.tableName),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_mount_composite ON %s_link_mount (knowledge_base, mount_path)", kb.tableName, kb.tableName),
	}

	for _, indexQuery := range indexes {
		if _, err := kb.conn.Exec(indexQuery); err != nil {
			return fmt.Errorf("error creating index: %w", err)
		}
	}

	return nil
}

// AddKB adds a knowledge base entry to the information table
func (kb *KnowledgeBaseManager) AddKB(kbName string, description string) error {
	infoTable := kb.tableName + "_info"
	query := fmt.Sprintf(`
		INSERT INTO %s (knowledge_base, description)
		VALUES ($1, $2)
		ON CONFLICT (knowledge_base) DO NOTHING`, infoTable)

	_, err := kb.conn.Exec(query, kbName, description)
	if err != nil {
		return fmt.Errorf("error adding knowledge base: %w", err)
	}

	return nil
}

// AddNode adds a node to the knowledge base
func (kb *KnowledgeBaseManager) AddNode(kbName, label, name string, properties, data map[string]interface{}, path string) error {
	// Check if kb_name exists in info table
	infoTable := kb.tableName + "_info"
	checkQuery := fmt.Sprintf("SELECT 1 FROM %s WHERE knowledge_base = $1", infoTable)

	var exists int
	err := kb.conn.QueryRow(checkQuery, kbName).Scan(&exists)
	if err == sql.ErrNoRows {
		return fmt.Errorf("knowledge base '%s' not found in info table", kbName)
	} else if err != nil {
		return fmt.Errorf("error checking knowledge base: %w", err)
	}

	// Convert maps to JSON
	var propertiesJSON, dataJSON []byte
	if properties != nil {
		propertiesJSON, err = json.Marshal(properties)
		if err != nil {
			return fmt.Errorf("error marshaling properties: %w", err)
		}
	}
	if data != nil {
		dataJSON, err = json.Marshal(data)
		if err != nil {
			return fmt.Errorf("error marshaling data: %w", err)
		}
	}

	// Insert node
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (knowledge_base, label, name, properties, data, has_link, path)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`, kb.tableName)

	_, err = kb.conn.Exec(insertQuery, kbName, label, name, propertiesJSON, dataJSON, false, path)
	if err != nil {
		return fmt.Errorf("error adding node: %w", err)
	}

	return nil
}

// AddLink adds a link between nodes
func (kb *KnowledgeBaseManager) AddLink(parentKB, parentPath, linkName string) error {
	// Check if parent knowledge base exists
	infoTable := kb.tableName + "_info"
	kbCheckQuery := fmt.Sprintf("SELECT knowledge_base FROM %s WHERE knowledge_base = $1", infoTable)

	var foundKB string
	err := kb.conn.QueryRow(kbCheckQuery, parentKB).Scan(&foundKB)
	if err == sql.ErrNoRows {
		return fmt.Errorf("parent knowledge base '%s' not found", parentKB)
	} else if err != nil {
		return fmt.Errorf("error checking knowledge base: %w", err)
	}

	// Check if parent node exists
	nodeCheckQuery := fmt.Sprintf("SELECT path FROM %s WHERE path = $1", kb.tableName)
	var foundPath string
	err = kb.conn.QueryRow(nodeCheckQuery, parentPath).Scan(&foundPath)
	if err == sql.ErrNoRows {
		return fmt.Errorf("parent node with path '%s' not found", parentPath)
	} else if err != nil {
		return fmt.Errorf("error checking node: %w", err)
	}

	// Check if link name already exists in link_mount table
	linkTable := kb.tableName + "_link"
	linkNameExistsQuery := fmt.Sprintf("SELECT link_name FROM %s WHERE link_name = $1", linkTable)
	var existingLinkName string
	err = kb.conn.QueryRow(linkNameExistsQuery, linkName).Scan(&existingLinkName)
	if err != sql.ErrNoRows {
		return fmt.Errorf("link name '%s' already exists in link_mount table", linkName)
	}

	// Begin transaction
	tx, err := kb.conn.Begin()
	if err != nil {
		return fmt.Errorf("error beginning transaction: %w", err)
	}
	defer tx.Rollback()

	
	linkInsertQuery := fmt.Sprintf(`
		INSERT INTO %s (parent_node_kb, parent_path, link_name)
		VALUES ($1, $2, $3)`, linkTable)

	_, err = tx.Exec(linkInsertQuery, parentKB, parentPath, linkName)
	if err != nil {
		return fmt.Errorf("error inserting link: %w", err)
	}

	// Update has_link flag
	updateQuery := fmt.Sprintf("UPDATE %s SET has_link = TRUE WHERE path = $1", kb.tableName)
	_, err = tx.Exec(updateQuery, parentPath)
	if err != nil {
		return fmt.Errorf("error updating has_link flag: %w", err)
	}

	// Commit transaction
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	return nil
}

// AddLinkMount adds a link mount
func (kb *KnowledgeBaseManager) AddLinkMount(knowledgeBase, path, linkMountName, description string) (string, string, error) {
	// Verify that knowledge_base exists in info table
	infoCheckQuery := fmt.Sprintf("SELECT knowledge_base FROM %s_info WHERE knowledge_base = $1", kb.tableName)
	var foundKB string
	err := kb.conn.QueryRow(infoCheckQuery, knowledgeBase).Scan(&foundKB)
	if err == sql.ErrNoRows {
		return "", "", fmt.Errorf("knowledge base '%s' does not exist in info table", knowledgeBase)
	} else if err != nil {
		return "", "", fmt.Errorf("error checking knowledge base: %w", err)
	}

	// Verify that the path exists for the given knowledge base
	pathCheckQuery := fmt.Sprintf("SELECT id FROM %s WHERE knowledge_base = $1 AND path = $2", kb.tableName)
	var nodeID int
	err = kb.conn.QueryRow(pathCheckQuery, knowledgeBase, path).Scan(&nodeID)
	if err == sql.ErrNoRows {
		return "", "", fmt.Errorf("path '%s' does not exist for knowledge base '%s'", path, knowledgeBase)
	} else if err != nil {
		return "", "", fmt.Errorf("error checking path: %w", err)
	}

	// Verify that link_name does not already exist in link_mount table
	linkNameExistsQuery := fmt.Sprintf("SELECT link_name FROM %s_link_mount WHERE link_name = $1", kb.tableName)
	var existingLinkName string
	err = kb.conn.QueryRow(linkNameExistsQuery, linkMountName).Scan(&existingLinkName)
	if err != sql.ErrNoRows {
		return "", "", fmt.Errorf("link name '%s' already exists in link_mount table", linkMountName)
	}

	// Begin transaction
	tx, err := kb.conn.Begin()
	if err != nil {
		return "", "", fmt.Errorf("error beginning transaction: %w", err)
	}
	defer tx.Rollback()

	// Insert record in link_mount table
	insertLinkMountQuery := fmt.Sprintf(`
		INSERT INTO %s_link_mount (link_name, knowledge_base, mount_path, description)
		VALUES ($1, $2, $3, $4)`, kb.tableName)

	result, err := tx.Exec(insertLinkMountQuery, linkMountName, knowledgeBase, path, description)
	if err != nil {
		return "", "", fmt.Errorf("error inserting link mount: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return "", "", fmt.Errorf("error getting rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return "", "", fmt.Errorf("failed to insert record with link_name '%s', knowledge_base '%s', path '%s' into link_mount table", linkMountName, knowledgeBase, path)
	}

	// Update has_link_mount flag
	updateQuery := fmt.Sprintf(`
		UPDATE %s SET has_link_mount = TRUE 
		WHERE knowledge_base = $1 AND path = $2`, kb.tableName)

	result, err = tx.Exec(updateQuery, knowledgeBase, path)
	if err != nil {
		return "", "", fmt.Errorf("error updating has_link_mount flag: %w", err)
	}

	rowsAffected, err = result.RowsAffected()
	if err != nil {
		return "", "", fmt.Errorf("error getting rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return "", "", fmt.Errorf("no rows were updated for knowledge_base '%s' and path '%s'", knowledgeBase, path)
	}

	// Commit transaction
	if err = tx.Commit(); err != nil {
		return "", "", fmt.Errorf("error committing transaction: %w", err)
	}

	return knowledgeBase, path, nil
}

/*
func main() {
	// Get password from user
	fmt.Print("Enter PostgreSQL password: ")
	var password string
	//fmt.Scanln(&password)
	password := os.Getenv("POSTGRES_PASSWORD")
	if password == "" {
		fmt.Println("password is empty")
		os.Exit(1)
	}

	// Database connection parameters
	connParams := ConnectionParams{
		Host:     "localhost",
		Database: "knowledge_base",
		User:     "gedgar",
		Password: password,
		Port:     5432,
	}

	// Create knowledge base manager
	kbManager, err := NewKnowledgeBaseManager("knowledge_base", connParams)
	if err != nil {
		log.Fatal("Error creating knowledge base manager:", err)
	}
	defer kbManager.Disconnect()

	fmt.Println("starting unit test")

	// Add knowledge bases
	if err := kbManager.AddKB("kb1", "First knowledge base"); err != nil {
		log.Fatal("Error adding kb1:", err)
	}
	if err := kbManager.AddKB("kb2", "Second knowledge base"); err != nil {
		log.Fatal("Error adding kb2:", err)
	}

	// Add nodes
	properties1 := map[string]interface{}{"age": 30}
	data1 := map[string]interface{}{"email": "john@example.com"}
	if err := kbManager.AddNode("kb1", "person", "John Doe", properties1, data1, "people.john"); err != nil {
		log.Fatal("Error adding John Doe node:", err)
	}

	properties2 := map[string]interface{}{"age": 25}
	data2 := map[string]interface{}{"email": "jane@example.com"}
	if err := kbManager.AddNode("kb2", "person", "Jane Smith", properties2, data2, "people.jane"); err != nil {
		log.Fatal("Error adding Jane Smith node:", err)
	}

	// Add link mount
	_, _, err = kbManager.AddLinkMount("kb1", "people.john", "link1", "link1 description")
	if err != nil {
		log.Fatal("Error adding link mount:", err)
	}
	

	// Add link
	if err := kbManager.AddLink("kb1", "people.john", "link1"); err != nil {
		log.Fatal("Error adding link:", err)
	}

	fmt.Println("ending unit test")
	os.Exit(1)
}

*/