package kb_memory_module

import (
	"fmt"
	//"log"
	"strings"
)

// SearchMemDB extends BasicConstructDB with search and filtering capabilities
type SearchMemDB struct {
	*BasicConstructDB                    // Embedded struct for inheritance-like behavior
	keys            map[string][]string  // Generated decoded keys
	kbs             map[string][]string  // Knowledge bases mapping
	labels          map[string][]string  // Labels mapping
	names           map[string][]string  // Names mapping
	DecodedKeys     map[string][]string  // Decoded path keys
	FilterResults   map[string]*TreeNode // Current filter results
}

// NewSearchMemDB creates a new SearchMemDB instance and loads data from PostgreSQL
func NewSearchMemDB(host string, port int, dbname, user, password, tableName string) (*SearchMemDB, error) {
	smdb := &SearchMemDB{
		BasicConstructDB: NewBasicConstructDB(host, port, dbname, user, password, tableName),
		kbs:              make(map[string][]string),
		labels:           make(map[string][]string),
		names:            make(map[string][]string),
		DecodedKeys:      make(map[string][]string),
		FilterResults:    make(map[string]*TreeNode),
	}

	// Import data from PostgreSQL
	_, err := smdb.ImportFromPostgres(tableName, "path", "data", "created_at", "updated_at")
	if err != nil {
		return nil, fmt.Errorf("failed to import from postgres: %w", err)
	}

	// Generate decoded keys
	smdb.keys = smdb.generateDecodedKeys(smdb.data)
	
	// Initialize filter results with all data
	smdb.ClearFilters()

	return smdb, nil
}

// generateDecodedKeys processes the data and creates lookup maps
func (smdb *SearchMemDB) generateDecodedKeys(data map[string]*TreeNode) map[string][]string {
	smdb.kbs = make(map[string][]string)
	smdb.labels = make(map[string][]string)
	smdb.names = make(map[string][]string)
	smdb.DecodedKeys = make(map[string][]string)

	for key := range data {
		// Split the key into components
		smdb.DecodedKeys[key] = strings.Split(key, ".")
		
		if len(smdb.DecodedKeys[key]) < 3 {
			// Skip keys that don't have at least kb.label.name structure
			continue
		}

		kb := smdb.DecodedKeys[key][0]
		label := smdb.DecodedKeys[key][len(smdb.DecodedKeys[key])-2]
		name := smdb.DecodedKeys[key][len(smdb.DecodedKeys[key])-1]

		// Add to knowledge bases map
		if _, exists := smdb.kbs[kb]; !exists {
			smdb.kbs[kb] = make([]string, 0)
		}
		smdb.kbs[kb] = append(smdb.kbs[kb], key)

		// Add to labels map
		if _, exists := smdb.labels[label]; !exists {
			smdb.labels[label] = make([]string, 0)
		}
		smdb.labels[label] = append(smdb.labels[label], key)

		// Add to names map
		if _, exists := smdb.names[name]; !exists {
			smdb.names[name] = make([]string, 0)
		}
		smdb.names[name] = append(smdb.names[name], key)
	}

	return smdb.DecodedKeys
}

// ClearFilters clears all filters and resets the query state
func (smdb *SearchMemDB) ClearFilters() {
	smdb.FilterResults = make(map[string]*TreeNode)
	// Copy all data to filter results
	for key, value := range smdb.data {
		smdb.FilterResults[key] = value
	}
}

// SearchKB searches for rows matching the specified knowledge base
func (smdb *SearchMemDB) SearchKB(knowledgeBase string) map[string]*TreeNode {
	newFilterResults := make(map[string]*TreeNode)
	
	if kbKeys, exists := smdb.kbs[knowledgeBase]; exists {
		for _, key := range kbKeys {
			if _, exists := smdb.FilterResults[key]; exists {
				newFilterResults[key] = smdb.FilterResults[key]
			}
		}
	}
	
	smdb.FilterResults = newFilterResults
	return smdb.FilterResults
}

// SearchLabel searches for rows matching the specified label
func (smdb *SearchMemDB) SearchLabel(label string) map[string]*TreeNode {
	newFilterResults := make(map[string]*TreeNode)
	
	if labelKeys, exists := smdb.labels[label]; exists {
		for _, key := range labelKeys {
			if _, exists := smdb.FilterResults[key]; exists {
				newFilterResults[key] = smdb.FilterResults[key]
			}
		}
	}
	
	smdb.FilterResults = newFilterResults
	return smdb.FilterResults
}

// SearchName searches for rows matching the specified name
func (smdb *SearchMemDB) SearchName(name string) map[string]*TreeNode {
	newFilterResults := make(map[string]*TreeNode)
	
	if nameKeys, exists := smdb.names[name]; exists {
		for _, key := range nameKeys {
			if _, exists := smdb.FilterResults[key]; exists {
				newFilterResults[key] = smdb.FilterResults[key]
			}
		}
	}
	
	smdb.FilterResults = newFilterResults
	return smdb.FilterResults
}

// SearchPropertyKey searches for rows that contain the specified property key
func (smdb *SearchMemDB) SearchPropertyKey(dataKey string) map[string]*TreeNode {
	newFilterResults := make(map[string]*TreeNode)
	
	for key := range smdb.FilterResults {
		if node, exists := smdb.data[key]; exists {
			if dataMap, ok := node.Data.(map[string]interface{}); ok {
				if _, hasKey := dataMap[dataKey]; hasKey {
					newFilterResults[key] = smdb.FilterResults[key]
				}
			}
		}
	}
	
	smdb.FilterResults = newFilterResults
	return smdb.FilterResults
}

// SearchPropertyValue searches for rows where the properties JSON field contains the specified key with the specified value
func (smdb *SearchMemDB) SearchPropertyValue(dataKey string, dataValue interface{}) map[string]*TreeNode {
	newFilterResults := make(map[string]*TreeNode)
	
	for key := range smdb.FilterResults {
		if node, exists := smdb.data[key]; exists {
			if dataMap, ok := node.Data.(map[string]interface{}); ok {
				if value, hasKey := dataMap[dataKey]; hasKey {
					if value == dataValue {
						newFilterResults[key] = smdb.FilterResults[key]
					}
				}
			}
		}
	}
	
	smdb.FilterResults = newFilterResults
	return smdb.FilterResults
}

// SearchStartingPath searches for a specific path and all its descendants
func (smdb *SearchMemDB) SearchStartingPath(startingPath string) (map[string]*TreeNode, error) {
	newFilterResults := make(map[string]*TreeNode)
	
	// Add starting path if it exists in filter results
	if _, exists := smdb.FilterResults[startingPath]; exists {
		newFilterResults[startingPath] = smdb.FilterResults[startingPath]
	} else {
		// If starting path doesn't exist, clear filter results
		smdb.FilterResults = make(map[string]*TreeNode)
		return newFilterResults, nil
	}
	
	// Get and add descendants
	descendants, err := smdb.QueryDescendants(startingPath)
	if err != nil {
		return nil, fmt.Errorf("error querying descendants: %w", err)
	}
	
	for _, item := range descendants {
		if _, exists := smdb.FilterResults[item.Path]; exists {
			newFilterResults[item.Path] = smdb.FilterResults[item.Path]
		}
	}
	
	smdb.FilterResults = newFilterResults
	return newFilterResults, nil
}

// SearchPath searches for rows matching the specified LTREE path expression using operators
func (smdb *SearchMemDB) SearchPath(operator, startingPath string) map[string]*TreeNode {
	// Use the parent class query method
	searchResults := smdb.QueryByOperator(operator, startingPath, "")
	
	newFilterResults := make(map[string]*TreeNode)
	for _, item := range searchResults {
		if _, exists := smdb.FilterResults[item.Path]; exists {
			newFilterResults[item.Path] = smdb.FilterResults[item.Path]
		}
	}
	
	smdb.FilterResults = newFilterResults
	return smdb.FilterResults
}

// FindDescriptions extracts descriptions from all data entries or a specific key
func (smdb *SearchMemDB) FindDescriptions(key interface{}) map[string]string {
	returnValues := make(map[string]string)
	
	// Process all data entries
	for rowKey, rowData := range smdb.data {
		if dataMap, ok := rowData.Data.(map[string]interface{}); ok {
			if description, exists := dataMap["description"]; exists {
				if descStr, ok := description.(string); ok {
					returnValues[rowKey] = descStr
				} else {
					returnValues[rowKey] = ""
				}
			} else {
				returnValues[rowKey] = ""
			}
		} else {
			returnValues[rowKey] = ""
		}
	}
	
	return returnValues
}

// GetFilterResults returns the current filter results
func (smdb *SearchMemDB) GetFilterResults() map[string]*TreeNode {
	// Return a copy to prevent external modification
	results := make(map[string]*TreeNode)
	for key, value := range smdb.FilterResults {
		results[key] = value
	}
	return results
}

// GetFilterResultKeys returns just the keys of current filter results
func (smdb *SearchMemDB) GetFilterResultKeys() []string {
	keys := make([]string, 0, len(smdb.FilterResults))
	for key := range smdb.FilterResults {
		keys = append(keys, key)
	}
	return keys
}

// GetKBs returns all knowledge bases
func (smdb *SearchMemDB) GetKBs() map[string][]string {
	return smdb.kbs
}

// GetLabels returns all labels
func (smdb *SearchMemDB) GetLabels() map[string][]string {
	return smdb.labels
}

// GetNames returns all names
func (smdb *SearchMemDB) GetNames() map[string][]string {
	return smdb.names
}

// GetDecodedKeys returns all decoded keys
func (smdb *SearchMemDB) GetDecodedKeys() map[string][]string {
	return smdb.DecodedKeys
}

