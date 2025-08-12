package kb_construct_module

import (
	"os"
	"testing"
)

// Test database configuration
var (
	testDBHost     = "localhost"
	testDBPort     = 5432
	testDBName     = "knowledge_base"
	testDBUser     = "gedgar"
	testDBTable    = "knowledge_base_test" // Use a different table for testing
	testDBPassword = os.Getenv("POSTGRES_PASSWORD")
)

// setupTestDB creates a test instance of ConstructKB
func setupTestDB(t *testing.T) *ConstructKB {
	t.Helper()
	
	// Check if password is set
	if testDBPassword == "" {
		t.Skip("DB_PASSWORD environment variable not set. Set it with: export DB_PASSWORD=yourpassword")
	}

	kb, err := NewConstructKB(testDBHost, testDBPort, testDBName, testDBUser, testDBPassword, testDBTable)
	if err != nil {
		t.Fatalf("Error creating ConstructKB: %v", err)
	}

	return kb
}

// TestConstructKBFullWorkflow tests the complete workflow
func TestConstructKBFullWorkflow(t *testing.T) {
	kb := setupTestDB(t)
	defer kb.Disconnect()

	// Test adding first knowledge base
	t.Run("AddFirstKnowledgeBase", func(t *testing.T) {
		if err := kb.AddKB("kb1", "First knowledge base"); err != nil {
			t.Fatalf("Error adding kb1: %v", err)
		}
	})

	// Test selecting kb1
	t.Run("SelectKB1", func(t *testing.T) {
		if err := kb.SelectKB("kb1"); err != nil {
			t.Fatalf("Error selecting kb1: %v", err)
		}
	})

	// Test adding header node
	t.Run("AddHeaderNode", func(t *testing.T) {
		if err := kb.AddHeaderNode("header1_link", "header1_name",
			map[string]interface{}{"prop1": "val1"},
			map[string]interface{}{"data": "header1_data"}, ""); err != nil {
			t.Fatalf("Error adding header node: %v", err)
		}
	})

	// Test adding info node
	t.Run("AddInfoNode", func(t *testing.T) {
		if err := kb.AddInfoNode("info1_link", "info1_name",
			map[string]interface{}{"prop2": "val2"},
			map[string]interface{}{"data": "info1_data"}, ""); err != nil {
			t.Fatalf("Error adding info node: %v", err)
		}
	})

	// Test leaving header node
	t.Run("LeaveHeaderNode", func(t *testing.T) {
		if err := kb.LeaveHeaderNode("header1_link", "header1_name"); err != nil {
			t.Fatalf("Error leaving header node: %v", err)
		}
	})

	// Test second header node workflow
	t.Run("SecondHeaderNodeWorkflow", func(t *testing.T) {
		// Add second header node
		if err := kb.AddHeaderNode("header2_link", "header2_name",
			map[string]interface{}{"prop3": "val3"},
			map[string]interface{}{"data": "header2_data"}, ""); err != nil {
			t.Fatalf("Error adding header2 node: %v", err)
		}

		// Add info node under header2
		if err := kb.AddInfoNode("info2_link", "info2_name",
			map[string]interface{}{"prop4": "val4"},
			map[string]interface{}{"data": "info2_data"}, ""); err != nil {
			t.Fatalf("Error adding info2 node: %v", err)
		}

		// Add link mount
		if err := kb.AddLinkMount("link1", "link1 description"); err != nil {
			t.Fatalf("Error adding link mount: %v", err)
		}

		// Leave second header node
		if err := kb.LeaveHeaderNode("header2_link", "header2_name"); err != nil {
			t.Fatalf("Error leaving header2 node: %v", err)
		}
	})

	// Test second knowledge base
	t.Run("SecondKnowledgeBase", func(t *testing.T) {
		// Add second knowledge base
		if err := kb.AddKB("kb2", "Second knowledge base"); err != nil {
			t.Fatalf("Error adding kb2: %v", err)
		}

		// Select kb2
		if err := kb.SelectKB("kb2"); err != nil {
			t.Fatalf("Error selecting kb2: %v", err)
		}

		// Add nodes to kb2
		if err := kb.AddHeaderNode("header1_link", "header1_name",
			map[string]interface{}{"prop1": "val1"},
			map[string]interface{}{"data": "header1_data"}, ""); err != nil {
			t.Fatalf("Error adding header node to kb2: %v", err)
		}

		if err := kb.AddInfoNode("info1_link", "info1_name",
			map[string]interface{}{"prop2": "val2"},
			map[string]interface{}{"data": "info1_data"}, ""); err != nil {
			t.Fatalf("Error adding info node to kb2: %v", err)
		}

		if err := kb.LeaveHeaderNode("header1_link", "header1_name"); err != nil {
			t.Fatalf("Error leaving header node in kb2: %v", err)
		}

		if err := kb.AddHeaderNode("header2_link", "header2_name",
			map[string]interface{}{"prop3": "val3"},
			map[string]interface{}{"data": "header2_data"}, ""); err != nil {
			t.Fatalf("Error adding header2 node to kb2: %v", err)
		}

		if err := kb.AddInfoNode("info2_link", "info2_name",
			map[string]interface{}{"prop4": "val4"},
			map[string]interface{}{"data": "info2_data"}, ""); err != nil {
			t.Fatalf("Error adding info2 node to kb2: %v", err)
		}

		// Add link node (different from kb1 which has link mount)
		if err := kb.AddLinkNode("link1"); err != nil {
			t.Fatalf("Error adding link node: %v", err)
		}

		if err := kb.LeaveHeaderNode("header2_link", "header2_name"); err != nil {
			t.Fatalf("Error leaving header2 node in kb2: %v", err)
		}
	})

	// Test installation check
	t.Run("CheckInstallation", func(t *testing.T) {
		if err := kb.CheckInstallation(); err != nil {
			t.Fatalf("Error during installation check: %v", err)
		}
	})
}

// TestAddKBDuplicate tests adding duplicate knowledge base
func TestAddKBDuplicate(t *testing.T) {
	kb := setupTestDB(t)
	defer kb.Disconnect()

	// Add first KB
	if err := kb.AddKB("test_kb", "Test KB"); err != nil {
		t.Fatalf("Error adding first KB: %v", err)
	}

	// Try to add duplicate KB
	err := kb.AddKB("test_kb", "Duplicate KB")
	if err == nil {
		t.Fatal("Expected error when adding duplicate KB, got nil")
	}

	expectedError := "knowledge base test_kb already exists"
	if err.Error() != expectedError {
		t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
	}
}

// TestSelectNonExistentKB tests selecting a non-existent knowledge base
func TestSelectNonExistentKB(t *testing.T) {
	kb := setupTestDB(t)
	defer kb.Disconnect()

	err := kb.SelectKB("non_existent_kb")
	if err == nil {
		t.Fatal("Expected error when selecting non-existent KB, got nil")
	}

	expectedError := "knowledge base non_existent_kb does not exist"
	if err.Error() != expectedError {
		t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
	}
}

// TestLeaveHeaderNodeMismatch tests leaving header node with wrong parameters
func TestLeaveHeaderNodeMismatch(t *testing.T) {
	kb := setupTestDB(t)
	defer kb.Disconnect()

	// Setup
	if err := kb.AddKB("test_kb", "Test KB"); err != nil {
		t.Fatalf("Error adding KB: %v", err)
	}
	if err := kb.SelectKB("test_kb"); err != nil {
		t.Fatalf("Error selecting KB: %v", err)
	}
	if err := kb.AddHeaderNode("link1", "name1", map[string]interface{}{"prop1": "val1"}, map[string]interface{}{"data": "header1_data"}, ""); err != nil {
		t.Fatalf("Error adding header node: %v", err)
	}

	// Test wrong name
	t.Run("WrongName", func(t *testing.T) {
		err := kb.LeaveHeaderNode("link1", "wrong_name")
		if err == nil {
			t.Fatal("Expected error when leaving header node with wrong name")
		}
		if !contains(err.Error(), "expected name 'wrong_name', but got 'name1'") {
			t.Errorf("Unexpected error message: %v", err)
		}
	})

	// Test wrong label (need to add another node first)
	t.Run("WrongLabel", func(t *testing.T) {
		// First, properly leave the current node
		if err := kb.LeaveHeaderNode("link1", "name1"); err != nil {
			t.Fatalf("Error leaving header node: %v", err)
		}
		
		// Add a new node
		if err := kb.AddHeaderNode("link2", "name2", map[string]interface{}{"prop1": "val1"}, map[string]interface{}{"data": "header1_data"}, ""); err != nil {
			t.Fatalf("Error adding header node: %v", err)
		}
		
		// Try to leave with wrong label
		err := kb.LeaveHeaderNode("wrong_link", "name2")
		if err == nil {
			t.Fatal("Expected error when leaving header node with wrong label")
		}
		if !contains(err.Error(), "expected label 'wrong_link', but got 'link2'") {
			t.Errorf("Unexpected error message: %v", err)
		}
	})
}

// TestEmptyPath tests operations on empty path
func TestEmptyPath(t *testing.T) {
	kb := setupTestDB(t)
	defer kb.Disconnect()

	// Try operations without selecting KB
	t.Run("NoKBSelected", func(t *testing.T) {
		err := kb.AddHeaderNode("link", "name", nil, nil, "")
		if err == nil {
			t.Fatal("Expected error when adding node without selecting KB")
		}
	})
}

// TestCheckInstallationFailure tests check installation with non-empty path
func TestCheckInstallationFailure(t *testing.T) {
	kb := setupTestDB(t)
	defer kb.Disconnect()

	// Setup
	if err := kb.AddKB("test_kb", "Test KB"); err != nil {
		t.Fatalf("Error adding KB: %v", err)
	}
	if err := kb.SelectKB("test_kb"); err != nil {
		t.Fatalf("Error selecting KB: %v", err)
	}
	if err := kb.AddHeaderNode("link1", "name1", map[string]interface{}{"prop1": "val1"}, map[string]interface{}{"data": "header1_data"}, ""); err != nil {
		t.Fatalf("Error adding header node: %v", err)
	}

	// Don't leave the header node - path should not be empty
	err := kb.CheckInstallation()
	if err == nil {
		t.Fatal("Expected error during installation check with non-empty path")
	}
	if !contains(err.Error(), "installation check failed: path is not empty") {
		t.Errorf("Unexpected error message: %v", err)
	}
}

// Helper function to check if string contains substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && s[:len(substr)] == substr ||
		len(s) >= len(substr) && s[len(s)-len(substr):] == substr ||
		len(s) > len(substr) && containsMiddle(s, substr)
}

func containsMiddle(s, substr string) bool {
	for i := 1; i < len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

