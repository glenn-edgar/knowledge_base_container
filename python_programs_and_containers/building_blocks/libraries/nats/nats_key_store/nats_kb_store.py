#!/usr/bin/env python3
"""
NatsKbStore: Knowledge Base extension for KeyStore

Extends the NATS KeyStore with specialized knowledge base operations
for storing and retrieving structured label and node data.
"""

import re
from typing import Dict, List, Any, Optional, Union
from .nats_key_store import KeyStore, KeyStoreConfig
from nats.js.errors import NoKeysError


class NatsKbStore(KeyStore):
    """
    Knowledge Base store extending KeyStore for structured KB operations.
    
    Stores knowledge base entries as combinations of labels and nodes,
    with validation and specialized retrieval methods.
    """
    
    def __init__(self, server: str, bucket: str, description: str):
        """
        Initialize NatsKbStore.
        
        Args:
            server: NATS server URL
            bucket: KV bucket name for knowledge base
            description: Description for the bucket
        """
        config = KeyStoreConfig(
            server=server,
            bucket=bucket,
            description=description,
            create_bucket=True
        )
        super().__init__(config)
        
    
    def _validate_topic(self, base_topic: str) -> None:
        """
        Validate base topic format.
        
        Args:
            base_topic: The base topic string to validate
            
        Raises:
            ValueError: If topic format is invalid
        """
        if not isinstance(base_topic, str):
            raise ValueError("base_topic must be a string")
        
        if not base_topic:
            raise ValueError("base_topic cannot be empty")
        
        # Check for valid topic format (alphanumeric, dots, underscores, hyphens)
        if not re.match(r'^[a-zA-Z0-9._-]+$', base_topic):
            raise ValueError(
                "base_topic must contain only alphanumeric characters, dots, underscores, and hyphens"
            )
        
        # Ensure it doesn't start or end with a dot
        if base_topic.startswith('.') or base_topic.endswith('.'):
            raise ValueError("base_topic cannot start or end with a dot")
        
        # Check for consecutive dots
        if '..' in base_topic:
            raise ValueError("base_topic cannot contain consecutive dots")
    
    def _validate_label_name(self, label_name: str) -> None:
        """
        Validate label name format.
        
        Args:
            label_name: The label name to validate
            
        Raises:
            ValueError: If label name format is invalid
        """
        if not isinstance(label_name, str):
            raise ValueError("label_name must be a string")
        
        if not label_name:
            raise ValueError("label_name cannot be empty")
        
        # Label names should be valid identifiers
        if not re.match(r'^[a-zA-Z0-9_-]+$', label_name):
            raise ValueError(
                "label_name must contain only alphanumeric characters, underscores, and hyphens"
            )
        
        if len(label_name) > 100:
            raise ValueError("label_name must be 100 characters or less")
    
    def _validate_node_name(self, node_name: str) -> None:
        """
        Validate node name format.
        
        Args:
            node_name: The node name to validate
            
        Raises:
            ValueError: If node name format is invalid
        """
        if not isinstance(node_name, str):
            raise ValueError("node_name must be a string")
        
        if not node_name:
            raise ValueError("node_name cannot be empty")
        
        # Node names should be valid identifiers
        if not re.match(r'^[a-zA-Z0-9_.-]+$', node_name):
            raise ValueError(
                "node_name must contain only alphanumeric characters, underscores, dots, and hyphens"
            )
        
        if len(node_name) > 100:
            raise ValueError("node_name must be 100 characters or less")
    
    def _validate_label_dict(self, label_dict: dict) -> None:
        """
        Validate label dictionary structure.
        
        Args:
            label_dict: The label dictionary to validate
            
        Raises:
            ValueError: If label dictionary is invalid
        """
        if not isinstance(label_dict, dict):
            raise ValueError("label_dict must be a dictionary")
        
        if not label_dict:
            raise ValueError("label_dict cannot be empty")
        
        # Validate that all keys are strings
        for key in label_dict.keys():
            if not isinstance(key, str):
                raise ValueError("All label_dict keys must be strings")
        
        # Check for required fields (customize based on your requirements)
        required_fields = ["type", "description"]
        for field in required_fields:
            if field not in label_dict:
                raise ValueError(f"label_dict must contain required field: {field}")
    
    def _validate_node_dict(self, node_dict: dict) -> None:
        """
        Validate node dictionary structure.
        
        Args:
            node_dict: The node dictionary to validate
            
        Raises:
            ValueError: If node dictionary is invalid
        """
        if not isinstance(node_dict, dict):
            raise ValueError("node_dict must be a dictionary")
        
        if not node_dict:
            raise ValueError("node_dict cannot be empty")
        
        # Validate that all keys are strings
        for key in node_dict.keys():
            if not isinstance(key, str):
                raise ValueError("All node_dict keys must be strings")
        
        # Check for required fields (customize based on your requirements)
        required_fields = ["id", "data"]
        for field in required_fields:
            if field not in node_dict:
                raise ValueError(f"node_dict must contain required field: {field}")
    
    def _validate_composite_node(self, composite_node: Any) -> None:
        """
        Validate composite_node is a boolean.
        
        Args:
            composite_node: The value to validate
            
        Raises:
            ValueError: If not a boolean
        """
        if not isinstance(composite_node, bool):
            raise ValueError("composite_node must be a boolean")
    
    def store_kb_key(self, 
                    base_topic: str, 
                    label_name: str, 
                    node_name: str, 
                    label_dict: dict, 
                    node_dict: dict, 
                    composite_node: bool = False) -> str:
        """
        Store a knowledge base key with label and node data.
        
        Args:
            base_topic: Base topic for the KB entry
            label_name: Name of the label
            node_name: Name of the node
            label_dict: Dictionary containing label data
            node_dict: Dictionary containing node data
            composite_node: Whether this is a composite node
            
        Returns:
            The full KB key if composite_node is True, otherwise base_topic
            
        Raises:
            ValueError: If any validation fails
        """
        # Validate all inputs
        self._validate_composite_node(composite_node)
        self._validate_topic(base_topic)
        self._validate_label_name(label_name)
        self._validate_node_name(node_name)
        self._validate_label_dict(label_dict)
        self._validate_node_dict(node_dict)
        
        # Construct the KB key
        kb_key = f"{base_topic}.{label_name}.{node_name}"
        
        # Store the data as a list of two dictionaries
        payload = [label_dict, node_dict]
        
        # Use sync version for simplicity (can be made async if needed)
        self.put_sync(kb_key, payload)
        
        # Return appropriate key based on composite_node flag
        if composite_node:
            return kb_key
        else:
            return base_topic
    
    def get_kb_key(self, kb_key: str) -> Dict[str, dict]:
        """
        Retrieve knowledge base data for a given key.
        
        Args:
            kb_key: The KB key to retrieve
            
        Returns:
            Dictionary with 'label' and 'node' keys containing the respective data
            
        Raises:
            ValueError: If key not found or data is invalid
        """
        if not isinstance(kb_key, str) or not kb_key:
            raise ValueError("kb_key must be a non-empty string")
        
        # Retrieve the payload
        payload = self.get_sync(kb_key)
        
        if payload is None:
            raise ValueError(f"KB key {kb_key} not found")
        
        # Verify that the payload is a list of two elements
        if not isinstance(payload, list) or len(payload) != 2:
            raise ValueError(f"KB key {kb_key} is not a valid KB key - expected list of 2 elements")
        
        # Verify both elements are dictionaries
        if not isinstance(payload[0], dict) or not isinstance(payload[1], dict):
            raise ValueError(f"KB key {kb_key} is not a valid KB key - elements must be dictionaries")
        
        return {
            "label": payload[0],
            "node": payload[1]
        }
    
    def delete_kb_key(self, kb_key: str) -> None:
        """
        Delete a knowledge base key.
        
        Args:
            kb_key: The KB key to delete
            
        Raises:
            ValueError: If key format is invalid
        """
        if not isinstance(kb_key, str) or not kb_key:
            raise ValueError("kb_key must be a non-empty string")
        
        self.delete_sync(kb_key)
    
    def pop_kb_key(self, kb_key: str) -> str:
        """
        Remove the last two segments from a KB key path.
        
        This method truncates the kb_key by removing the last two dot-separated
        segments, effectively "popping" the label and node components.
        
        Args:
            kb_key: The KB key to truncate
            
        Returns:
            Truncated string with last two segments removed
            
        Raises:
            ValueError: If key format is invalid or insufficient segments
        """
        if not isinstance(kb_key, str) or not kb_key:
            raise ValueError("kb_key must be a non-empty string")
        
        # Split the key by dots
        segments = kb_key.split('.')
        
        # Ensure we have at least 3 segments (base_topic.label.node)
        if len(segments) < 3:
            raise ValueError(f"kb_key must have at least 3 dot-separated segments, got {len(segments)}")
        
        # Remove the last two segments (label and node)
        truncated_segments = segments[:-2]
        truncated_string = '.'.join(truncated_segments)
        
        if not truncated_string:
            raise ValueError("Truncation would result in empty string")
        
        return truncated_string
    
    def list_kb_keys(self, base_topic: Optional[str] = None) -> List[str]:
        """
        List all KB keys, optionally filtered by base topic.
        
        Args:
            base_topic: Optional base topic to filter by
            
        Returns:
            List of KB key strings
        """
        if base_topic:
            self._validate_topic(base_topic)
            pattern = f"{base_topic}.*"
        else:
            pattern = None
        
        try:
            return self.keys_sync(pattern)
        except Exception as e:
            # Handle the case where keys_sync might still raise exceptions
            if "no keys found" in str(e).lower() or "NoKeysError" in str(type(e).__name__):
                return []
            raise e
    
    def get_kb_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the knowledge base.
        
        Returns:
            Dictionary with KB statistics
        """
        try:
            all_keys = self.keys_sync()
        except Exception as e:
            # Handle the case where keys_sync might still raise exceptions
            if "no keys found" in str(e).lower() or "NoKeysError" in str(type(e).__name__):
                all_keys = []
            else:
                raise e
        
        kb_keys = [key for key in all_keys if len(key.split('.')) >= 3]
        
        # Group by base topic
        topics = {}
        for key in kb_keys:
            try:
                base_topic = self.pop_kb_key(key)
                if base_topic not in topics:
                    topics[base_topic] = 0
                topics[base_topic] += 1
            except ValueError:
                # Skip keys that don't follow KB format
                continue
        
        return {
            "total_kb_keys": len(kb_keys),
            "total_topics": len(topics),
            "keys_per_topic": topics,
            "all_keys_count": len(all_keys)
        }
    
    def validate_kb_key_format(self, kb_key: str) -> bool:
        """
        Validate that a KB key follows the expected format.
        
        Args:
            kb_key: The key to validate
            
        Returns:
            True if valid format, False otherwise
        """
        try:
            if not isinstance(kb_key, str) or not kb_key:
                return False
            
            segments = kb_key.split('.')
            if len(segments) < 3:
                return False
            
            # Reconstruct and validate each component
            base_topic = '.'.join(segments[:-2])
            label_name = segments[-2]
            node_name = segments[-1]
            
            self._validate_topic(base_topic)
            self._validate_label_name(label_name)
            self._validate_node_name(node_name)
            
            return True
        except ValueError:
            return False
        
    def disconnect_sync(self) -> None:
        """Synchronous version of disconnect."""
        return self._run_async(self.disconnect())

# Example usage and demonstration
if __name__ == "__main__":
    import json
    
    def demo_nats_kb_store():
        """Demonstrate NatsKbStore functionality."""
        print("NatsKbStore Demo")
        print("=" * 50)
        
        # Create KB store
        kb_store = NatsKbStore(
            server="nats://127.0.0.1:4222",
            bucket="knowledge_base",
            description="Knowledge Base Store"
        )
        
        print("\n1. Store KB entries:")
        
        # Example label and node dictionaries
        label_dict = {
            "type": "entity",
            "description": "Person entity with attributes",
            "category": "human",
            "version": "1.0"
        }
        
        node_dict = {
            "id": "person_001",
            "data": {
                "name": "Alice Johnson",
                "age": 30,
                "occupation": "Software Engineer",
                "skills": ["Python", "Machine Learning", "Data Analysis"]
            },
            "metadata": {
                "created": "2024-01-15",
                "confidence": 0.95
            }
        }
        
        try:
            # Store as composite node
            kb_key = kb_store.store_kb_key(
                base_topic="company.employees",
                label_name="person",
                node_name="alice_johnson",
                label_dict=label_dict,
                node_dict=node_dict,
                composite_node=True
            )
            print(f"   Stored composite node: {kb_key}")
            
            # Store as simple topic
            simple_key = kb_store.store_kb_key(
                base_topic="company.departments.engineering",
                label_name="team",
                node_name="backend_team",
                label_dict={
                    "type": "organizational_unit",
                    "description": "Backend development team"
                },
                node_dict={
                    "id": "team_001",
                    "data": {
                        "members": ["alice", "bob", "charlie"],
                        "tech_stack": ["Python", "PostgreSQL", "Redis"]
                    }
                },
                composite_node=False
            )
            print(f"   Stored simple topic: {simple_key}")
            
        except Exception as e:
            print(f"   Error storing: {e}")
        
        print("\n2. Retrieve KB entries:")
        try:
            data = kb_store.get_kb_key(kb_key)
            print(f"   Retrieved label type: {data['label']['type']}")
            print(f"   Retrieved node name: {data['node']['data']['name']}")
            print(f"   Retrieved node skills: {data['node']['data']['skills']}")
            
        except Exception as e:
            print(f"   Error retrieving: {e}")
        
        print("\n3. Test pop_kb_key:")
        try:
            original_key = "company.employees.person.alice_johnson"
            popped_key = kb_store.pop_kb_key(original_key)
            print(f"   Original: {original_key}")
            print(f"   Popped:   {popped_key}")
            
        except Exception as e:
            print(f"   Error popping: {e}")
        
        print("\n4. List KB keys:")
        try:
            all_keys = kb_store.list_kb_keys()
            print(f"   All KB keys: {all_keys}")
            
            company_keys = kb_store.list_kb_keys("company")
            print(f"   Company keys: {company_keys}")
            
        except Exception as e:
            print(f"   Error listing: {e}")
        
        print("\n5. KB Statistics:")
        try:
            stats = kb_store.get_kb_stats()
            print(f"   Total KB keys: {stats['total_kb_keys']}")
            print(f"   Topics: {stats['total_topics']}")
            for topic, count in stats['keys_per_topic'].items():
                print(f"      {topic}: {count} keys")
                
        except Exception as e:
            print(f"   Error getting stats: {e}")
        
        print("\n6. Validation tests:")
        test_cases = [
            "valid.topic.label.node",
            "invalid",  # too short
            "also.invalid.only.two",  # only 2 segments after split
            "",  # empty
            "valid.multi.segment.topic.label.node"  # longer topic
        ]
        
        for test_key in test_cases:
            is_valid = kb_store.validate_kb_key_format(test_key)
            print(f"   '{test_key}' -> {is_valid}")
        
        print("\n7. Cleanup:")
        try:
            # Clean up test data
            all_keys = kb_store.list_kb_keys()
            for key in all_keys:
                kb_store.delete_kb_key(key)
            print(f"   Deleted {len(all_keys)} KB keys")
            
        except Exception as e:
            print(f"   Error cleaning up: {e}")
        
        print("\n" + "=" * 50)
    
    # Run demo if NATS server is available
    print("\n⚠️  Make sure NATS server is running at 127.0.0.1:4222")
    print("   You can start it with: docker run -p 4222:4222 nats:latest -js\n")
    
    try:
        demo_nats_kb_store()
    except Exception as e:
        print(f"Demo failed: {e}")
        print("Make sure NATS server is running and accessible.")