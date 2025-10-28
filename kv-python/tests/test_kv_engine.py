"""Tests for the KV Python engine."""

import pytest
import tempfile
import os
from pathlib import Path

try:
    from kv_python import PyKVEngine
except ImportError:
    pytest.skip("kv_python not available", allow_module_level=True)


class TestPyKVEngine:
    """Test cases for PyKVEngine."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir

    @pytest.fixture
    def kv_engine(self, temp_dir):
        """Create a KV engine instance for testing."""
        master_key = "Y2hhbmdlLXRoaXMtYmFzZTY0LWtleQ=="  # Valid base64 key
        engine = PyKVEngine(
            master_key=master_key,
            persistence_mode="memory",
            data_dir=temp_dir
        )
        return engine

    def test_engine_creation(self, kv_engine):
        """Test that the engine can be created."""
        assert kv_engine is not None

    def test_basic_set_get(self, kv_engine):
        """Test basic set and get operations."""
        # Test string value
        kv_engine.set(0, "test_key", "test_value", None)
        result = kv_engine.get(0, "test_key")
        assert result == "test_value"

    def test_get_nonexistent_key(self, kv_engine):
        """Test getting a non-existent key returns None."""
        result = kv_engine.get(0, "nonexistent_key")
        assert result is None

    def test_set_with_ttl(self, kv_engine):
        """Test setting a key with TTL."""
        kv_engine.set(0, "ttl_key", "ttl_value", 3600)  # 1 hour TTL
        result = kv_engine.get(0, "ttl_key")
        assert result == "ttl_value"

    def test_delete_key(self, kv_engine):
        """Test deleting a key."""
        kv_engine.set(0, "delete_key", "delete_value", None)
        result = kv_engine.delete(0, "delete_key")
        assert result is True
        
        # Verify key is deleted
        result = kv_engine.get(0, "delete_key")
        assert result is None

    def test_delete_nonexistent_key(self, kv_engine):
        """Test deleting a non-existent key returns False."""
        result = kv_engine.delete(0, "nonexistent_key")
        assert result is False

    def test_exists_key(self, kv_engine):
        """Test checking if a key exists."""
        kv_engine.set(0, "exists_key", "exists_value", None)
        assert kv_engine.exists(0, "exists_key") is True
        assert kv_engine.exists(0, "nonexistent_key") is False

    def test_keys_operation(self, kv_engine):
        """Test getting all keys in a database."""
        # Add some keys
        kv_engine.set(0, "key1", "value1", None)
        kv_engine.set(0, "key2", "value2", None)
        kv_engine.set(0, "key3", "value3", None)
        
        keys = kv_engine.keys(0)
        assert len(keys) == 3
        assert "key1" in keys
        assert "key2" in keys
        assert "key3" in keys

    def test_clear_database(self, kv_engine):
        """Test clearing a database."""
        # Add some keys
        kv_engine.set(0, "key1", "value1", None)
        kv_engine.set(0, "key2", "value2", None)
        
        # Clear database
        kv_engine.clear_database(0)
        
        # Verify keys are gone
        keys = kv_engine.keys(0)
        assert len(keys) == 0

    def test_multiple_databases(self, kv_engine):
        """Test operations across multiple databases."""
        # Set keys in different databases
        kv_engine.set(0, "key0", "value0", None)
        kv_engine.set(1, "key1", "value1", None)
        kv_engine.set(2, "key2", "value2", None)
        
        # Verify keys are isolated
        assert kv_engine.get(0, "key0") == "value0"
        assert kv_engine.get(1, "key1") == "value1"
        assert kv_engine.get(2, "key2") == "value2"
        
        # Verify cross-database access returns None
        assert kv_engine.get(0, "key1") is None
        assert kv_engine.get(1, "key2") is None
        assert kv_engine.get(2, "key0") is None

    def test_unicode_strings(self, kv_engine):
        """Test handling of Unicode strings."""
        unicode_key = "测试键"
        unicode_value = "测试值"
        
        kv_engine.set(0, unicode_key, unicode_value, None)
        result = kv_engine.get(0, unicode_key)
        assert result == unicode_value

    def test_empty_strings(self, kv_engine):
        """Test handling of empty strings."""
        kv_engine.set(0, "", "empty_key_value", None)
        kv_engine.set(0, "empty_value_key", "", None)
        
        assert kv_engine.get(0, "") == "empty_key_value"
        assert kv_engine.get(0, "empty_value_key") == ""

    def test_large_values(self, kv_engine):
        """Test handling of large values."""
        large_value = "x" * 10000  # 10KB string
        kv_engine.set(0, "large_key", large_value, None)
        result = kv_engine.get(0, "large_key")
        assert result == large_value

    def test_special_characters(self, kv_engine):
        """Test handling of special characters in keys and values."""
        special_key = "key!@#$%^&*()_+-=[]{}|;':\",./<>?"
        special_value = "value!@#$%^&*()_+-=[]{}|;':\",./<>?"
        
        kv_engine.set(0, special_key, special_value, None)
        result = kv_engine.get(0, special_key)
        assert result == special_value

    def test_numeric_strings(self, kv_engine):
        """Test handling of numeric strings."""
        kv_engine.set(0, "123", "456", None)
        result = kv_engine.get(0, "123")
        assert result == "456"

    def test_boolean_strings(self, kv_engine):
        """Test handling of boolean-like strings."""
        kv_engine.set(0, "true_key", "true", None)
        kv_engine.set(0, "false_key", "false", None)
        
        assert kv_engine.get(0, "true_key") == "true"
        assert kv_engine.get(0, "false_key") == "false"

    def test_json_like_strings(self, kv_engine):
        """Test handling of JSON-like strings."""
        json_value = '{"name": "test", "value": 123, "active": true}'
        kv_engine.set(0, "json_key", json_value, None)
        result = kv_engine.get(0, "json_key")
        assert result == json_value

    def test_concurrent_operations(self, kv_engine):
        """Test basic concurrent-like operations."""
        # Simulate concurrent operations by doing many rapid operations
        for i in range(100):
            kv_engine.set(0, f"concurrent_key_{i}", f"concurrent_value_{i}", None)
        
        # Verify all keys exist
        for i in range(100):
            result = kv_engine.get(0, f"concurrent_key_{i}")
            assert result == f"concurrent_value_{i}"

    def test_mixed_operations(self, kv_engine):
        """Test mixed operations on the same key."""
        key = "mixed_key"
        
        # Set initial value
        kv_engine.set(0, key, "initial", None)
        assert kv_engine.get(0, key) == "initial"
        
        # Update value
        kv_engine.set(0, key, "updated", None)
        assert kv_engine.get(0, key) == "updated"
        
        # Set with TTL
        kv_engine.set(0, key, "with_ttl", 3600)
        assert kv_engine.get(0, key) == "with_ttl"
        
        # Delete
        kv_engine.delete(0, key)
        assert kv_engine.get(0, key) is None
        assert kv_engine.exists(0, key) is False

    def test_engine_cleanup(self, kv_engine):
        """Test that the engine can be properly cleaned up."""
        # Add some data
        kv_engine.set(0, "cleanup_key", "cleanup_value", None)
        
        # Close the engine (if close method exists)
        if hasattr(kv_engine, 'close'):
            kv_engine.close()
        
        # This test mainly ensures no panics occur during cleanup
        assert True
