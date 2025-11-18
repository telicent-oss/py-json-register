# Copyright TELICENT LTD
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for JSON canonicalisation.

These tests mirror the canonicalisation tests from the Rust implementation
to ensure consistent behavior across both libraries.
"""

import pytest

from json_register._canonicalise import canonicalise_json
from json_register.exceptions import CanonicalisationError


def test_canonicalise_simple_object():
    """Test canonicalisation of a simple object."""
    obj = {"b": 2, "a": 1}
    result = canonicalise_json(obj)
    assert result == '{"a":1,"b":2}'


def test_canonicalise_different_key_order_same_result():
    """Test that different key orders produce the same canonical form."""
    obj1 = {"name": "Alice", "age": 30}
    obj2 = {"age": 30, "name": "Alice"}
    assert canonicalise_json(obj1) == canonicalise_json(obj2)


def test_canonicalise_nested_object():
    """Test canonicalisation of nested objects."""
    obj = {"outer": {"b": 2, "a": 1}}
    result = canonicalise_json(obj)
    assert result == '{"outer":{"a":1,"b":2}}'


def test_canonicalise_deeply_nested():
    """Test canonicalisation of deeply nested structures."""
    obj = {"level1": {"level2": {"level3": {"level4": {"d": 4, "c": 3, "b": 2, "a": 1}}}}}
    result = canonicalise_json(obj)
    assert '"a":1,"b":2,"c":3,"d":4' in result
    # Verify all levels are properly nested and sorted
    assert result == '{"level1":{"level2":{"level3":{"level4":{"a":1,"b":2,"c":3,"d":4}}}}}'


def test_canonicalise_array_order_preserved():
    """Test that array order is preserved."""
    obj = {"items": [3, 1, 2]}
    result = canonicalise_json(obj)
    assert result == '{"items":[3,1,2]}'


def test_canonicalise_array_with_objects():
    """Test canonicalisation of arrays containing objects."""
    obj = {"users": [{"name": "Bob", "age": 25}, {"name": "Alice", "age": 30}]}
    result = canonicalise_json(obj)
    # Array order preserved, but object keys sorted
    assert result == '{"users":[{"age":25,"name":"Bob"},{"age":30,"name":"Alice"}]}'


def test_canonicalise_primitives():
    """Test canonicalisation of primitive types."""
    assert canonicalise_json("hello") == '"hello"'
    assert canonicalise_json(42) == "42"
    assert canonicalise_json(3.14) == "3.14"
    assert canonicalise_json(True) == "true"
    assert canonicalise_json(False) == "false"
    assert canonicalise_json(None) == "null"


def test_canonicalise_empty_structures():
    """Test canonicalisation of empty structures."""
    assert canonicalise_json({}) == "{}"
    assert canonicalise_json([]) == "[]"


def test_canonicalise_whitespace_variations():
    """Test that different whitespace produces same canonical form."""
    obj1 = {"a": 1, "b": 2}
    obj2 = {"a": 1, "b": 2}  # No spaces

    # Both should produce identical canonical form (no whitespace)
    canonical1 = canonicalise_json(obj1)
    canonical2 = canonicalise_json(obj2)

    assert canonical1 == canonical2
    assert canonical1 == '{"a":1,"b":2}'


def test_canonicalise_number_formatting():
    """Test canonicalisation of various number formats."""
    # Integers
    assert canonicalise_json(42) == "42"
    assert canonicalise_json(0) == "0"
    assert canonicalise_json(-10) == "-10"

    # Floats
    assert canonicalise_json(3.14) == "3.14"
    assert canonicalise_json(0.0) == "0.0"
    assert canonicalise_json(-2.5) == "-2.5"

    # Scientific notation
    assert canonicalise_json(1e10) == "10000000000.0"


def test_canonicalise_unicode():
    """Test canonicalisation preserves Unicode characters."""
    obj = {"russian": "Алиса", "emoji": "🎉", "chinese": "你好", "arabic": "مرحبا"}
    result = canonicalise_json(obj)

    # All Unicode should be preserved (not escaped)
    assert "Алиса" in result
    assert "🎉" in result
    assert "你好" in result
    assert "مرحبا" in result


def test_canonicalise_special_characters():
    """Test canonicalisation of special characters."""
    obj = {
        "quote": 'He said "hello"',
        "newline": "line1\nline2",
        "tab": "col1\tcol2",
        "backslash": "path\\to\\file",
    }
    result = canonicalise_json(obj)

    # Special characters should be properly escaped
    assert r'"He said \"hello\""' in result or '"He said \\"hello\\""' in result
    assert r"\n" in result
    assert r"\t" in result
    assert r"\\" in result or "\\\\" in result


def test_canonicalise_mixed_types():
    """Test canonicalisation with mixed types in same structure."""
    obj = {
        "string": "hello",
        "number": 42,
        "float": 3.14,
        "bool": True,
        "null": None,
        "array": [1, "two", 3.0],
        "object": {"nested": "value"},
    }
    result = canonicalise_json(obj)

    # Keys should be sorted
    expected = (
        '{"array":[1,"two",3.0],"bool":true,"float":3.14,"null":null,'
        '"number":42,"object":{"nested":"value"},"string":"hello"}'
    )
    assert result == expected


def test_canonicalise_with_arrays():
    """Test canonicalisation preserves array ordering but sorts object keys."""
    obj = {"z_last": [{"b": 2, "a": 1}, {"d": 4, "c": 3}], "a_first": [3, 2, 1]}
    result = canonicalise_json(obj)

    # Object keys sorted, but arrays maintain order
    expected = '{"a_first":[3,2,1],"z_last":[{"a":1,"b":2},{"c":3,"d":4}]}'
    assert result == expected


def test_canonicalise_complex_real_world():
    """Test canonicalisation with complex real-world-like data."""
    obj = {
        "user": {
            "name": "Alice",
            "id": 12345,
            "email": "alice@example.com",
            "roles": ["admin", "user"],
            "metadata": {
                "created": "2023-01-01",
                "updated": "2023-12-31",
                "flags": {"active": True, "verified": False},
            },
        },
        "timestamp": 1234567890,
        "version": "1.0.0",
    }

    result = canonicalise_json(obj)

    # Should have sorted keys at all levels
    assert result.startswith('{"timestamp":')
    assert '"user":{' in result
    assert '"email":"alice@example.com"' in result
    assert '"roles":["admin","user"]' in result  # Array order preserved


def test_canonicalise_invalid_type():
    """Test that invalid types raise CanonicalisationError."""
    with pytest.raises(CanonicalisationError):
        canonicalise_json(object())  # type: ignore


def test_canonicalise_circular_reference():
    """Test that circular references raise CanonicalisationError."""
    obj: dict = {"a": 1}
    obj["self"] = obj  # Circular reference
    with pytest.raises(CanonicalisationError):
        canonicalise_json(obj)  # type: ignore
