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

"""JSON canonicalisation utilities.

This module provides functions to convert JSON objects into a canonical string
representation for use as cache keys. The canonicalisation ensures that objects
with the same content but different key orders produce identical strings.
"""

import json
from typing import Any, Dict, List, Union

from .exceptions import CanonicalisationError

# Type alias for JSON-compatible types
JsonType = Union[Dict[str, Any], List[Any], str, int, float, bool, None]


def canonicalise_json(obj: JsonType) -> str:
    """
    Convert a JSON object to its canonical string representation.

    This function ensures that:
    - Object keys are sorted alphabetically
    - Whitespace is removed
    - Number formatting is consistent
    - Array order is preserved

    The canonical string is used as the LRU cache key, guaranteeing that
    semantically equivalent JSON objects map to the same cache entry.

    Args:
        obj: A JSON-serialisable object (dict, list, str, int, float, bool, None)

    Returns:
        str: The canonical JSON string representation

    Raises:
        CanonicalisationError: If the object cannot be serialised to JSON

    Examples:
        >>> canonicalise_json({"b": 2, "a": 1})
        '{"a":1,"b":2}'

        >>> canonicalise_json({"a": 1, "b": 2})
        '{"a":1,"b":2}'

        Both produce the same output despite different key orders.
    """
    try:
        # sort_keys=True ensures consistent key ordering
        # separators=(',', ':') removes whitespace
        # ensure_ascii=False preserves Unicode characters
        return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except (TypeError, ValueError) as e:
        raise CanonicalisationError(f"Failed to canonicalise JSON: {e}") from e
