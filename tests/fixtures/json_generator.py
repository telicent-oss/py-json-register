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

"""Random JSON object generator for performance testing."""

import random
import string
from typing import Any

# Type alias for JSON-compatible types
JsonValue = dict[str, Any] | list[Any] | str | int | float | bool | None


class RandomJsonGenerator:
    """Generate random JSON objects with configurable complexity."""

    def __init__(self, seed: int = 42):
        """
        Initialize the random JSON generator.

        Args:
            seed: Random seed for reproducibility
        """
        self.rng = random.Random(seed)

    def generate_string(self, min_length: int = 5, max_length: int = 50) -> str:
        """Generate a random string."""
        length = self.rng.randint(min_length, max_length)
        return "".join(self.rng.choices(string.ascii_letters + string.digits + " ", k=length))

    def generate_value(self, depth: int = 0, max_depth: int = 3) -> JsonValue:
        """
        Generate a random JSON value.

        Args:
            depth: Current nesting depth
            max_depth: Maximum nesting depth

        Returns:
            A random JSON-compatible value
        """
        if depth >= max_depth:
            # At max depth, only generate primitives
            value_type = self.rng.choice(["string", "int", "float", "bool", "null"])
        else:
            # Can generate any type including nested structures
            value_type = self.rng.choice(["string", "int", "float", "bool", "null", "list", "dict"])

        if value_type == "string":
            return self.generate_string()
        elif value_type == "int":
            return self.rng.randint(-1000000, 1000000)
        elif value_type == "float":
            return self.rng.uniform(-1000000, 1000000)
        elif value_type == "bool":
            return self.rng.choice([True, False])
        elif value_type == "null":
            return None
        elif value_type == "list":
            list_length = self.rng.randint(0, 10)
            return [self.generate_value(depth + 1, max_depth) for _ in range(list_length)]
        elif value_type == "dict":
            return self.generate_object(min_keys=1, max_keys=10, depth=depth + 1, max_depth=max_depth)
        else:
            return None

    def generate_object(
        self, min_keys: int = 1, max_keys: int = 20, depth: int = 0, max_depth: int = 3
    ) -> dict[str, JsonValue]:
        """
        Generate a random JSON object.

        Args:
            min_keys: Minimum number of keys
            max_keys: Maximum number of keys
            depth: Current nesting depth
            max_depth: Maximum nesting depth

        Returns:
            A random JSON object
        """
        num_keys = self.rng.randint(min_keys, max_keys)
        obj: dict[str, JsonValue] = {}

        for i in range(num_keys):
            # Generate unique key
            key = f"key_{i}_{self.rng.randint(0, 999999)}"
            obj[key] = self.generate_value(depth, max_depth)

        return obj

    def generate_batch(
        self,
        count: int,
        min_keys: int = 1,
        max_keys: int = 20,
        max_depth: int = 3,
        unique: bool = True,
    ) -> list[dict[str, JsonValue]]:
        """
        Generate a batch of random JSON objects.

        Args:
            count: Number of objects to generate
            min_keys: Minimum number of keys per object
            max_keys: Maximum number of keys per object
            max_depth: Maximum nesting depth
            unique: If True, ensure all objects are unique

        Returns:
            List of random JSON objects
        """
        objects = []
        seen = set()

        for _ in range(count):
            if unique:
                # Keep generating until we get a unique object
                attempts = 0
                max_attempts = count * 10
                while attempts < max_attempts:
                    obj = self.generate_object(min_keys, max_keys, 0, max_depth)
                    obj_str = str(sorted(obj.items()))
                    if obj_str not in seen:
                        seen.add(obj_str)
                        objects.append(obj)
                        break
                    attempts += 1
                else:
                    # If we can't generate unique objects, just add it anyway
                    objects.append(obj)
            else:
                objects.append(self.generate_object(min_keys, max_keys, 0, max_depth))

        return objects

    def generate_profile(self, profile_name: str) -> dict[str, JsonValue]:
        """
        Generate a JSON object based on a predefined profile.

        Profiles:
        - "small": Small object (1-5 keys, depth 1)
        - "medium": Medium object (5-15 keys, depth 2)
        - "large": Large object (15-30 keys, depth 3)
        - "deep": Deeply nested object (5 keys, depth 5)
        - "wide": Wide object (50+ keys, depth 1)

        Args:
            profile_name: Name of the profile

        Returns:
            A JSON object matching the profile
        """
        if profile_name == "small":
            return self.generate_object(min_keys=1, max_keys=5, max_depth=1)
        elif profile_name == "medium":
            return self.generate_object(min_keys=5, max_keys=15, max_depth=2)
        elif profile_name == "large":
            return self.generate_object(min_keys=15, max_keys=30, max_depth=3)
        elif profile_name == "deep":
            return self.generate_object(min_keys=3, max_keys=5, max_depth=5)
        elif profile_name == "wide":
            return self.generate_object(min_keys=50, max_keys=100, max_depth=1)
        else:
            raise ValueError(f"Unknown profile: {profile_name}")
