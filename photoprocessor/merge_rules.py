from typing import Any, Callable, Dict
from functools import partial
import math

class MergeRules:
    """A simple rules engine for comparing metadata values."""
    def __init__(self):
        # Default comparison is simple equality.
        self._default_rule = lambda v1, v2: v1 == v2
        self._rules: Dict[str, Callable[[Any, Any], bool]] = {}

    def register(self, field_name: str, rule: Callable[[Any, Any], bool]):
        """Registers a specific comparison rule for a field."""
        self._rules[field_name] = rule

    def compare(self, field_name: str, value1: Any, value2: Any) -> bool:
        """Compares two values using the appropriate rule for the field."""
        rule = self._rules.get(field_name, self._default_rule)
        return rule(value1, value2)

# Rule for GPS coordinates: they are considered equal if they are very close.
def gps_comparator(val1: float, val2: float, tolerance: float = 1e-6) -> bool:
    """Returns True if the absolute difference is within the tolerance."""
    return math.isclose(val1, val2, abs_tol=tolerance)

# --- Create and Configure the Rules Engine ---

rules = MergeRules()
rules.register("gps_latitude", gps_comparator)
rules.register("gps_longitude", gps_comparator)