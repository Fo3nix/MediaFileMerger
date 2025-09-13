from photoprocessor import models
from typing import Any, Dict, List, Set
import abc
from timezonefinder import TimezoneFinder
from zoneinfo import ZoneInfo
from photoprocessor.merge_rules import rules
from datetime import datetime, timezone, timedelta


class MergeContext:
    """Holds the state for a single media file's metadata merge process."""
    def __init__(self, sources: List[models.Metadata]):
        self.sources: List[models.Metadata] = sources
        self.merged_data: Dict[str, Any] = {}
        self.conflicts: Dict[str, Set[Any]] = {}
        self.finalized_fields: Set[str] = set()

    def get_value(self, field_name: str, required: bool = False) -> Any:
        """
        Gets a value from the merged data.
        Raises an error if a required field has not been finalized yet.
        """
        if required and field_name not in self.finalized_fields:
            raise RuntimeError(
                f"Dependency error: Attempted to access '{field_name}' before it was finalized."
            )
        return self.merged_data.get(field_name)

    def set_value(self, field_name: str, value: Any):
        """Sets a final value in the merged data and marks it as finalized."""
        if value is not None:
            self.merged_data[field_name] = value
            self.finalized_fields.add(field_name)

    def record_conflict(self, field_name: str, values: Set[Any]):
        """Records a conflict for a specific field."""
        self.conflicts[field_name] = values

class MergeStep(abc.ABC):
    """Abstract base class for a single step in the merge pipeline."""
    @abc.abstractmethod
    def process(self, context: MergeContext):
        """Processes the data within the context."""
        pass


class BasicFieldMergeStep(MergeStep):
    """
    Merges simple fields by picking the first non-None value.
    Detects conflicts if subsequent sources have different, non-None values.
    """

    def __init__(self, field_name: str):
        self.field_name = field_name

    def process(self, context: MergeContext):
        potential_values = {getattr(source, self.field_name) for source in context.sources if
                            getattr(source, self.field_name) is not None}

        if not potential_values:
            return  # No values to merge

        if len(potential_values) == 1:
            context.set_value(self.field_name, potential_values.pop())
        else:
            # More advanced conflict detection using your rules
            # For simplicity here, we assume any multiple distinct values are a conflict
            context.record_conflict(self.field_name, potential_values)


class GPSMergeStep(MergeStep):
    """Merges GPS fields using the proximity comparison rule."""

    def process(self, context: MergeContext):
        for field in ["gps_latitude", "gps_longitude"]:
            # Gather all non-none values from the sources
            values = [getattr(s, field) for s in context.sources if getattr(s, field) is not None]
            if not values:
                continue

            # Use the first value as the reference
            reference_val = values[0]
            conflicting_values = {reference_val}

            # Compare all other values against the reference
            for val in values[1:]:
                if not rules.compare(field, reference_val, val):
                    conflicting_values.add(val)

            if len(conflicting_values) > 1:
                context.record_conflict(field, conflicting_values)
            else:
                context.set_value(field, reference_val)


class DateTimeAndZoneMergeStep(BasicFieldMergeStep):
    """
    Merges date/time fields by establishing a single, canonical, timezone-aware datetime.

    The logic proceeds as follows:
    1.  **Categorize Values**: All non-null datetime values are collected and separated into timezone-aware and naive lists.

    2.  **No Aware Values (Naive Only)**:
        a.  If all naive values are identical, the result is that single naive time. If GPS is available, it's localized to that timezone.
        b.  If naive values differ and there is no GPS, it's an unresolvable conflict.
        c.  If naive values differ and GPS is available, a heuristic is applied: it checks if the difference between the times can be explained by one being a naive UTC time and the other being a naive local time in the GPS-inferred timezone. If so, they are resolved. Otherwise, it's a conflict.

    3.  **Aware Values Exist**:
        a.  **UTC Consistency Check**: All aware values are converted to UTC. If they do not all represent the exact same moment in time, it is an unresolvable conflict.
        b.  **Establish Canonical Timezone**: The GPS data is considered the source of truth for the local timezone.
        c.  **Standardize**: The consistent UTC time is represented in the canonical (GPS-inferred) timezone. If no GPS is available, and the original values had conflicting offsets, it's a conflict about the true local time.
        d.  **Validate Naive Values**: Any naive values are checked against the final, standardized aware value. They must match the local time of the aware value exactly. If not, they are considered a conflict.
    """

    def __init__(self, field_name: str):
        super().__init__(field_name)
        self.tz_finder = TimezoneFinder()

    # Helper function to infer timezone from GPS data
    def infer_timezone(self, context: MergeContext) -> ZoneInfo | None:
        lat = context.get_value("gps_latitude")
        lon = context.get_value("gps_longitude")
        if lat is None or lon is None:
            return None
        tz_name = self.tz_finder.timezone_at(lat=lat, lng=lon)
        if tz_name is None:
            return None
        try:
            return ZoneInfo(tz_name)
        except Exception:
            return None

    def process(self, context: MergeContext):
        values = [getattr(s, self.field_name) for s in context.sources if getattr(s, self.field_name) is not None]
        if not values:
            return

        # Corrected type checking
        if not all(isinstance(v, datetime) for v in values):
            raise TypeError(f"All values for {self.field_name} must be datetime objects")

        aware_values = [v for v in values if v.tzinfo is not None]
        naive_values = [v for v in values if v.tzinfo is None]
        inferred_tz = self.infer_timezone(context)

        if not aware_values:
            self._process_only_naive(context, naive_values, inferred_tz)
        else:
            self._process_with_aware(context, aware_values, naive_values, inferred_tz)


    def _process_only_naive(self, context: MergeContext, naive_values: list[datetime], inferred_tz: ZoneInfo | None):
        if not naive_values:
            return

        unique_naive_values = list(set(naive_values))
        if len(unique_naive_values) == 1:
            single_value = unique_naive_values[0]
            if inferred_tz:
                single_value = single_value.replace(tzinfo=inferred_tz)
            context.set_value(self.field_name, single_value)
            return

        # Multiple distinct naive values
        if not inferred_tz or len(unique_naive_values) != 2:
            # Conflict if no GPS to help, or if more than 2 distinct times to compare
            context.record_conflict(self.field_name, set(unique_naive_values))
            return

        # --- IMPLEMENTING YOUR HEURISTIC for exactly two differing naive times ---
        t1, t2 = unique_naive_values[0], unique_naive_values[1]
        # We must use a sample datetime to get the offset, as it can depend on DST
        offset = inferred_tz.utcoffset(t1)

        utc_time = None

        # Possibility 1: t1 is the UTC time and t2 is the local time
        if t1.replace(tzinfo=timezone.utc).astimezone(inferred_tz).replace(tzinfo=None) == t2:
            utc_time = t1.replace(tzinfo=timezone.utc)

        # Possibility 2: t2 is the UTC time and t1 is the local time
        elif t2.replace(tzinfo=timezone.utc).astimezone(inferred_tz).replace(tzinfo=None) == t1:
            utc_time = t2.replace(tzinfo=timezone.utc)

        if utc_time:
            # Success. We found the correct UTC time. Standardize it to the inferred local timezone.
            final_value = utc_time.astimezone(inferred_tz)
            context.set_value(self.field_name, final_value)
        else:
            # The difference cannot be explained by the timezone offset
            context.record_conflict(self.field_name, set(unique_naive_values))

    def _process_with_aware(self, context: MergeContext, aware_values: list[datetime], naive_values: list[datetime],
                            inferred_tz: ZoneInfo | None):
        # 3a. UTC Consistency Check
        utc_times = {v.astimezone(timezone.utc) for v in aware_values}
        if len(utc_times) > 1:
            context.record_conflict(self.field_name, set(aware_values))
            return

        final_utc_time = utc_times.pop()

        # 3b/c. Establish Canonical Timezone and Standardize
        if inferred_tz:
            # GPS is the truth. Standardize the final value to this timezone.
            final_value = final_utc_time.astimezone(inferred_tz)
        else:
            # No GPS. Check if original offsets create ambiguity.
            unique_offsets = {v.utcoffset() for v in aware_values}
            if len(unique_offsets) > 1:
                # Without GPS, we cannot resolve the "Germany vs Russia" problem. Conflict.
                context.record_conflict(self.field_name, set(aware_values))
                return
            final_value = aware_values[0]  # All have same offset, so just use one

        context.set_value(self.field_name, final_value)

        # 3d. Validate Naive Values
        if naive_values:
            conflicting_naives = set()
            for nv in set(naive_values):
                # A naive value must match the final local time representation exactly.
                if (nv.year, nv.month, nv.day, nv.hour, nv.minute, nv.second) != \
                        (final_value.year, final_value.month, final_value.day,
                         final_value.hour, final_value.minute, final_value.second):
                    conflicting_naives.add(nv)

            if conflicting_naives:
                context.record_conflict(self.field_name, conflicting_naives)




# --- The Pipeline Orchestrator ---

class MergePipeline:
    def __init__(self, steps: List[MergeStep]):
        self.steps = steps

    def run(self, sources: List[models.Metadata]) -> MergeContext:
        context = MergeContext(sources)
        for step in self.steps:
            step.process(context)
        return context