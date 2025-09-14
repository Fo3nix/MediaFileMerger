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
        self.conflicts: Dict[str, List[str]] = {}
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

    def record_conflict(self, field_name: str, message: str):
        """Records a conflict for a specific field."""
        if field_name not in self.conflicts:
            self.conflicts[field_name] = []
        self.conflicts[field_name].append(message)

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
            context.record_conflict(self.field_name, f"Conflicting values: {sorted(list(potential_values))}")


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
                all_values = {reference_val} | conflicting_values
                msg = (f"GPS coordinates from different sources are not close enough to be considered the same. "
                       f"Found values: {sorted(list(all_values))}"
                       f"Source IDs: {[s.id for s in context.sources if getattr(s, field) in all_values]}"
                       )
                context.record_conflict(field, msg)
            else:
                context.set_value(field, reference_val)


class DateTimeAndZoneMergeStep(BasicFieldMergeStep):
    """
    Merges date/time fields by establishing a single, canonical, timezone-aware datetime.

    This step follows a prioritized hierarchy to resolve the final time and timezone,
    handling various combinations of timezone-aware and naive datetime sources.

    The logic proceeds as follows:

    1.  **UTC Time Consolidation**: First, all non-null, timezone-aware datetime values are
        converted to UTC. They must all represent the exact same moment in time (within a
        2-second tolerance). If they conflict, an unresolvable conflict is recorded. This
        establishes the definitive "when".

    2.  **Local Time Inference via Naive Time (Primary Method)**: The step then analyzes all
        naive (timezone-unaware) datetime values.
        -   If there is **exactly one unique naive time** across all sources, it is assumed
            to be the correct **local time**. The timezone offset is calculated from the
            difference between this local time and the consolidated UTC time. The final
            result is a new, aware datetime using this inferred offset. This is the
            preferred method for determining the correct timezone.

    3.  **Fallback to GPS-Inferred Timezone**: If the primary method cannot be used (e.g.,
        there are no naive times, or there are multiple conflicting naive times), the logic
        falls back to using GPS coordinates, if they have been finalized by a previous step.
        -   The timezone is determined from the latitude and longitude.
        -   The final value is the consolidated UTC time localized to this GPS-inferred timezone.

    4.  **Fallback to Original Timezone Offset**: If both naive time inference and GPS localization
        are not possible, the logic examines the timezone offsets of the original aware sources.
        -   If all aware sources share the **same timezone offset**, that offset is used to create
            the final datetime from the consolidated UTC time.
        -   If the original aware sources have conflicting offsets, it is an unresolvable conflict,
            as the true local time cannot be determined.

    5.  **Conflict Handling**: A conflict is recorded if:
        -   Aware datetimes do not agree on the absolute UTC time.
        -   Multiple, different naive datetimes exist, creating ambiguity about the local time.
        -   Aware datetimes have different offsets, and there is no unique naive time or GPS
            data to serve as a tie-breaker.
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
        # Get all Metadata objects that have a value for the target field
        sources_with_date = [s for s in context.sources if getattr(s, self.field_name) is not None]
        if not sources_with_date:
            return

        # Check that all values are datetime objects
        if not all(isinstance(getattr(s, self.field_name), datetime) for s in sources_with_date):
            raise TypeError(f"All values for {self.field_name} must be datetime objects")

        # Separate the SOURCE OBJECTS into aware and naive lists
        aware_sources = [s for s in sources_with_date if getattr(s, self.field_name).tzinfo is not None]
        naive_sources = [s for s in sources_with_date if getattr(s, self.field_name).tzinfo is None]

        inferred_tz = self.infer_timezone(context)

        if not aware_sources:
            # Pass the list of source objects, not just values
            self._process_only_naive(context, naive_sources, inferred_tz)
        else:
            # Pass both lists of source objects
            self._process_with_aware(context, aware_sources, naive_sources, inferred_tz)

    def _process_only_naive(self, context: MergeContext, naive_sources: list[models.Metadata],
                            inferred_tz: ZoneInfo | None):
        if not naive_sources:
            return

        # Group sources by their naive datetime value
        unique_naive_groups = {}
        for s in naive_sources:
            unique_naive_groups.setdefault(getattr(s, self.field_name), []).append(s.id)

        if len(unique_naive_groups) == 1:
            # Success, all are the same
            single_value = list(unique_naive_groups.keys())[0]
            if inferred_tz:
                single_value = single_value.replace(tzinfo=inferred_tz)
            context.set_value(self.field_name, single_value)
            return

        unique_naive_values = list(unique_naive_groups.keys())
        if not inferred_tz or len(unique_naive_values) != 2:
            conflicting_ids = sorted([s.id for s in naive_sources])
            msg = (f"Found multiple distinct naive times {sorted(unique_naive_values)}, but cannot resolve them. "
                   f"Source IDs: {conflicting_ids}")
            context.record_conflict(self.field_name, msg)
            return

        # Heuristic with IDs
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
            final_value = utc_time.astimezone(inferred_tz)
            context.set_value(self.field_name, final_value)
        else:
            conflicting_ids = sorted([s.id for s in naive_sources])
            offset = inferred_tz.utcoffset(t1)
            msg = (f"The difference between naive times '{t1}' and '{t2}' cannot be explained "
                   f"by the inferred timezone offset ({offset}). Source IDs: {conflicting_ids}")
            context.record_conflict(self.field_name, msg)

    def _process_with_aware(self, context: MergeContext, aware_sources: list[models.Metadata],
                            naive_sources: list[models.Metadata],
                            inferred_tz: ZoneInfo | None):
        # Group source IDs by their UTC time
        utc_groups = {}
        for s in aware_sources:
            utc_time = getattr(s, self.field_name).astimezone(timezone.utc)
            utc_groups.setdefault(utc_time, []).append(s.id)

        # UTC Consistency Check with tolerance
        if len(utc_groups) > 1:
            times = sorted(utc_groups.keys())
            min_time, max_time = times[0], times[-1]

            if (max_time - min_time) > timedelta(seconds=2):
                report = {k.isoformat(): sorted(v) for k, v in utc_groups.items()}
                msg = f"Timezone-aware datetimes do not agree on the absolute UTC time. Groups: {report}"
                context.record_conflict(self.field_name, msg)
                return

        final_utc_time = min(utc_groups.keys())
        final_value = None

        # Group naive sources by their datetime value to find unique times
        unique_naive_groups = {}
        for s in naive_sources:
            unique_naive_groups.setdefault(getattr(s, self.field_name), []).append(s.id)


        # Case 1: Resolve using a single unique naive time as the local time reference.
        # This handles your scenario where aware + naive times can determine the offset.
        if final_value is None and len(unique_naive_groups) == 1:
            unique_naive_time = list(unique_naive_groups.keys())[0]
            offset = unique_naive_time - final_utc_time.replace(tzinfo=None)

            try:
                # Create a fixed-offset timezone from the calculated difference
                inferred_tz_from_naive = timezone(offset)
                final_value = final_utc_time.astimezone(inferred_tz_from_naive)
            except ValueError:
                msg = (f"The difference between aware UTC time '{final_utc_time.isoformat()}' and naive time "
                       f"'{unique_naive_time.isoformat()}' resulted in an invalid timezone offset of '{offset}'.")
                context.record_conflict(self.field_name, msg)
                return

        # Case 2: No naive times, or conflicting naive times. Fall back to using GPS or original offsets.
        if final_value is None:
            if inferred_tz:
                # Use GPS as the source of truth for the timezone.
                final_value = final_utc_time.astimezone(inferred_tz)
            else:
                # No GPS. Check if original aware sources had a consistent offset.
                unique_offsets = {getattr(s, self.field_name).utcoffset() for s in aware_sources}
                if len(unique_offsets) == 1:
                    # All aware times have the same offset, so it's safe to use that zone.
                    final_value = final_utc_time.astimezone(getattr(aware_sources[0], self.field_name).tzinfo)
                elif len(unique_offsets) == 2:
                    # if one of the offsets is zero (UTC), we can still use the other offset
                    # so check for that case (that one has timedelta of 0, the other does not)
                    if None in unique_offsets:
                        unique_offsets.remove(None)
                    if timedelta(0) in unique_offsets:
                        unique_offsets.remove(timedelta(0))
                    if len(unique_offsets) == 1:
                        final_value = final_utc_time.astimezone(timezone(unique_offsets.pop()))
                    else:
                        offsets_repr = sorted([o for o in unique_offsets if o is not None])
                        msg = (f"Aware datetimes have conflicting offsets ({offsets_repr}), and no GPS or unique naive "
                               f"time is available to determine the correct local timezone.")
                        context.record_conflict(self.field_name, msg)
                        return
                elif len(unique_offsets) > 2:
                    offsets_repr = sorted([o for o in unique_offsets if o is not None])
                    msg = (f"Aware datetimes have conflicting offsets ({offsets_repr}), and no GPS or unique naive "
                           f"time is available to determine the correct local timezone.")
                    context.record_conflict(self.field_name, msg)
                    return

        context.set_value(self.field_name, final_value)

        # If there were multiple distinct naive times initially, they represent an unresolvable conflict.
        if len(unique_naive_groups) > 1:
            conflicting_ids = sorted([s.id for s in naive_sources])
            msg = (f"Found multiple distinct naive times {sorted(unique_naive_groups.keys())}, creating ambiguity. "
                   f"While a final value was determined from higher-priority data, this conflict in the source is being noted. Chosen value: {final_value.isoformat()}. "
                   f"Source IDs: {conflicting_ids}")
            context.record_conflict(self.field_name, msg)

# --- The Pipeline Orchestrator ---

class MergePipeline:
    def __init__(self, steps: List[MergeStep]):
        self.steps = steps

    def run(self, sources: List[models.Metadata]) -> MergeContext:
        context = MergeContext(sources)
        for step in self.steps:
            step.process(context)
        return context