from photoprocessor import models
from typing import Any, Dict, List, Set
import abc
from timezonefinder import TimezoneFinder
from zoneinfo import ZoneInfo
from photoprocessor.merge_rules import rules
from datetime import datetime, timezone, timedelta
from photoprocessor.export_arguments import DateTimeArgument, SimpleArgument, ExportArgument
import re

from photoprocessor.models import MetadataEntry


class MergeContext:
    """Holds the state for a single media file's metadata merge process."""
    def __init__(self, sources: List[models.MetadataSource]):
        self.entries: List[models.MetadataEntry] = [entry for src in sources for entry in src.entries]
        self.merged_data: Dict[str, ExportArgument] = {}
        self.conflicts: Dict[str, List[str]] = {}
        self.finalized_fields: Set[str] = set()

    def get_entries_by_keys(self, key: list[str]) -> List[models.MetadataEntry]:
        """Returns all MetadataEntry objects with the specified keys."""
        return [e for e in self.entries if e.key in key]

    def get_value(self, field_name: str, required: bool = False) -> Any:
        """
        Gets a value from the merged data.
        If the stored value is an ExportArgument, its raw value is returned for dependency checks.
        Raises an error if a required field has not been finalized yet.
        """
        if required and field_name not in self.finalized_fields:
            raise RuntimeError(
                f"Dependency error: Attempted to access '{field_name}' before it was finalized."
            )

        stored_value = self.merged_data.get(field_name)

        # If a later step needs the raw value from an argument object, extract it.
        if isinstance(stored_value, ExportArgument):
            return stored_value.value

        return stored_value

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

    def _validate_argument_conflicts(self):
        """
        Checks for overlapping tags among all final export arguments in merged_data.
        This is an internal method called by get_all_arguments.
        """
        seen_tags = set()
        arguments = [arg for arg in self.merged_data.values() if isinstance(arg, ExportArgument)]

        for arg_obj in arguments:
            managed_tags = arg_obj.get_managed_tags()
            intersection = seen_tags.intersection(managed_tags)

            if intersection:
                conflict_msg = (
                    f"Tag conflict detected! Multiple merge steps produced arguments that write to the same tags: "
                    f"{sorted(list(intersection))}. Conflicting argument type: {type(arg_obj).__name__}"
                )
                # Record this as a general, file-level conflict
                self.record_conflict("_File", conflict_msg)

            seen_tags.update(managed_tags)

    def get_all_arguments(self) -> List[ExportArgument]:
        """
        Validates for tag conflicts and returns all ExportArgument objects from the merged data.
        If conflicts are found, they will be recorded in the `conflicts` dictionary, which can be
        checked after calling this method.
        """
        self._validate_argument_conflicts()

        # This will only return argument objects, filtering out any other intermediate data
        return [arg for arg in self.merged_data.values() if isinstance(arg, ExportArgument)]

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

    def __init__(self, key: str):
        self.key = key

    def process(self, context: MergeContext):
        # Filter sources by the key we are interested in
        relevant_entries = context.get_entries_by_keys([self.key])

        # COALESCE the typed value columns to get the actual value
        potential_values = {
            s.value_str or s.value_dt or s.value_real for s in relevant_entries
        }
        potential_values.discard(None)  # Remove None if it exists

        if not potential_values:
            return  # No values to merge

        if len(potential_values) == 1:
            context.set_value(self.key, potential_values.pop())
        else:
            context.record_conflict(self.key, f"Conflicting values: {sorted(list(potential_values), key=str)}. Entry IDs: {[s.id for s in relevant_entries if (s.value_str or s.value_dt or s.value_real) in potential_values]}")


class GPSMergeStep(MergeStep):
    """
    Merges GPS fields using a prioritized, two-stage approach.

    1.  **Primary Source (Composite EXIF)**: The step first attempts to merge coordinates
        using only the `Composite:GPSLatitude` and `Composite:GPSLongitude` tags. If a
        non-conflicting value is found, it is used, and the process for that
        coordinate (latitude or longitude) stops.

    2.  **Secondary Source (Google JSON)**: If and only if the primary search yields
        no data, the step proceeds to merge coordinates using the `google:geoDataLatitude`
        and `google:geoDataLongitude` tags.

    A conflict is recorded if multiple values from within the *same stage* are not
    close enough to each other (as defined by the gps_comparator rule).
    """

    def _merge_values_from_tags(self, context: MergeContext, key: str, tags: List[str]) -> tuple[float | None, str | None]:
        """
        Merges values from a given set of tags.

        Returns:
            A tuple containing (merged_value, conflict_message).
            - (float, None) on success.
            - (None, str) on conflict.
            - (None, None) if no data was found.
        """
        entries = context.get_entries_by_keys(tags)
        if not entries:
            return None, None  # No data found

        values = {e.value_real for e in entries if e.value_real is not None}
        if not values:
            return None, None  # No non-null data found

        # Use the first value as the reference for comparison
        reference_val = next(iter(values))
        conflicting_values = set()

        # Compare all other values against the reference
        for val in values:
            if not rules.compare(key, reference_val, val):
                conflicting_values.add(val)

        if conflicting_values:
            # If there are conflicts, report all differing values
            all_distinct_values = {reference_val} | conflicting_values
            source_ids = {e.id for e in entries if e.value_real in all_distinct_values}
            msg = (f"GPS coordinates from the same source type are not close enough. "
                   f"Found values: {sorted(list(all_distinct_values))}. "
                   f"Source Entry IDs: {sorted(list(source_ids))}")
            return None, msg
        else:
            # Success, only one unique value (within tolerance)
            return reference_val, None

    def _process_coordinate(self, context: MergeContext, key: str, primary_tags: List[str], secondary_tags: List[str], final_tag: str):
        """Processes a single coordinate (lat or lon) using the prioritized stages."""
        # --- Stage 1: Primary Source (Composite EXIF) ---
        merged_value, conflict_msg = self._merge_values_from_tags(context, key, primary_tags)

        if conflict_msg:
            context.record_conflict(key, f"[Primary Source Conflict] {conflict_msg}")
            return  # Conflict found, stop processing this coordinate

        if merged_value is not None:
            # Success on primary source
            export_argument = SimpleArgument(final_tag, str(merged_value))
            context.set_value(key, export_argument)
            return  # Value found, stop processing

        # --- Stage 2: Secondary Source (Google JSON) ---
        # This stage only runs if the primary source had no data
        merged_value, conflict_msg = self._merge_values_from_tags(context, key, secondary_tags)

        if conflict_msg:
            context.record_conflict(key, f"[Secondary Source Conflict] {conflict_msg}")
            return  # Conflict found in secondary source

        if merged_value is not None:
            # Success on secondary source
            export_argument = SimpleArgument(final_tag, str(merged_value))
            context.set_value(key, export_argument)

    def process(self, context: MergeContext):
        """Executes the merge process for both latitude and longitude."""
        # Process Latitude
        self._process_coordinate(
            context,
            key="gps_latitude",
            primary_tags=["Composite:GPSLatitude"],
            secondary_tags=["google:geoDataLatitude"],
            final_tag="GPSLatitude"
        )

        # Process Longitude
        self._process_coordinate(
            context,
            key="gps_longitude",
            primary_tags=["Composite:GPSLongitude"],
            secondary_tags=["google:geoDataLongitude"],
            final_tag="GPSLongitude"
        )

class DateTimeAndZoneMergeStep(MergeStep):
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

    def __init__(self, date_type: str):
        if date_type not in ("taken", "modified"):
            raise ValueError("date_type must be 'taken' or 'modified'")
        self.date_type = date_type
        self.tz_finder = TimezoneFinder()

    def _detect_date_from_file_name(self, filename: str) -> datetime | None:
        """
        Detects a date from a filename using a comprehensive regex pattern.
        Returns a naive datetime object if a full timestamp is found, otherwise None.
        """
        if not filename:
            return None

        # This single regex is designed to capture the most common timestamp formats.
        # It looks for Year, Month, Day, Hour, Minute, and Second with various
        # optional separators (-, _, :, T, or space).
        # It can handle formats like:
        # - "IMG20251001172015.jpg"
        # - "VID_2025-10-01_17-20-15.mp4"
        # - "Screenshot_2025-10-01-17-20-15-123.png"
        # - "20251001T172015Z"
        pattern = re.search(
            r"(?<!\d)"                  # PREVENTS: Matching a year if it's preceded by a digit (e.g., in "803041205")
            r"(19[7-9]\d|20[0-4]\d|2050)" # CAPTURES: Year, but only from 1970-2050
            r"[-_]?"  # Optional separator
            r"(0[1-9]|1[0-2])"  # Capture Month (01-12)
            r"[-_]?"  # Optional separator
            r"(0[1-9]|[12][0-9]|3[01])"  # Capture Day (01-31)
            r"[-_T\s]?"  # Optional date-time separator (T, space, etc.)
            r"([01][0-9]|2[0-3])"  # Capture Hour (00-23)
            r"[-_:]?"  # Optional time separator
            r"([0-5][0-9])"  # Capture Minute (00-59)
            r"[-_:]?"  # Optional time separator
            r"([0-5][0-9])",  # Capture Second (00-59)
            filename
        )

        # If a pattern was found, try to construct a datetime object from it.
        if pattern:
            try:
                # The pattern has 6 capture groups for y, mo, d, h, mi, s.
                # We map them to integers.
                y, mo, d, h, mi, s = map(int, pattern.groups())

                # The datetime constructor automatically validates the date.
                # It will raise a ValueError for impossible dates like February 30th.
                return datetime(y, mo, d, h, mi, s)
            except ValueError:
                # This catches invalid dates (e.g., month=13) that the regex
                # might technically match but are not real dates.
                return None

        return None

    def _get_exif_keys(self) -> List[str|tuple[str,str]]:
        if self.date_type == "taken":
            return [
                "XMP:DateTimeOriginal",
                ("EXIF:DateTimeOriginal", "EXIF:OffsetTimeOriginal"),
                "EXIF:DateTimeOriginal",
                "QuickTime:CreationDate",
                "QuickTime:CreateDate",
                "Keys:CreationDate",
                "UserData:DateTimeOriginal",
                "XMP:CreateDate",
                "EXIF:CreateDate",
                "google:photoTakenTime",
            ]
        elif self.date_type == "modified":
            return [
                "EXIF:ModifyDate",
                "XMP:ModifyDate",
                "QuickTime:ModifyDate",
            ]
        return []

    def _get_value_from_tag_and_entries(self, tag: str|tuple[str,str], entries: List[models.MetadataEntry]) -> datetime | None:
        if isinstance(tag, str):
            for e in entries:
                if e.key == tag and e.value_dt is not None:
                    return e.value_dt
        elif isinstance(tag, tuple) and len(tag) == 2:
            first_tag, second_tag = tag
            first_value = self._get_value_from_tag_and_entries(first_tag, entries)
            second_value = self._get_value_from_tag_and_entries(second_tag, entries)

            # if first_value and second_value are not none
            if first_value and second_value:
                # if first value is datetime and second value is string
                if isinstance(first_value, datetime) and isinstance(second_value, str):
                    # if first value is naive and second value is in format +HH:MM or -HH:MM
                    regex_time_offset = r'^[+-](0[0-9]|1[0-4]):([0-5][0-9])$'
                    if first_value.tzinfo is None and re.match(regex_time_offset, second_value):
                        hours, minutes = map(int, second_value.split(':'))
                        offset = timedelta(hours=hours, minutes=minutes)
                        tzinfo = timezone(offset)
                        return first_value.replace(tzinfo=tzinfo)
        return None

    def _get_all_values_from_tags(self, tags: List[str|tuple[str,str]], entries: List[models.MetadataEntry]) -> List[datetime]:
        values = []
        for tag in tags:
            value = self._get_value_from_tag_and_entries(tag, entries)
            if value is not None:
                values.append(value)
        return values

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

    def _process_filenames(self, context: MergeContext) -> List[MetadataEntry]|None:
        # Attempt to extract dates from filenames as a last resort
        file_name_entries = context.get_entries_by_keys(["google:title", "exiftool:SourceFile"])
        for entry in file_name_entries:
            if entry.value_str:
                detected_date = self._detect_date_from_file_name(entry.value_str)
                if detected_date:
                    entry.value_dt = detected_date

        date_set = {entry.value_dt for entry in file_name_entries if entry.value_dt is not None}
        aware_date_set = {d for d in date_set if d.tzinfo is not None}
        naive_date_set = {d for d in date_set if d.tzinfo is None}

        if len(aware_date_set) > 1 or len(naive_date_set) > 1:
            context.record_conflict(self.date_type, f"Multiple distinct dates inferred from filenames: {sorted(list(date_set))}")
            return None

        # return entries with value_dt set
        return [e for e in file_name_entries if e.value_dt is not None]

    def process(self, context: MergeContext):
        # Get all MetadataEntry objects that have a key for the target field
        all_values = context.get_entries_by_keys(self._get_exif_keys())

        if self.date_type == "taken":
            filename_dates = self._process_filenames(context)
            if filename_dates is None:
                #return cause conflict
                return
            all_values.extend(filename_dates)

        # Check that all values are datetime objects
        if not all(isinstance(s.value_dt, datetime) for s in all_values):
            raise TypeError(f"All values for {self.date_type} must be datetime objects")

        # Separate the SOURCE OBJECTS into aware and naive lists
        aware_sources = [s for s in all_values if s.value_dt.tzinfo is not None]
        naive_sources = [s for s in all_values if s.value_dt.tzinfo is None]

        inferred_tz = self.infer_timezone(context)

        value = None
        if not aware_sources:
            value = self._process_only_naive(context, naive_sources, inferred_tz)
        else:
            value = self._process_with_aware(context, aware_sources, naive_sources, inferred_tz)

        if not value:
            # try to see if we can get the composte:GPSDateTime tag
            gps_datetime = context.get_value("Composite:GPSDateTime")
            if isinstance(gps_datetime, datetime):
                value = gps_datetime.astimezone(inferred_tz) if inferred_tz else gps_datetime

        if value:
            export_arg = DateTimeArgument(value, "taken")
            context.set_value(self.date_type, export_arg)

    def _process_only_naive(self, context: MergeContext, naive_sources: list[models.MetadataEntry],
                            inferred_tz: ZoneInfo | None):
        if not naive_sources:
            return None

        # Group sources by their naive datetime value
        unique_naive_groups = {}
        for s in naive_sources:
            unique_naive_groups.setdefault(s.value_dt, []).append(s.id)

        if len(unique_naive_groups) == 1:
            # Success, all are the same
            single_value = list(unique_naive_groups.keys())[0]
            if inferred_tz:
                single_value = single_value.replace(tzinfo=inferred_tz)
            return single_value

        unique_naive_values = list(unique_naive_groups.keys())
        if not inferred_tz or len(unique_naive_values) != 2:
            conflicting_ids = sorted([s.id for s in naive_sources])
            msg = (f"Found multiple distinct naive times {sorted(unique_naive_values)}, but cannot resolve them. "
                   f"Source IDs: {conflicting_ids}")
            context.record_conflict(self.date_type, msg)
            return None

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
            return final_value
        else:
            conflicting_ids = sorted([s.id for s in naive_sources])
            offset = inferred_tz.utcoffset(t1)
            msg = (f"The difference between naive times '{t1}' and '{t2}' cannot be explained "
                   f"by the inferred timezone offset ({offset}). Source IDs: {conflicting_ids}")
            context.record_conflict(self.date_type, msg)
            return None

    def _process_with_aware(self, context: MergeContext, aware_sources: list[models.MetadataEntry],
                            naive_sources: list[models.MetadataEntry],
                            inferred_tz: ZoneInfo | None):
        # Group source IDs by their UTC time
        utc_groups = {}
        for s in aware_sources:
            utc_time = s.value_dt.astimezone(timezone.utc)
            utc_groups.setdefault(utc_time, []).append(s.id)

        # UTC Consistency Check with tolerance
        if len(utc_groups) > 1:
            times = sorted(utc_groups.keys())
            min_time, max_time = times[0], times[-1]

            if (max_time - min_time) > timedelta(seconds=2):
                report = {k.isoformat(): sorted(v) for k, v in utc_groups.items()}
                msg = f"Timezone-aware datetimes do not agree on the absolute UTC time. Groups: {report}"
                context.record_conflict(self.date_type, msg)
                return None

        final_utc_time = min(utc_groups.keys())
        final_value = None

        # Group naive sources by their datetime value to find unique times
        unique_naive_groups = {}
        for s in naive_sources:
            unique_naive_groups.setdefault(s.value_dt, []).append(s.id)


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
                context.record_conflict(self.date_type, msg)
                return None

        # Case 2: No naive times, or conflicting naive times. Fall back to using GPS or original offsets.
        if final_value is None:
            if inferred_tz:
                # Use GPS as the source of truth for the timezone.
                final_value = final_utc_time.astimezone(inferred_tz)
            else:
                # No GPS. Check if original aware sources had a consistent offset.
                unique_offsets = {s.value_dt.utcoffset() for s in aware_sources}
                if len(unique_offsets) == 1:
                    # All aware times have the same offset, so it's safe to use that zone.
                    final_value = final_utc_time.astimezone(aware_sources[0].value_dt.tzinfo)
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
                        context.record_conflict(self.date_type, msg)
                        return None
                elif len(unique_offsets) > 2:
                    offsets_repr = sorted([o for o in unique_offsets if o is not None])
                    msg = (f"Aware datetimes have conflicting offsets ({offsets_repr}), and no GPS or unique naive "
                           f"time is available to determine the correct local timezone.")
                    context.record_conflict(self.date_type, msg)
                    return None

        # If there were multiple distinct naive times initially, they represent an unresolvable conflict.
        if len(unique_naive_groups) > 1:
            conflicting_ids = sorted([s.id for s in naive_sources])
            msg = (f"Found multiple distinct naive times {sorted(unique_naive_groups.keys())}, creating ambiguity. "
                   f"While a final value was determined from higher-priority data, this conflict in the source is being noted. Chosen value: {final_value.isoformat()}. "
                   f"Source IDs: {conflicting_ids}")
            # context.record_conflict(self.date_type, msg)

        if final_value is not None:
            return final_value
        else:
            return None


# --- The Pipeline Orchestrator ---

class MergePipeline:
    def __init__(self, steps: List[MergeStep]):
        self.steps = steps

    def run(self, sources: List[models.MetadataSource]) -> MergeContext:
        context = MergeContext(sources)
        for step in self.steps:
            step.process(context)
        return context