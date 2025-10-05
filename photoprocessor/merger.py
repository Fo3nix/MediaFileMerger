from photoprocessor import models
from typing import Any, Dict, List, Set
import abc
from timezonefinder import TimezoneFinder
from zoneinfo import ZoneInfo
from photoprocessor.merge_rules import rules
from datetime import datetime, timezone, timedelta
from photoprocessor.export_arguments import DateTimeArgument, SimpleArgument, ExportArgument
import re
from dataclasses import dataclass, field

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

@dataclass
class DateTimeCandidate:
    """Represents a cluster of nearly identical datetime values."""
    representative_value: datetime
    source_keys: Set[str|tuple[str,str]] = field(default_factory=set)
    source_ids: Set[int|tuple[int,int]] = field(default_factory=set)

    @classmethod
    def from_entry(cls, entry: MetadataEntry) -> 'DateTimeCandidate':
        """Creates a DateTimeCandidate from a single MetadataEntry."""
        return cls(
            representative_value=entry.value_dt,
            source_keys={entry.key},
            source_ids={entry.id}
        )

    @property
    def is_aware(self) -> bool:
        """Checks if the representative datetime is timezone-aware."""
        return self.representative_value.tzinfo is not None

    def __repr__(self) -> str:
        return (f"DateTimeCandidate(value='{self.representative_value.isoformat()}',"
                f" keys={self.source_keys}, ids={self.source_ids})")

class DateTimeCandidateContainer:
    """Manages a collection of DateTimeCandidate objects, merging them on the fly."""

    def __init__(self, tolerance: timedelta = timedelta(seconds=5)):
        self._candidates: List[DateTimeCandidate] = []
        self.tolerance = tolerance

    @property
    def candidates(self) -> List[DateTimeCandidate]:
        """Returns a copy of all current candidates."""
        return list(self._candidates)

    @property
    def aware_candidates(self) -> List[DateTimeCandidate]:
        """Returns all timezone-aware candidates."""
        return [c for c in self._candidates if c.is_aware]

    @property
    def naive_candidates(self) -> List[DateTimeCandidate]:
        """Returns all naive (timezone-unaware) candidates."""
        return [c for c in self._candidates if not c.is_aware]

    def _is_match(self, cand1: DateTimeCandidate, cand2: DateTimeCandidate) -> bool:
        """Checks if two candidates match based on time and timezone within a tolerance."""
        if cand1.is_aware != cand2.is_aware:
            return False  # An aware and a naive time can never match.

        # For aware times, compare their absolute time in UTC and their offset
        if cand1.is_aware:
            utc1 = cand1.representative_value.astimezone(timezone.utc)
            utc2 = cand2.representative_value.astimezone(timezone.utc)
            offsets_match = cand1.representative_value.utcoffset() == cand2.representative_value.utcoffset()
            return abs(utc1 - utc2) <= self.tolerance and offsets_match
        # For naive times, just compare their values directly
        else:
            return abs(cand1.representative_value - cand2.representative_value) <= self.tolerance

    def add_candidate(self, new_candidate: DateTimeCandidate):
        """
        Adds a new candidate to the container. If it matches one or more existing
        candidates, they are all merged into a single new candidate.
        """
        # Find all existing candidates that match the new one
        matching_candidates = [c for c in self._candidates if self._is_match(c, new_candidate)]

        if not matching_candidates:
            # No matches found, just add the new candidate
            self._candidates.append(new_candidate)
            return

        # --- Merge Logic ---
        # Include the new candidate in the list of items to be merged
        all_to_merge = matching_candidates + [new_candidate]

        # 1. Choose the earliest representative date
        merged_repr_value = min(c.representative_value for c in all_to_merge)

        # 2. Combine all source keys and IDs into new sets
        merged_keys = set()
        merged_ids = set()
        for c in all_to_merge:
            merged_keys.update(c.source_keys)
            merged_ids.update(c.source_ids)

        # 3. Create the new, merged candidate
        merged_candidate = DateTimeCandidate(
            representative_value=merged_repr_value,
            source_keys=merged_keys,
            source_ids=merged_ids
        )

        # 4. Remove all the old candidates that were part of the merge
        self._candidates = [c for c in self._candidates if c not in matching_candidates]

        # 5. Add the single merged candidate
        self._candidates.append(merged_candidate)

    def __repr__(self) -> str:
        return f"DateTimeCandidateContainer(candidates={self._candidates})"

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

    UTC_KEYS = {
        "QuickTime:CreationDate",
        "QuickTime:CreateDate",
        "QuickTime:ModifyDate",
        "Composite:GPSDateTime",
        "Keys:CreationDate",
        "google:photoTakenTime",
    }

    def __init__(self, date_type: str):
        if date_type not in ("taken", "modified"):
            raise ValueError("date_type must be 'taken' or 'modified'")
        self.date_type = date_type
        self.tz_finder = TimezoneFinder()

    def _get_metadata_keys(self) -> List[str | tuple[str, str]]:
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

    def _get_filename_date_candidates(self, context: MergeContext) -> List[DateTimeCandidate] | None:
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
        return [DateTimeCandidate.from_entry(e) for e in file_name_entries if e.value_dt is not None]

    def _get_candidate(self, key: str|tuple[str,str], entries: List[models.MetadataEntry]) -> DateTimeCandidate | None:

        def _get_value_from_key_and_entries(key: str, entries: List[models.MetadataEntry]) -> Any:
            for ent in entries:
                if ent.key == key:
                    return ent.value

        if isinstance(key, str):
            for e in entries:
                if e.key == key and e.value_dt is not None:
                    if e.value_dt.tzinfo is not None or e.key not in self.UTC_KEYS:
                        return DateTimeCandidate.from_entry(e)
                    else:
                        # If the key is a known UTC key but the datetime is naive, localize it to UTC
                        utc_dt = e.value_dt.replace(tzinfo=timezone.utc)
                        return DateTimeCandidate(
                            representative_value=utc_dt,
                            source_keys={e.key},
                            source_ids={e.id}
                        )

        elif isinstance(key, tuple) and len(key) == 2:
            first_key, second_key = key

            first_value = _get_value_from_key_and_entries(first_key, entries)
            second_value = _get_value_from_key_and_entries(second_key, entries)

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
                        date = first_value.replace(tzinfo=tzinfo)

                        return DateTimeCandidate(
                            representative_value=date,
                            source_keys={key},
                            source_ids={e.id for e in entries if e.key in key}
                        )

        return None

    def _get_candidate_container(self, keys: List[str|tuple[str,str]], entries: List[models.MetadataEntry]) -> DateTimeCandidateContainer:
        container = DateTimeCandidateContainer()

        for key in keys:
            candidate = self._get_candidate(key, entries)
            if candidate:
                container.add_candidate(candidate)

        return container

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
        # STEP 1: Gather all datetime candidates from metadata entries
        candidate_container = self._get_candidate_container(self._get_metadata_keys(), context.entries)

        # Also consider dates inferred from filenames, if date_type is taken
        if self.date_type == "taken":
            filename_candidates = self._get_filename_date_candidates(context)
            if filename_candidates:
                for fc in filename_candidates:
                    candidate_container.add_candidate(fc)

        inferred_tz = self.infer_timezone(context)
        aware_candidates = candidate_container.aware_candidates
        naive_candidates = candidate_container.naive_candidates

        value = None
        if aware_candidates:
            value = self._resolve_with_aware(context, aware_candidates, naive_candidates, inferred_tz)
        else:
            value = self._resolve_with_naive_only(context, naive_candidates, inferred_tz)

        if value:
            export_arg = DateTimeArgument(value, self.date_type)
            context.set_value(self.date_type, export_arg)

    def _resolve_with_aware(self, context: MergeContext, aware_candidates: List[DateTimeCandidate],
                            naive_candidates: List[DateTimeCandidate], inferred_tz: ZoneInfo | None):

        # 0. There should be at least one aware candidate here
        if not aware_candidates:
            context.record_conflict(self.date_type, "No timezone-aware datetime candidates available for resolution.")
            return None

        # 1. if multiple aware candidates, conflict, as these should have been merged already
        if len(aware_candidates) > 1:
            context.record_conflict(self.date_type, f"Multiple distinct timezone-aware datetime candidates found: {aware_candidates}")
            return None


        primary_candidate = aware_candidates[0]

        # The primary candidate is assumed to be in UTC if all its source keys are in UTC_KEYS and its tzinfo is UTC
        primary_candidate_timezone_assumed_utc = primary_candidate.representative_value.tzinfo is timezone.utc \
                                                 and primary_candidate.source_keys.intersection(self.UTC_KEYS) == primary_candidate.source_keys

        # 2. Check the primary candidate against the gps inferred timezone if available
        if inferred_tz:
            # 2.1 If the primary candidate is assumed to be in UTC, convert it to the inferred timezone
            # 2.2 If not, then the primary candidate is already in a local timezone, so it needs to match the inferred timezone
            if primary_candidate_timezone_assumed_utc:
                primary_candidate.representative_value = primary_candidate.representative_value.astimezone(inferred_tz)
                primary_candidate_timezone_assumed_utc = False
            else:
                if primary_candidate.representative_value.tzinfo != inferred_tz:
                    context.record_conflict(self.date_type, f"The timezone-aware datetime candidate '{primary_candidate}' has a different timezone than the GPS-inferred timezone '{inferred_tz.key}'.")
                    return None

        # 3. If there are no naive candidates, we can return the primary candidate as is
        if not naive_candidates:
            return primary_candidate.representative_value

        # 4. If primary timezone is still assumed to be UTC, we can try to infer the offset from naive candidates
        #    If not, we need to validate the naive candidates against the primary candidate's local and utc time
        if primary_candidate_timezone_assumed_utc:
            offset = self._infer_timezone_offset_from_naive_candidates(primary_candidate, naive_candidates, context)

            if offset:
                tz = timezone(offset)
                primary_candidate.representative_value = primary_candidate.representative_value.astimezone(tz)
            else:
                return None
        else:
            if not self._validate_aware_against_naive(primary_candidate, naive_candidates, context):
                return None


        return primary_candidate.representative_value

    def _infer_timezone_offset_from_naive_candidates(self, aware_cand: DateTimeCandidate, naive_candidates: List[DateTimeCandidate], context: MergeContext) -> timedelta | None:
        offsets = set()

        # for each naive candidate, calculate the offset from the aware candidate's UTC time
        # The offset should be in x hours with a tolerance of 5 seconds
        for nc in naive_candidates:
            offset = nc.representative_value - aware_cand.representative_value.astimezone(timezone.utc).replace(tzinfo=None)

            hours = float(offset.total_seconds()) / 3600.0
            if not (-12 <= hours <= 14):
                context.record_conflict(self.date_type, f"The naive datetime candidate '{nc}' implies an invalid timezone offset of {offset} from the timezone-aware candidate '{aware_cand}'. Valid offsets are between -12 and +14 hours.")
                return None

            # offset, amount of seconds shy of a whole 15 minutes
            offset_dist_from_quarter_hour = abs(offset.total_seconds() % 900)

            if 60 <offset_dist_from_quarter_hour < 830:
                context.record_conflict(self.date_type, f"The naive datetime candidate '{nc}' implies a timezone offset of {offset} from the timezone-aware candidate '{aware_cand}', which is not a whole number of 15-minute increments (with a tolerance of 1 minute).")
                return None

            # round to nearest 15 minutes
            if offset_dist_from_quarter_hour <= 60:
                offset -= timedelta(seconds=offset_dist_from_quarter_hour)
            elif offset_dist_from_quarter_hour >= 830:
                offset += timedelta(seconds=(900 - offset_dist_from_quarter_hour))

            offsets.add(offset)
        if len(offsets) == 1:
            return offsets.pop()
        else:
            context.record_conflict(self.date_type, f"The naive datetime candidates {naive_candidates} imply multiple different timezone offsets {sorted(list(offsets))} from the timezone-aware candidate '{aware_cand}'. Cannot infer a unique timezone offset.")
            return None

    def _validate_aware_against_naive(self, aware_cand: DateTimeCandidate, naive_candidates: List[DateTimeCandidate], context: MergeContext) -> bool:
        for nc in naive_candidates:
            # 3.1 If the naive candidate matches the primary candidate's local time, it's fine
            if abs(nc.representative_value - aware_cand.representative_value.replace(tzinfo=None)) <= timedelta(seconds=30):
                continue
            # 3.2 If the naive candidate matches the primary candidate's UTC time, it's also fine
            elif abs(nc.representative_value - aware_cand.representative_value.astimezone(timezone.utc).replace(tzinfo=None)) <= timedelta(seconds=30):
                continue
            else:
                context.record_conflict(self.date_type, f"The naive datetime candidate '{nc}' does not match the timezone-aware candidate '{aware_cand}' in either local or UTC time (with tolerance of 30 seconds).")
                return False
        return True

    def _resolve_with_naive_only(self, context: MergeContext, naive_candidates: List[DateTimeCandidate], inferred_tz: ZoneInfo | None):
        # 0. There should be at least one naive candidate here
        if not naive_candidates:
            return None

        # 1. if only one naive candidate, use it
        if len(naive_candidates) == 1:
            single_value = naive_candidates[0].representative_value
            if inferred_tz:
                single_value = single_value.replace(tzinfo=inferred_tz)
            return single_value
        # 2. if 2 naive candidates, try to resolve with heuristic
        elif len(naive_candidates) == 2 and inferred_tz:
            cand1 = naive_candidates[0]
            cand2 = naive_candidates[1]
            return self._handle_naive_pair_with_gps(cand1, cand2, inferred_tz, context)

        # 3. Conflict
        else:
            context.record_conflict(self.date_type, f"Multiple distinct naive datetime candidates found: {naive_candidates}. Cannot resolve without GPS-inferred timezone.")
            return None

    def _handle_naive_pair_with_gps(
            self,
            cand1: DateTimeCandidate,
            cand2: DateTimeCandidate,
            gps_tz: ZoneInfo,
            context: MergeContext
    ) -> datetime | None:
        """Heuristic: Tries to see if one naive time is UTC and the other is local."""
        t1, t2 = cand1.representative_value, cand2.representative_value

        # Possibility 1: t1 is UTC, t2 is local
        if abs(t1.replace(tzinfo=timezone.utc).astimezone(gps_tz).replace(tzinfo=None) - t2) < timedelta(seconds=5):
            return t1.replace(tzinfo=timezone.utc).astimezone(gps_tz)

        # Possibility 2: t2 is UTC, t1 is local
        if abs(t2.replace(tzinfo=timezone.utc).astimezone(gps_tz).replace(tzinfo=None) - t1) < timedelta(seconds=5):
            return t2.replace(tzinfo=timezone.utc).astimezone(gps_tz)

        context.record_conflict(self.date_type,
                                f"The difference between naive times '{t1}' and '{t2}' cannot be explained by the GPS-inferred timezone '{gps_tz.key}'.")
        return None


class FallbackDateToGpsDateTimeStep(MergeStep):
    """
    Sets both the taken and modified dates to the GPSDateTime if no dates were found
    and no conflicts occurred during the initial date merges.
    This step should run AFTER the primary DateTimeAndZoneMergeStep for both 'taken' and 'modified'.
    """
    def __init__(self):
        self.tz_finder = TimezoneFinder()

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
        # 1. Check if a 'taken' date has already been successfully merged.
        if "taken" not in context.finalized_fields and "taken" not in context.conflicts:
            try:
                gps_datetime_arg = context.get_value("Composite:GPSDateTime", required=True)
            except RuntimeError:
                # This should not happen if the pipeline is ordered correctly, but it's a safe check.
                gps_datetime_arg = None

            if gps_datetime_arg and isinstance(gps_datetime_arg, datetime):
                inferred_tz = self.infer_timezone(context)
                if inferred_tz:
                    gps_datetime_arg = gps_datetime_arg.astimezone(inferred_tz)

                # Create a new ExportArgument for 'taken' using the value from 'Composite:GPSDateTime'.
                taken_arg = DateTimeArgument(gps_datetime_arg, "taken")

                # Set the final value for 'taken' in the context.
                context.set_value("taken", taken_arg)


class FallbackModifiedDateStep(MergeStep):
    """
    Sets the modified date to the taken date if no modified date was found
    and no conflicts occurred during the initial modified date merge.
    This step should run AFTER the primary DateTimeAndZoneMergeStep for 'modified'.
    """
    def process(self, context: MergeContext):
        # 1. Check if a 'modified' date has already been successfully merged.
        if "modified" in context.finalized_fields:
            return  # A value already exists, so do nothing.

        # 2. Check if the original 'modified' date step recorded a conflict.
        if "modified" in context.conflicts:
            return  # There was a conflict, so we should not apply a fallback.

        # 3. If we proceed, it means 'modified' is empty and conflict-free.
        #    Try to get the 'taken' date value, which should have been finalized by a previous step.
        try:
            taken_date_arg = context.get_value("taken", required=True)
        except RuntimeError:
            # This should not happen if the pipeline is ordered correctly, but it's a safe check.
            return

        if taken_date_arg and isinstance(taken_date_arg, DateTimeArgument):
            # Create a new ExportArgument for 'modified' using the value from 'taken'.
            fallback_value = taken_date_arg.value
            modified_arg = DateTimeArgument(fallback_value, "modified")

            # Set the final value for 'modified' in the context.
            context.set_value("modified", modified_arg)

# --- The Pipeline Orchestrator ---

class MergePipeline:
    def __init__(self, steps: List[MergeStep]):
        self.steps = steps

    @classmethod
    def get_default_pipeline(cls) -> 'MergePipeline':
        steps: List[MergeStep] = [
            GPSMergeStep(),
            BasicFieldMergeStep("Composite:GPSDateTime"),
            DateTimeAndZoneMergeStep("taken"),
            DateTimeAndZoneMergeStep("modified"),
            FallbackDateToGpsDateTimeStep(),
            FallbackModifiedDateStep(),
        ]
        return cls(steps)

    def run(self, sources: List[models.MetadataSource]) -> MergeContext:
        context = MergeContext(sources)
        for step in self.steps:
            step.process(context)
        return context