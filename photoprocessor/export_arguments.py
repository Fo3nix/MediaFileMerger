# In a new file, e.g., photoprocessor/export_arguments.py
import abc
from datetime import datetime, timezone
from typing import List, Set


class ExportArgument(abc.ABC):
    """Abstract base class for an object that can build command-line arguments for a tool."""

    def __init__(self, value):
        self.value = value

    @abc.abstractmethod
    def build(self) -> List[str]:
        """Builds and returns a list of string arguments for the export tool."""
        pass

    @abc.abstractmethod
    def get_managed_tags(self) -> Set[str]:
        """Returns a set of the specific command-line tags this argument writes to."""
        pass


class SimpleArgument(ExportArgument):
    """Handles simple key-value pairs."""

    def __init__(self, tag: str, value: str):
        super().__init__(value)
        self.tag = tag

    def build(self) -> List[str]:
        if not self.value:
            return []
        return [f"-{self.tag}={self.value}"]

    def get_managed_tags(self) -> Set[str]:
        return {f"-{self.tag}"}


class DateTimeArgument(ExportArgument):
    """Handles the complex logic of writing a datetime to multiple EXIF/XMP tags."""

    def __init__(self, value: datetime, date_type: str):
        """date_type can be 'taken' or 'modified' to target different tags."""
        super().__init__(value)
        self.date_type = date_type

    def get_managed_tags(self) -> Set[str]:
        tags = set()
        if not self.value or not isinstance(self.value, datetime):
            return tags

        if self.date_type == "taken":
            tags.update({
                "-EXIF:DateTimeOriginal",
                "-EXIF:CreateDate",
                "-FileCreateDate",
            })
        elif self.date_type == "modified":
            tags.update({
                "-EXIF:ModifyDate",
                "-FileModifyDate",
            })

        if self.value.tzinfo:
            if self.date_type == "taken":
                tags.update({
                    "-EXIF:OffsetTimeOriginal",
                    "-XMP:DateTimeOriginal",
                    "-QuickTime:CreateDate",
                    "-Keys:CreationDate",
                })
            elif self.date_type == "modified":
                tags.update({
                    "-XMP:ModifyDate",
                    "-QuickTime:ModifyDate",
                })
        return tags

    def build(self) -> List[str]:
        if not self.value or not isinstance(self.value, datetime):
            return []

        args = []
        # Format for EXIF/File dates (local time, no offset)
        local_time_str = self.value.strftime('%Y:%m:%d %H:%M:%S')

        if self.date_type == "taken":
            args.extend([
                f"-EXIF:DateTimeOriginal={local_time_str}",
                f"-EXIF:CreateDate={local_time_str}",
                f"-FileCreateDate={local_time_str}",
            ])
        elif self.date_type == "modified":
            args.extend([
                f"-EXIF:ModifyDate={local_time_str}",
                f"-FileModifyDate={local_time_str}",
            ])

        # If the date is timezone-aware, write additional offset and UTC tags
        if self.value.tzinfo:
            offset_str = self.value.strftime('%z')
            offset_str_formatted = f"{offset_str[:3]}:{offset_str[3:]}"
            utc_date = self.value.astimezone(timezone.utc)
            utc_time_str = utc_date.strftime('%Y:%m:%d %H:%M:%S')

            if self.date_type == "taken":
                args.extend([
                    f"-EXIF:OffsetTimeOriginal={offset_str_formatted}",
                    f"-XMP:DateTimeOriginal={self.value.isoformat()}",
                    f"-QuickTime:CreateDate={utc_time_str}",
                    f"-Keys:CreationDate={utc_time_str}",
                ])
            elif self.date_type == "modified":
                args.extend([
                    f"-XMP:ModifyDate={self.value.isoformat()}",
                    f"-QuickTime:ModifyDate={utc_time_str}",
                ])

        return args