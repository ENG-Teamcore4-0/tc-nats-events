"""
Generic Event Adapter
======================

Universal adapter that can handle any JSON event format by auto-detecting fields.
"""

import json
import logging
from typing import Any, Dict, Optional, Set

from ..models.event import Event, EventMetadata
from .base_adapter import EventAdapter

logger = logging.getLogger(__name__)


class GenericAdapter(EventAdapter):
    """
    Universal adapter that auto-detects event format and converts to tc-nats-events format.

    This adapter tries to intelligently map common field names to tc-nats-events format:
    - event_type, type, eventType, event-type → event_type
    - data, payload, body, content, message → data
    - timestamp, time, created_at, createdAt → timestamp
    - environment, env, source, origin → metadata.environment

    Configurable field mappings for maximum flexibility.
    """

    def __init__(
        self,
        event_type_fields: Optional[Set[str]] = None,
        data_fields: Optional[Set[str]] = None,
        timestamp_fields: Optional[Set[str]] = None,
        environment_fields: Optional[Set[str]] = None,
        default_event_type: str = "generic.event",
        auto_decode_json_strings: bool = True,
    ):
        """
        Initialize generic adapter with configurable field mappings.

        Args:
            event_type_fields: Field names that contain event type
            data_fields: Field names that contain event data
            timestamp_fields: Field names that contain timestamp
            environment_fields: Field names that contain environment info
            default_event_type: Default event type if none found
            auto_decode_json_strings: Try to decode JSON strings in data fields
        """
        # Default field mappings - covers most common patterns
        self.event_type_fields = event_type_fields or {
            "event_type",
            "type",
            "eventType",
            "event-type",
            "event_name",
            "name",
        }

        self.data_fields = data_fields or {
            "data",
            "payload",
            "body",
            "content",
            "message",
            "event_data",
            "details",
        }

        self.timestamp_fields = timestamp_fields or {
            "timestamp",
            "time",
            "created_at",
            "createdAt",
            "created",
            "date",
            "when",
        }

        self.environment_fields = environment_fields or {
            "environment",
            "env",
            "source",
            "origin",
            "service",
            "source_service",
        }

        self.default_event_type = default_event_type
        self.auto_decode_json_strings = auto_decode_json_strings

    def can_handle(self, raw_data: bytes) -> bool:
        """
        Generic adapter can handle valid JSON that's not already in tc-nats-events format.
        """
        try:
            data = json.loads(raw_data.decode("utf-8"))

            # Check if this is already in standard tc-nats-events format
            # Standard format has: event_type, data, timestamp (and maybe metadata)
            has_event_type = "event_type" in data
            has_data = "data" in data
            has_timestamp = "timestamp" in data

            # If it looks like standard format, don't handle it
            if has_event_type and has_data and has_timestamp:
                return False

            return True
        except (json.JSONDecodeError, UnicodeDecodeError):
            return False

    def adapt_to_event(self, raw_data: bytes) -> Event:
        """
        Convert any JSON event to tc-nats-events Event format using field mapping.
        """
        try:
            original_data = json.loads(raw_data.decode("utf-8"))

            # Auto-detect and extract fields
            event_type = self._extract_field(
                original_data, self.event_type_fields, self.default_event_type
            )
            data = self._extract_data_field(original_data)
            timestamp = self._extract_field(original_data, self.timestamp_fields)
            environment = self._extract_field(original_data, self.environment_fields)

            # Create metadata
            metadata = EventMetadata(
                environment=environment, source_service="auto-detected"
            )

            # Create event
            return self._create_safe_event(
                event_type=event_type, data=data, timestamp=timestamp, metadata=metadata
            )

        except Exception as e:
            logger.error(f"Failed to adapt generic event: {e}")
            # Create error event with original data
            try:
                original_data = json.loads(raw_data.decode("utf-8", errors="replace"))
            except Exception:
                original_data = raw_data.decode("utf-8", errors="replace")

            return self._create_safe_event(
                event_type="generic.adapter.error",
                data={
                    "error": str(e),
                    "original_data": original_data,
                },
            )

    def _extract_field(
        self, data: Dict[str, Any], field_names: Set[str], default: Any = None
    ) -> Any:
        """Extract first matching field from data."""
        for field_name in field_names:
            if field_name in data:
                return data[field_name]
        return default

    def _extract_data_field(self, original_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract data field with intelligent handling.
        """
        # Try to find explicit data field
        data = self._extract_field(original_data, self.data_fields)

        if data is not None:
            # Found explicit data field, decode if it's a JSON string
            decoded_data = self._decode_if_json_string(data)
            return (
                decoded_data
                if isinstance(decoded_data, dict)
                else {"value": decoded_data}
            )

        # No explicit data field found, use entire original data
        # but exclude metadata fields to avoid duplication
        excluded_fields = (
            self.event_type_fields
            | self.timestamp_fields
            | self.environment_fields
            | {"metadata", "meta"}
        )

        filtered_data = {
            k: v for k, v in original_data.items() if k not in excluded_fields
        }

        # If everything was excluded, use original data
        return filtered_data if filtered_data else original_data

    def _decode_if_json_string(self, data: Any) -> Any:
        """
        Recursively decode JSON strings if auto_decode_json_strings is enabled.
        """
        if not self.auto_decode_json_strings:
            return data

        if isinstance(data, str):
            # Try to decode up to 3 levels of JSON encoding
            for _ in range(3):
                try:
                    decoded = json.loads(data)
                    data = decoded
                    # If decoded to non-string, stop trying
                    if not isinstance(data, str):
                        break
                except (json.JSONDecodeError, TypeError):
                    break

        return data

    def get_adapter_name(self) -> str:
        """Get adapter name."""
        return "Generic Universal Adapter"


class FlexibleAdapter(GenericAdapter):
    """
    Alias for GenericAdapter with maximum flexibility settings.

    This is the most permissive adapter that tries to handle any event format.
    """

    def __init__(self) -> None:
        super().__init__(
            # Very broad field matching
            event_type_fields={
                "event_type",
                "type",
                "eventType",
                "event-type",
                "event_name",
                "name",
                "action",
                "operation",
                "method",
                "command",
                "subject",
                "topic",
            },
            data_fields={
                "data",
                "payload",
                "body",
                "content",
                "message",
                "event_data",
                "details",
                "info",
                "properties",
                "attributes",
                "fields",
                "params",
                "args",
            },
            timestamp_fields={
                "timestamp",
                "time",
                "created_at",
                "createdAt",
                "created",
                "date",
                "when",
                "occurred_at",
                "happened_at",
                "event_time",
                "time_stamp",
                "datetime",
            },
            environment_fields={
                "environment",
                "env",
                "source",
                "origin",
                "service",
                "source_service",
                "from",
                "sender",
                "producer",
                "app",
                "application",
                "system",
            },
            default_event_type="flexible.event",
            auto_decode_json_strings=True,
        )

    def get_adapter_name(self) -> str:
        return "Flexible Universal Adapter"
