"""
TC-IAM Event Adapter
====================

Adapter for converting tc-iam event format to tc-nats-events Event format.
"""

import json
import logging
from typing import Any, Dict

from ..models.event import Event, EventMetadata
from .base_adapter import EventAdapter

logger = logging.getLogger(__name__)


class TCIamAdapter(EventAdapter):
    """
    Adapter for tc-iam event format.

    tc-iam format:
    {
        "event_type": "teamcore.tenant.registration",
        "environment": "localhost",
        "timestamp": "2025-06-11T16:18:24.602161Z",
        "payload": "{\"tenantShortName\": \"tst3\", \"tenantID\": 1}"
    }

    tc-nats-events format:
    {
        "event_type": "teamcore.tenant.registration",
        "data": {"tenantShortName": "tst3", "tenantID": 1},
        "timestamp": "2025-06-11T16:18:24.602161Z",
        "metadata": {...}
    }
    """

    def can_handle(self, raw_data: bytes) -> bool:
        """
        Check if this looks like a tc-iam event.

        tc-iam events have 'payload' field instead of 'data'.
        """
        try:
            data = json.loads(raw_data.decode("utf-8"))

            # tc-iam events have these characteristics:
            # 1. Have 'payload' field (not 'data')
            # 2. Have 'event_type' field
            # 3. Often have 'environment' field
            # 4. event_type often starts with 'teamcore.'

            has_payload = "payload" in data
            has_event_type = "event_type" in data
            has_environment = "environment" in data
            is_teamcore_event = data.get("event_type", "").startswith("teamcore.")

            # Strong indicators this is a tc-iam event
            return (
                has_payload
                and has_event_type
                and (has_environment or is_teamcore_event)
            )

        except (json.JSONDecodeError, UnicodeDecodeError):
            return False

    def adapt_to_event(self, raw_data: bytes) -> Event:
        """
        Convert tc-iam event to tc-nats-events Event format.
        """
        try:
            tc_iam_data = json.loads(raw_data.decode("utf-8"))

            # Extract tc-iam fields
            event_type = tc_iam_data.get("event_type", "unknown")
            environment = tc_iam_data.get("environment", "unknown")
            timestamp = tc_iam_data.get("timestamp")
            payload_raw = tc_iam_data.get("payload", "{}")

            # Parse the potentially multi-encoded payload
            payload = self._parse_tc_iam_payload(payload_raw)

            # Create metadata with tc-iam specific fields
            metadata = EventMetadata(environment=environment, source_service="tc-iam")

            # Create tc-nats-events compatible Event
            return self._create_safe_event(
                event_type=event_type,
                data=payload,  # tc-iam 'payload' becomes tc-nats-events 'data'
                timestamp=timestamp,
                metadata=metadata,
            )

        except Exception as e:
            logger.error(f"Failed to adapt tc-iam event: {e}")
            # Create error event
            return self._create_safe_event(
                event_type="tc_iam.adapter.error",
                data={
                    "error": str(e),
                    "raw_data": raw_data.decode("utf-8", errors="replace"),
                },
            )

    def _parse_tc_iam_payload(self, payload_raw: Any) -> Dict[str, Any]:
        """
        Parse tc-iam payload which can be multi-encoded JSON.

        tc-iam sometimes double or triple encodes JSON strings.
        """
        payload = payload_raw

        # Try to decode up to 3 levels of JSON encoding
        for attempt in range(3):
            try:
                if isinstance(payload, str):
                    decoded = json.loads(payload)
                    payload = decoded
                else:
                    break
            except json.JSONDecodeError:
                break

        # Ensure we return a dict
        if isinstance(payload, dict):
            return payload
        elif payload is None:
            return {}
        else:
            # If it's still not a dict, wrap it
            return {"value": payload}

    def get_adapter_name(self) -> str:
        """Get adapter name."""
        return "TC-IAM Adapter"
