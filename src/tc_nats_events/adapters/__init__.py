"""
Event Format Adapters
======================

Adapters to convert different event formats to tc-nats-events Event format.
"""

from .base_adapter import EventAdapter
from .generic_adapter import FlexibleAdapter, GenericAdapter

__all__ = ["EventAdapter", "GenericAdapter", "FlexibleAdapter"]
