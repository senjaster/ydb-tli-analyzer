#!/usr/bin/env python3
"""
Shared data models for chain tracing functionality.
"""

from typing import List, Optional
from ordered_set import OrderedSet
from dataclasses import dataclass
from log_parser import LogEntry


@dataclass
class TraceInfo:
    """Aggregated information extracted from all entries with the same trace_id."""
    query_text: Optional[str] = None
    tx_id: Optional[str] = None
    session_id: Optional[str] = None
    lock_id: Optional[List[str]] = None  # For LOCKS_BROKEN entries
    representative_entry: Optional[LogEntry] = None  # First entry with session_id


@dataclass
class LockInvalidationChain:
    """Полное описание всей информации, извлеченной из лога по одной ошибке TLI."""
    victim_session_id: str
    victim_trace_id: str
    victim_tx_id: str
    victim_entry: LogEntry
    table_name: str
    lock_id: Optional[str] = None
    culprit_phy_tx_id: Optional[str] = None
    culprit_trace_id: Optional[str] = None
    culprit_session_id: Optional[str] = None
    victim_phy_tx_id: Optional[str] = None
    culprit_entry: Optional[LogEntry] = None

    culprit_tx_id: Optional[str] = None
    victim_queries: Optional[List[LogEntry]] = None
    culprit_queries: Optional[List[LogEntry]] = None
    log_details: Optional[OrderedSet[str]] = None

    @property
    def is_victim_committed(self):
        return self.victim_queries[-1].query_action == "QUERY_ACTION_COMMIT_TX"