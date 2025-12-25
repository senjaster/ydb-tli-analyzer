#!/usr/bin/env python3
"""
SQL-script-like reporter for YDB transaction lock invalidation analysis.
Generates human-friendly SQL-script-like reports from analyzed chains.
"""

import sys
import textwrap
from typing import List, TextIO
from datetime import datetime
from chain_models import LockInvalidationChain


class SQLReporter:
    """Generates SQL-script-like reports from lock invalidation chains."""
    
    def __init__(self):
        pass
    
    def write_sql_report(self, chains: List[LockInvalidationChain], file: TextIO = sys.stdout) -> None:
        """Writes SQL-script-like report to the specified file."""
        
        if not chains:
            file.write("-- No transaction lock invalidation events found\n")
            return
        
        # Sort chains by timestamp
        sorted_chains = sorted(chains, key=lambda x: datetime.fromisoformat(x.victim_entry.timestamp))
        
        # Write header
        self._write_header(sorted_chains, file)
        
        # Write each TLI event
        for i, chain in enumerate(sorted_chains, 1):
            self._write_tli_event(chain, i, file)
    
    def _write_header(self, chains: List[LockInvalidationChain], file: TextIO) -> None:
        """Writes report header with metadata."""
        file.write("-- " + "=" * 120 + "\n")
        file.write("-- YDB Transaction Lock Invalidation (TLI) Analysis Report\n")
        file.write("-- " + "=" * 120 + "\n")
        file.write(f"-- Generated at: {datetime.now().isoformat()}\n")
        file.write(f"-- Total invalidation events: {len(chains)}\n")
        file.write("-- " + "=" * 120 + "\n\n")
    
    def _write_tli_event(self, chain: LockInvalidationChain, event_id: int, file: TextIO) -> None:
        """Writes a single TLI event with big header separator."""
        
        # Big header block with long separators
        file.write("-- " + "=" * 120 + "\n")
        file.write(f"-- TLI EVENT #{event_id}\n")
        file.write(f"-- Timestamp: {chain.victim_entry.timestamp}\n")
        if chain.table_name:
            file.write(f"-- Table: {chain.table_name}\n")
        file.write(f"-- Victim raw log: {chain.victim_entry.raw_line.strip()}\n")
        if chain.culprit_entry:
            file.write(f"-- Culprit raw log: {chain.culprit_entry.raw_line.strip()}\n")
        file.write("-- " + "=" * 120 + "\n\n")
        
        # Write victim section
        self._write_victim_section(chain, file)
        
        # Write culprit section
        self._write_culprit_section(chain, file)
        
        file.write("\n")
    
    def _write_victim_section(self, chain: LockInvalidationChain, file: TextIO) -> None:
        """Writes the victim section with queries."""
        
        file.write("-- " + "-" * 120 + "\n")
        file.write("-- VICTIM TRANSACTION\n")
        file.write("-- " + "-" * 120 + "\n")
        file.write(f"-- Session ID: {chain.victim_session_id}\n")
        if chain.victim_tx_id:
            file.write(f"-- Transaction ID: {chain.victim_tx_id}\n")
        if chain.victim_queries:
            file.write(f"-- Transaction Start: {chain.victim_queries[0].timestamp or 'unknown'}\n")
            file.write(f"-- Transaction End: {chain.victim_queries[-1].timestamp or 'unknown'}\n")

        file.write("\n")
        
        # Write victim queries
        if chain.victim_queries:
            for query_entry in chain.victim_queries:
                self._write_query_statement(query_entry, file)
        
        file.write("\n")
    
    def _write_culprit_section(self, chain: LockInvalidationChain, file: TextIO) -> None:
        """Writes the culprit section with queries."""
        
        file.write("-- " + "-" * 120 + "\n")
        file.write("-- CULPRIT TRANSACTION\n")
        file.write("-- " + "-" * 120 + "\n")
        file.write(f"-- Session ID: {chain.culprit_session_id}\n")
        if chain.culprit_tx_id:
            file.write(f"-- Transaction ID: {chain.culprit_tx_id}\n")
        file.write("\n")
        
        # Write culprit queries
        if chain.culprit_queries:
            for query_entry in chain.culprit_queries:
                self._write_query_statement(query_entry, file)
        
        file.write("\n")
    
    def _write_query_statement(self, query_entry, file: TextIO) -> None:
        """Writes a single query statement with timestamp separator."""
        
        # One-line separator with timestamp and dashes
        timestamp = query_entry.timestamp or 'unknown'
        sep = f"-- {timestamp} --- {query_entry.trace_id} " + "-" * 120
        file.write(sep[:123] + "\n")
        
        # Write the actual query
        if query_entry.query_text:
            query_text = textwrap.dedent(query_entry.query_text).strip()
            file.write(f"{query_text}")
            if not query_text.endswith(';'):
                file.write(";")
            file.write("\n")
        else:
            file.write(f"-- {query_entry.query_action}\n")
        
        file.write("\n")