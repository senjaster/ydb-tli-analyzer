#!/usr/bin/env python3
"""
Aggregated summary reporter for YDB transaction lock invalidation analysis.
Generates aggregated summary of culprit+victim combinations with counts.
"""

import sys
from typing import List, Dict, Tuple, TextIO
from collections import defaultdict
from datetime import datetime
from chain_models import LockInvalidationChain


class SummaryReporter:
    """Generates aggregated summary reports from lock invalidation chains."""
    
    def __init__(self):
        pass
    
    def write_summary_report(self, chains: List[LockInvalidationChain], file: TextIO = sys.stdout) -> None:
        """Writes aggregated summary report to the specified file."""
        
        if not chains:
            file.write("No transaction lock invalidation events found\n")
            return
        
        # Write header
        self._write_header(chains, file)
        
        # Aggregate combinations
        combinations = self._aggregate_combinations(chains)
        
        # Write aggregated results
        self._write_aggregated_results(combinations, file)
    
    def _write_header(self, chains: List[LockInvalidationChain], file: TextIO) -> None:
        """Writes report header with metadata."""
        file.write("=" * 80 + "\n")
        file.write("YDB Transaction Lock Invalidation (TLI) Aggregated Summary\n")
        file.write("=" * 80 + "\n")
        file.write(f"Generated at: {datetime.now().isoformat()}\n")
        file.write(f"Total invalidation events: {len(chains)}\n")
        file.write("=" * 80 + "\n\n")
    
    def _aggregate_combinations(self, chains: List[LockInvalidationChain]) -> Dict[Tuple[int, int], List[LockInvalidationChain]]:
        """Aggregates chains by victim+culprit hash combinations."""
        
        combinations = defaultdict(list)
        
        for chain in chains:
            # Skip chains without both victim and culprit queries
            if not chain.victim_queries or not chain.culprit_queries:
                continue
            
            victim_hash = chain.get_victim_hash()
            culprit_hash = chain.get_culprit_hash()
            combination_key = (victim_hash, culprit_hash)
            
            combinations[combination_key].append(chain)
        
        return combinations
    
    def _write_aggregated_results(self, combinations: Dict[Tuple[int, int], List[LockInvalidationChain]], file: TextIO) -> None:
        """Writes the aggregated results sorted by count (descending)."""
        
        if not combinations:
            file.write("No valid victim+culprit combinations found\n")
            return
        
        # Sort by count (descending)
        sorted_combinations = sorted(combinations.items(), key=lambda x: len(x[1]), reverse=True)
        
        file.write(f"Found {len(sorted_combinations)} unique victim+culprit combinations:\n\n")
        
        for i, ((victim_hash, culprit_hash), chain_list) in enumerate(sorted_combinations, 1):
            count = len(chain_list)
            representative_chain = chain_list[0]  # Use first chain as representative
            
            file.write("-" * 80 + "\n")
            file.write(f"#{i} TLI Count: {count}\n")
            file.write("-" * 80 + "\n")
            
            # Write victim information
            file.write("VICTIM:\n")
            if representative_chain.victim_queries:
                for j, query in enumerate(representative_chain.victim_queries, 1):
                    if query.query_text:
                        # Clean and format query text, preserving full content
                        query_text = query.query_text.strip()
                        # Replace newlines and tabs with spaces for single-line display
                        query_text = query_text.replace('\n', ' ').replace('\t', ' ')
                        # Remove extra spaces
                        query_text = ' '.join(query_text.split())
                        file.write(f"  {j}. {query_text}\n")
                    else:
                        file.write(f"  {j}. {query.query_action}\n")
            else:
                file.write("  No victim queries available\n")
            
            file.write("\n")
            
            # Write culprit information
            file.write("CULPRIT:\n")
            if representative_chain.culprit_queries:
                for j, query in enumerate(representative_chain.culprit_queries, 1):
                    if query.query_text:
                        # Clean and format query text, preserving full content
                        query_text = query.query_text.strip()
                        # Replace newlines and tabs with spaces for single-line display
                        query_text = query_text.replace('\n', ' ').replace('\t', ' ')
                        # Remove extra spaces
                        query_text = ' '.join(query_text.split())
                        file.write(f"  {j}. {query_text}\n")
                    else:
                        file.write(f"  {j}. {query.query_action}\n")
            else:
                file.write("  No culprit queries available\n")
            
            file.write("\n")
            
            # Write additional details
            file.write("DETAILS:\n")
            file.write(f"  Table: {representative_chain.table_name}\n")
            file.write(f"  Victim Hash: {victim_hash}\n")
            file.write(f"  Culprit Hash: {culprit_hash}\n")
            if victim_hash == culprit_hash:
                file.write(f"  Victim and cuplrit are different instances of the same transaction.\n")
            
            # Show timestamps 
            timestamps = [chain.victim_entry.timestamp for chain in chain_list]
            timestamps.sort()
            file.write(f"  First occurrence: {timestamps[0]}\n")
            file.write(f"  Last occurrence: {timestamps[-1]}\n")
            
            
            file.write("\n")