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
    
    def write_summary_report(self, chains: List[LockInvalidationChain], file: TextIO = sys.stdout, only_found: bool = False) -> None:
        """Writes aggregated summary report to the specified file.
        
        Args:
            chains: List of lock invalidation chains to report
            file: Output file stream
            only_found: If True, only include chains where culprit has been found
        """
        
        if not chains:
            file.write("No transaction lock invalidation events found\n")
            return
        
        # Filter chains if only_found is True
        filtered_chains = self._filter_chains(chains, only_found)
        
        if not filtered_chains:
            file.write("No transaction lock invalidation events with found culprits\n")
            return
        
        # Write header
        self._write_header(filtered_chains, file, only_found)
        
        # Aggregate combinations
        combinations = self._aggregate_combinations(filtered_chains, only_found)
        
        # Write aggregated results
        self._write_aggregated_results(combinations, file)
    
    def _filter_chains(self, chains: List[LockInvalidationChain], only_found: bool) -> List[LockInvalidationChain]:
        """Filters chains based on whether culprit has been found.
        
        Args:
            chains: List of all chains
            only_found: If True, only return chains where culprit has been found
            
        Returns:
            Filtered list of chains
        """
        if not only_found:
            return chains
        
        # Filter chains that have both victim and culprit queries
        return [chain for chain in chains if chain.victim_queries and chain.culprit_queries]
    
    def _write_header(self, chains: List[LockInvalidationChain], file: TextIO, only_found: bool = False) -> None:
        """Writes report header with metadata."""
        file.write("=" * 80 + "\n")
        if only_found:
            file.write("YDB Transaction Lock Invalidation (TLI) Aggregated Summary - Culprits Found\n")
        else:
            file.write("YDB Transaction Lock Invalidation (TLI) Aggregated Summary\n")
        file.write("=" * 80 + "\n")
        file.write(f"Generated at: {datetime.now().isoformat()}\n")
        file.write(f"Total invalidation events: {len(chains)}\n")
        file.write("=" * 80 + "\n\n")
    
    def _aggregate_combinations(self, chains: List[LockInvalidationChain], only_found: bool = False) -> Dict[Tuple[int, int], List[LockInvalidationChain]]:
        """Aggregates chains by victim+culprit hash combinations.
        
        Args:
            chains: List of chains to aggregate
            only_found: If True, skip chains without culprits (already filtered, but double-check)
        """
        
        combinations = defaultdict(list)
        
        for chain in chains:
            # For only_found mode, skip chains without both victim and culprit queries
            # For all events mode, we need to handle chains without culprits differently
            if only_found:
                if not chain.victim_queries or not chain.culprit_queries:
                    continue
                victim_hash = chain.get_victim_hash()
                culprit_hash = chain.get_culprit_hash()
                combination_key = (victim_hash, culprit_hash)
            else:
                # For all events, use victim hash and culprit hash (or 0 if no culprit)
                victim_hash = chain.get_victim_hash() if chain.victim_queries else 0
                culprit_hash = chain.get_culprit_hash() if chain.culprit_queries else 0
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
                file.write("  CULPRIT NOT FOUND\n")
            
            file.write("\n")
            
            # Write additional details
            file.write("DETAILS:\n")
            file.write(f"  Table: {representative_chain.table_name}\n")
            if victim_hash != 0:
                file.write(f"  Victim Hash: {victim_hash}\n")
            if culprit_hash != 0:
                file.write(f"  Culprit Hash: {culprit_hash}\n")
            if victim_hash == culprit_hash and victim_hash != 0:
                file.write(f"  Victim and cuplrit are different instances of the same transaction.\n")
            
            # Show timestamps 
            timestamps = [chain.victim_entry.timestamp for chain in chain_list]
            timestamps.sort()
            file.write(f"  First occurrence: {timestamps[0]}\n")
            file.write(f"  Last occurrence: {timestamps[-1]}\n")
            
            
            file.write("\n")