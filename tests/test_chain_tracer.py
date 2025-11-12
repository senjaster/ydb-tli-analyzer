#!/usr/bin/env python3
"""
Unit tests for ChainTracer class.
"""

import pytest
import os
from log_parser import LogParser, LogEntry
from chain_tracer_single_pass import ChainTracerSinglePass
from chain_models import LockInvalidationChain


class TestChainTracer:
    """Test cases for ChainTracer class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Create sample log entries for testing
        self.sample_entries = [
            LogEntry(
                timestamp="2025-10-22T07:54:51.433950Z",
                node="ydb-static-node-3",
                process="ydbd[889]",
                log_level="DEBUG",
                kikimr_service="DATA_INTEGRITY",
                session_id="ydb://session/3?node_id=50005&id=test_session",
                trace_id="01k85ekrmy9tcyvnx2qvwdkcts",
                tx_id="01k85ekret8vjt70bq09a16h6q",
                status="ABORTED",
                issues='message: "Transaction locks invalidated. Table: `/Root/database/test_schema_ca7eb8ed/tt1`" issue_code: 2001 severity: 1',
                raw_line="test line 1"
            ),
            LogEntry(
                timestamp="2025-10-22T07:54:51.425247Z",
                node="ydb-static-node-1",
                process="ydbd[846]",
                log_level="INFO",
                kikimr_service="DATA_INTEGRITY",
                trace_id="01k85ekrmy9tcyvnx2qvwdkcts",
                phy_tx_id="562949953837887",
                status="LOCKS_BROKEN",
                lock_id=["562949953837886"],
                raw_line="test line 2"
            ),
            LogEntry(
                timestamp="2025-10-22T07:54:51.399202Z",
                node="ydb-static-node-1",
                process="ydbd[846]",
                log_level="INFO",
                kikimr_service="DATA_INTEGRITY",
                phy_tx_id="844424930570467",
                break_lock_id=["844424930570466", "562949953837886"],
                raw_line="test line 3"
            ),
            LogEntry(
                timestamp="2025-10-22T07:54:51.396819Z",
                node="ydb-static-node-3",
                process="ydbd[887]",
                log_level="INFO",
                kikimr_service="DATA_INTEGRITY",
                trace_id="01k85ekrm1bx847jes53b6k9qb",
                phy_tx_id="844424930570467",
                session_id="ydb://session/3?node_id=50003&id=culprit_session",
                raw_line="test line 4"
            )
        ]
        
        self.tracer = ChainTracerSinglePass(self.sample_entries)
        
        
    def test_find_invalidated_entries(self):
        """Test finding invalidated entries by running chain analysis."""
        chains = self.tracer.find_all_invalidation_chains()
        
        # Should find one chain from the sample data
        assert len(chains) == 1
        assert chains[0].victim_session_id == "ydb://session/3?node_id=50005&id=test_session"
        assert chains[0].victim_entry.status == "ABORTED"
        assert "Transaction locks invalidated" in chains[0].victim_entry.issues
        
        
    @pytest.mark.parametrize("issues,expected_table", [
        (
            'message: "Transaction locks invalidated. Table: `/Root/database/test_schema_ca7eb8ed/tt1`" issue_code: 2001 severity: 1',
            "/Root/database/test_schema_ca7eb8ed/tt1"
        ),
        (
            "Transaction locks invalidated. Table: `simple_table`",
            "simple_table"
        ),
        (
            "Transaction locks invalidated without table info",
            None
        )
    ])
    def test_table_name_extraction_in_chains(self, issues, expected_table):
        """Test that table names are correctly extracted in complete chains."""
        # Create a complete TLI scenario
        entries = [
            # TLI event
            LogEntry(
                timestamp="2025-10-22T07:54:51.300000Z",
                node="test-node",
                process="test[123]",
                log_level="ERROR",
                kikimr_service="TLI",
                session_id="victim_session",
                trace_id="victim_trace",
                tx_id="victim_tx",
                status="ABORTED",
                issues=issues,
                raw_line="test line"
            ),
            # LOCKS_BROKEN event
            LogEntry(
                timestamp="2025-10-22T07:54:51.250000Z",
                node="test-node",
                process="test[123]",
                log_level="DEBUG",
                kikimr_service="LOCKS",
                trace_id="victim_trace",
                status="LOCKS_BROKEN",
                lock_id=["12345"],
                raw_line="test line"
            ),
            # Break locks event
            LogEntry(
                timestamp="2025-10-22T07:54:51.200000Z",
                node="test-node",
                process="test[123]",
                log_level="DEBUG",
                kikimr_service="BREAK",
                phy_tx_id="67890",
                break_lock_id=["12345"],
                raw_line="test line"
            ),
            # Culprit transaction
            LogEntry(
                timestamp="2025-10-22T07:54:51.050000Z",
                node="test-node",
                process="test[123]",
                log_level="DEBUG",
                kikimr_service="TX",
                phy_tx_id="67890",
                trace_id="culprit_trace",
                session_id="culprit_session",
                tx_id="culprit_tx",
                raw_line="test line"
            )
        ]
        
        tracer = ChainTracerSinglePass(entries)
        chains = tracer.find_all_invalidation_chains()
        
        assert len(chains) == 1
        assert chains[0].table_name == expected_table
        
    def test_find_all_invalidation_chains_with_fixture(self):
        """Test finding chains with real log data."""
        fixture_path = os.path.join(os.path.dirname(__file__), 'fixtures', 'test_log.log')
        
        if not os.path.exists(fixture_path):
            pytest.skip(f"Test fixture not found: {fixture_path}")
            
        parser = LogParser()
        with open(fixture_path, 'r', encoding='utf-8') as f:
            entries = parser.parse_stream(f)
            # Sort entries in reverse chronological order as expected by ChainTracerSinglePass
            sorted_entries = sorted(entries, key=lambda x: x.timestamp or '', reverse=True)
            tracer = ChainTracerSinglePass(sorted_entries)
            chains = tracer.find_all_invalidation_chains()
        
        # We should find at least one complete chain from the test data
        assert len(chains) >= 1
        
        for chain in chains:
            assert isinstance(chain, LockInvalidationChain)
            assert chain.victim_session_id is not None
            assert chain.victim_trace_id is not None
            assert chain.victim_tx_id is not None
            assert chain.victim_entry is not None
            assert chain.table_name is not None
            # These fields should be filled for complete chains
            assert chain.lock_id is not None
            assert chain.culprit_phy_tx_id is not None
            assert chain.culprit_trace_id is not None
            assert chain.culprit_session_id is not None
            
    def test_lock_invalidation_chain_dataclass(self):
        """Test LockInvalidationChain dataclass properties."""
        victim_entry = self.sample_entries[0]
        
        chain = LockInvalidationChain(
            victim_session_id="victim_session",
            victim_trace_id="victim_trace",
            victim_tx_id="victim_tx_id",
            victim_entry=victim_entry,
            table_name="test_table",
            lock_id="test_lock",
            culprit_phy_tx_id="culprit_phy_tx",
            culprit_trace_id="culprit_trace",
            culprit_session_id="culprit_session"
        )
        
        assert chain.victim_session_id == "victim_session"
        assert chain.victim_trace_id == "victim_trace"
        assert chain.victim_tx_id == "victim_tx_id"
        assert chain.table_name == "test_table"
        assert chain.lock_id == "test_lock"
        assert chain.culprit_phy_tx_id == "culprit_phy_tx"
        assert chain.culprit_trace_id == "culprit_trace"
        assert chain.culprit_session_id == "culprit_session"
        assert chain.victim_entry == victim_entry
        
        # Test optional fields default to None
        assert chain.culprit_entry is None
        assert chain.culprit_tx_id is None
        assert chain.victim_queries is None
        assert chain.culprit_queries is None
        
    def test_empty_log_entries(self):
        """Test tracer with empty log entries."""
        empty_tracer = ChainTracerSinglePass([])
        
        chains = empty_tracer.find_all_invalidation_chains()
        assert len(chains) == 0
        
    @pytest.mark.parametrize("victim_query_count,culprit_query_count", [
        (1, 1),  # Single query scenario
        (2, 2),  # Multiple queries scenario
        (0, 0),  # No queries scenario
    ])
    def test_complete_chain_with_queries(self, victim_query_count, culprit_query_count):
        """Test that chains include all queries for victim and culprit transactions."""
        entries = []
        
        # TLI event (victim)
        entries.append(LogEntry(
            timestamp="2025-10-22T07:54:51.300000Z",
            node="test-node",
            process="test[123]",
            log_level="ERROR",
            kikimr_service="TLI",
            session_id="victim_session",
            trace_id="victim_trace",
            tx_id="victim_tx",
            status="ABORTED",
            issues="Transaction locks invalidated. Table: `test_table`",
            raw_line="test line"
        ))
        
        # Add victim queries
        for i in range(victim_query_count):
            entries.append(LogEntry(
                timestamp=f"2025-10-22T07:54:51.{100 + i * 10:06d}Z",
                node="test-node",
                process="test[123]",
                log_level="DEBUG",
                kikimr_service="QUERY",
                tx_id="victim_tx",
                query_text=f"SELECT * FROM test_table WHERE id={i}",
                query_action="QUERY_ACTION_EXECUTE",
                trace_id="victim_trace",
                raw_line="test line"
            ))
        
        # LOCKS_BROKEN event
        entries.append(LogEntry(
            timestamp="2025-10-22T07:54:51.250000Z",
            node="test-node",
            process="test[123]",
            log_level="DEBUG",
            kikimr_service="LOCKS",
            trace_id="victim_trace",
            status="LOCKS_BROKEN",
            lock_id=["12345"],
            raw_line="test line"
        ))
        
        # Break locks event
        entries.append(LogEntry(
            timestamp="2025-10-22T07:54:51.200000Z",
            node="test-node",
            process="test[123]",
            log_level="DEBUG",
            kikimr_service="BREAK",
            phy_tx_id="67890",
            break_lock_id=["12345"],
            raw_line="test line"
        ))
        
        # Culprit transaction
        entries.append(LogEntry(
            timestamp="2025-10-22T07:54:51.050000Z",
            node="test-node",
            process="test[123]",
            log_level="DEBUG",
            kikimr_service="TX",
            phy_tx_id="67890",
            trace_id="culprit_trace",
            session_id="culprit_session",
            tx_id="culprit_tx",
            raw_line="test line"
        ))
        
        # Add culprit queries
        for i in range(culprit_query_count):
            entries.append(LogEntry(
                timestamp=f"2025-10-22T07:54:51.{i * 10:06d}Z",
                node="test-node",
                process="test[123]",
                log_level="DEBUG",
                kikimr_service="QUERY",
                tx_id="culprit_tx",
                query_text=f"INSERT INTO test_table VALUES ({i})",
                query_action="QUERY_ACTION_EXECUTE",
                trace_id="culprit_trace",
                raw_line="test line"
            ))

        tracer = ChainTracerSinglePass(entries)
        chains = tracer.find_all_invalidation_chains()
        
        assert len(chains) == 1
        chain = chains[0]
        
        # Verify chain structure
        assert chain.victim_session_id == "victim_session"
        assert chain.victim_trace_id == "victim_trace"
        assert chain.culprit_session_id == "culprit_session"
        assert chain.culprit_trace_id == "culprit_trace"
        assert chain.lock_id == "12345"
        assert chain.culprit_phy_tx_id == "67890"
        assert chain.table_name == "test_table"
        
        # Verify queries are collected
        if victim_query_count > 0:
            assert chain.victim_queries is not None
            assert len(chain.victim_queries) == victim_query_count
            # Verify queries are sorted by timestamp
            for i in range(len(chain.victim_queries) - 1):
                assert chain.victim_queries[i].timestamp <= chain.victim_queries[i + 1].timestamp
        else:
            assert chain.victim_queries == []
            
        if culprit_query_count > 0:
            assert chain.culprit_queries is not None
            assert len(chain.culprit_queries) == culprit_query_count
            # Verify queries are sorted by timestamp
            for i in range(len(chain.culprit_queries) - 1):
                assert chain.culprit_queries[i].timestamp <= chain.culprit_queries[i + 1].timestamp
        else:
            assert chain.culprit_queries == []
            
    def test_chain_tracer_edge_cases(self):
        """Test edge cases in chain tracing."""
        # Test with missing trace_id in TLI entry
        entries_no_trace = [
            LogEntry(
                timestamp="2025-10-22T07:54:51.300000Z",
                node="test-node",
                process="test[123]",
                log_level="ERROR",
                kikimr_service="TLI",
                session_id="victim_session",
                trace_id=None,  # Missing trace_id
                tx_id="victim_tx",
                status="ABORTED",
                issues="Transaction locks invalidated. Table: `test_table`",
                raw_line="test line"
            )
        ]
        
        tracer = ChainTracerSinglePass(entries_no_trace)
        chains = tracer.find_all_invalidation_chains()
        assert len(chains) == 0  # Should not create chain without trace_id
        
    def test_chain_tracer_missing_session_id(self):
        """Test chain creation with missing session_id."""
        entries_no_session = [
            LogEntry(
                timestamp="2025-10-22T07:54:51.300000Z",
                node="test-node",
                process="test[123]",
                log_level="ERROR",
                kikimr_service="TLI",
                session_id=None,  # Missing session_id
                trace_id="victim_trace",
                tx_id="victim_tx",
                status="ABORTED",
                issues="Transaction locks invalidated. Table: `test_table`",
                raw_line="test line"
            )
        ]
        
        tracer = ChainTracerSinglePass(entries_no_session)
        chains = tracer.find_all_invalidation_chains()
        assert len(chains) == 0  # Should not create chain without session_id
        
    def test_multiple_lock_ids_warning(self):
        """Test warning when multiple lock_ids are present."""
        entries = [
            # TLI event
            LogEntry(
                timestamp="2025-10-22T07:54:51.300000Z",
                node="test-node",
                process="test[123]",
                log_level="ERROR",
                kikimr_service="TLI",
                session_id="victim_session",
                trace_id="victim_trace",
                tx_id="victim_tx",
                status="ABORTED",
                issues="Transaction locks invalidated. Table: `test_table`",
                raw_line="test line"
            ),
            # LOCKS_BROKEN event with multiple lock_ids
            LogEntry(
                timestamp="2025-10-22T07:54:51.250000Z",
                node="test-node",
                process="test[123]",
                log_level="DEBUG",
                kikimr_service="LOCKS",
                trace_id="victim_trace",
                status="LOCKS_BROKEN",
                lock_id=["12345", "67890"],  # Multiple lock_ids
                raw_line="test line"
            )
        ]
        
        tracer = ChainTracerSinglePass(entries)
        chains = tracer.find_all_invalidation_chains()
        
        assert len(chains) == 1
        assert chains[0].lock_id == "12345"  # Should use first lock_id
        
    def test_duplicate_lock_id_warning(self):
        """Test warning when trying to set lock_id twice."""
        entries = [
            # TLI event
            LogEntry(
                timestamp="2025-10-22T07:54:51.300000Z",
                node="test-node",
                process="test[123]",
                log_level="ERROR",
                kikimr_service="TLI",
                session_id="victim_session",
                trace_id="victim_trace",
                tx_id="victim_tx",
                status="ABORTED",
                issues="Transaction locks invalidated. Table: `test_table`",
                raw_line="test line"
            ),
            # First LOCKS_BROKEN event
            LogEntry(
                timestamp="2025-10-22T07:54:51.250000Z",
                node="test-node",
                process="test[123]",
                log_level="DEBUG",
                kikimr_service="LOCKS",
                trace_id="victim_trace",
                status="LOCKS_BROKEN",
                lock_id=["12345"],
                raw_line="test line"
            ),
            # Second LOCKS_BROKEN event (duplicate)
            LogEntry(
                timestamp="2025-10-22T07:54:51.240000Z",
                node="test-node",
                process="test[123]",
                log_level="DEBUG",
                kikimr_service="LOCKS",
                trace_id="victim_trace",
                status="LOCKS_BROKEN",
                lock_id=["67890"],  # Different lock_id
                raw_line="test line"
            )
        ]
        
        tracer = ChainTracerSinglePass(entries)
        chains = tracer.find_all_invalidation_chains()
        
        assert len(chains) == 1
        assert chains[0].lock_id == "12345"  # Should keep first lock_id
        
    def test_culprit_phy_tx_same_as_victim(self):
        """Test skipping culprit when phy_tx_id matches victim."""
        entries = [
            # TLI event
            LogEntry(
                timestamp="2025-10-22T07:54:51.300000Z",
                node="test-node",
                process="test[123]",
                log_level="ERROR",
                kikimr_service="TLI",
                session_id="victim_session",
                trace_id="victim_trace",
                tx_id="victim_tx",
                status="ABORTED",
                issues="Transaction locks invalidated. Table: `test_table`",
                raw_line="test line"
            ),
            # LOCKS_BROKEN event
            LogEntry(
                timestamp="2025-10-22T07:54:51.250000Z",
                node="test-node",
                process="test[123]",
                log_level="DEBUG",
                kikimr_service="LOCKS",
                trace_id="victim_trace",
                status="LOCKS_BROKEN",
                lock_id=["12345"],
                phy_tx_id="same_phy_tx",
                raw_line="test line"
            ),
            # Break locks event with same phy_tx_id as victim
            LogEntry(
                timestamp="2025-10-22T07:54:51.200000Z",
                node="test-node",
                process="test[123]",
                log_level="DEBUG",
                kikimr_service="BREAK",
                phy_tx_id="same_phy_tx",  # Same as victim
                break_lock_id=["12345"],
                raw_line="test line"
            )
        ]
        
        tracer = ChainTracerSinglePass(entries)
        chains = tracer.find_all_invalidation_chains()
        
        assert len(chains) == 1
        assert chains[0].lock_id == "12345"
        assert chains[0].victim_phy_tx_id == "same_phy_tx"
        assert chains[0].culprit_phy_tx_id is None  # Should be skipped
        
    def test_query_collection_without_tx_id(self):
        """Test query collection when query has no tx_id."""
        entries = [
            # TLI event
            LogEntry(
                timestamp="2025-10-22T07:54:51.300000Z",
                node="test-node",
                process="test[123]",
                log_level="ERROR",
                kikimr_service="TLI",
                session_id="victim_session",
                trace_id="victim_trace",
                tx_id="victim_tx",
                status="ABORTED",
                issues="Transaction locks invalidated. Table: `test_table`",
                raw_line="test line"
            ),
            # Begin transaction
            LogEntry(
                timestamp="2025-10-22T07:54:51.250000Z",
                node="test-node",
                process="test[123]",
                log_level="DEBUG",
                kikimr_service="TX",
                session_id="victim_session",
                trace_id="victim_trace",
                tx_id="victim_tx",
                begin_tx=True,
                raw_line="test line"
            ),
            # Query without tx_id but with session_id
            LogEntry(
                timestamp="2025-10-22T07:54:51.200000Z",
                node="test-node",
                process="test[123]",
                log_level="DEBUG",
                kikimr_service="QUERY",
                session_id="victim_session",
                trace_id="victim_trace",
                tx_id=None,  # No tx_id
                query_text="SELECT * FROM test_table",
                query_action="QUERY_ACTION_EXECUTE",
                raw_line="test line"
            )
        ]
        
        tracer = ChainTracerSinglePass(entries)
        chains = tracer.find_all_invalidation_chains()
        
        assert len(chains) == 1
        # Query should be inferred and collected
        assert chains[0].victim_queries is not None
        assert len(chains[0].victim_queries) == 1
        assert chains[0].victim_queries[0].query_text == "SELECT * FROM test_table"
        
    def test_query_collection_no_session_no_tx(self):
        """Test query collection when query has neither tx_id nor session_id."""
        entries = [
            # TLI event
            LogEntry(
                timestamp="2025-10-22T07:54:51.300000Z",
                node="test-node",
                process="test[123]",
                log_level="ERROR",
                kikimr_service="TLI",
                session_id="victim_session",
                trace_id="victim_trace",
                tx_id="victim_tx",
                status="ABORTED",
                issues="Transaction locks invalidated. Table: `test_table`",
                raw_line="test line"
            ),
            # Query without tx_id and session_id
            LogEntry(
                timestamp="2025-10-22T07:54:51.200000Z",
                node="test-node",
                process="test[123]",
                log_level="DEBUG",
                kikimr_service="QUERY",
                session_id=None,  # No session_id
                trace_id="victim_trace",
                tx_id=None,  # No tx_id
                query_text="SELECT * FROM test_table",
                query_action="QUERY_ACTION_EXECUTE",
                raw_line="test line"
            )
        ]
        
        tracer = ChainTracerSinglePass(entries)
        chains = tracer.find_all_invalidation_chains()
        
        assert len(chains) == 1
        # Query should not be collected
        assert chains[0].victim_queries == []
        
    def test_culprit_tx_id_same_as_phy_tx_id(self):
        """Test skipping tx_id when it matches phy_tx_id."""
        entries = [
            # TLI event
            LogEntry(
                timestamp="2025-10-22T07:54:51.300000Z",
                node="test-node",
                process="test[123]",
                log_level="ERROR",
                kikimr_service="TLI",
                session_id="victim_session",
                trace_id="victim_trace",
                tx_id="victim_tx",
                status="ABORTED",
                issues="Transaction locks invalidated. Table: `test_table`",
                raw_line="test line"
            ),
            # LOCKS_BROKEN event
            LogEntry(
                timestamp="2025-10-22T07:54:51.250000Z",
                node="test-node",
                process="test[123]",
                log_level="DEBUG",
                kikimr_service="LOCKS",
                trace_id="victim_trace",
                status="LOCKS_BROKEN",
                lock_id=["12345"],
                raw_line="test line"
            ),
            # Break locks event
            LogEntry(
                timestamp="2025-10-22T07:54:51.200000Z",
                node="test-node",
                process="test[123]",
                log_level="DEBUG",
                kikimr_service="BREAK",
                phy_tx_id="67890",
                break_lock_id=["12345"],
                raw_line="test line"
            ),
            # Culprit transaction with tx_id same as phy_tx_id
            LogEntry(
                timestamp="2025-10-22T07:54:51.050000Z",
                node="test-node",
                process="test[123]",
                log_level="DEBUG",
                kikimr_service="TX",
                phy_tx_id="67890",
                trace_id="culprit_trace",
                session_id="culprit_session",
                tx_id="67890",  # Same as phy_tx_id
                raw_line="test line"
            )
        ]
        
        tracer = ChainTracerSinglePass(entries)
        chains = tracer.find_all_invalidation_chains()
        
        assert len(chains) == 1
        assert chains[0].culprit_phy_tx_id == "67890"
        assert chains[0].culprit_tx_id is None  # Should be skipped
        
    def test_collect_details_flag(self):
        """Test collect_details flag functionality."""
        entries = [
            LogEntry(
                timestamp="2025-10-22T07:54:51.300000Z",
                node="test-node",
                process="test[123]",
                log_level="ERROR",
                kikimr_service="TLI",
                session_id="victim_session",
                trace_id="victim_trace",
                tx_id="victim_tx",
                status="ABORTED",
                issues="Transaction locks invalidated. Table: `test_table`",
                raw_line="TLI log line"
            )
        ]
        
        # Test with collect_details=True
        tracer = ChainTracerSinglePass(entries)
        chains = tracer.find_all_invalidation_chains(collect_details=True)
        
        assert len(chains) == 1
        assert chains[0].log_details is not None
        assert len(chains[0].log_details) == 1  # Should contain the TLI entry
        
        # Test with collect_details=False (default)
        tracer2 = ChainTracerSinglePass(entries)
        chains2 = tracer2.find_all_invalidation_chains(collect_details=False)
        
        assert len(chains2) == 1
        assert chains2[0].log_details is None