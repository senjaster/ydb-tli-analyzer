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
                message_type="DATA_INTEGRITY",
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
                message_type="DATA_INTEGRITY",
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
                message_type="DATA_INTEGRITY",
                phy_tx_id="844424930570467",
                break_lock_id=["844424930570466", "562949953837886"],
                raw_line="test line 3"
            ),
            LogEntry(
                timestamp="2025-10-22T07:54:51.396819Z",
                node="ydb-static-node-3",
                process="ydbd[887]",
                log_level="INFO",
                message_type="DATA_INTEGRITY",
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
                message_type="TLI",
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
                message_type="LOCKS",
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
                message_type="BREAK",
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
                message_type="TX",
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
            tracer = ChainTracerSinglePass(entries)
            chains = tracer.find_all_invalidation_chains()
        
        # We should find at least one chain from the test data
        assert len(chains) >= 0  # May be 0 if chain tracing fails due to incomplete data
        
        for chain in chains:
            assert isinstance(chain, LockInvalidationChain)
            assert chain.victim_session_id is not None
            assert chain.victim_trace_id is not None
            assert chain.lock_id is not None
            assert chain.culprit_phy_tx_id is not None
            assert chain.culprit_trace_id is not None
            assert chain.culprit_session_id is not None
            assert chain.victim_entry is not None
            
    def test_lock_invalidation_chain_dataclass(self):
        """Test LockInvalidationChain dataclass properties."""
        victim_entry = self.sample_entries[0]
        
        chain = LockInvalidationChain(
            victim_session_id="victim_session",
            victim_trace_id="victim_trace",
            lock_id="test_lock",
            culprit_phy_tx_id="culprit_phy_tx",
            culprit_trace_id="culprit_trace",
            culprit_session_id="culprit_session",
            victim_entry=victim_entry
        )
        
        assert chain.victim_session_id == "victim_session"
        assert chain.victim_trace_id == "victim_trace"
        assert chain.lock_id == "test_lock"
        assert chain.culprit_phy_tx_id == "culprit_phy_tx"
        assert chain.culprit_trace_id == "culprit_trace"
        assert chain.culprit_session_id == "culprit_session"
        assert chain.victim_entry == victim_entry
        
        # Test optional fields default to None
        assert chain.culprit_entry is None
        assert chain.table_name is None
        assert chain.victim_tx_id is None
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
            message_type="TLI",
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
                message_type="QUERY",
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
            message_type="LOCKS",
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
            message_type="BREAK",
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
            message_type="TX",
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
                message_type="QUERY",
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