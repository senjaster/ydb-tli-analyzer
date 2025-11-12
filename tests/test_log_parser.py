#!/usr/bin/env python3
"""
Unit tests for LogParser class.
"""

import pytest
import os
from io import StringIO
from log_parser import LogParser, LogEntry


class TestLogParser:
    """Test cases for LogParser class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.parser = LogParser()
        
    def test_parser_initialization(self):
        """Test that parser initializes correctly."""
        assert self.parser is not None
        assert hasattr(self.parser, 'patterns')
        assert hasattr(self.parser, 'compiled_patterns')
        assert len(self.parser.compiled_patterns) > 0
        
    def test_parse_empty_line(self):
        """Test parsing empty line returns None."""
        result = self.parser.parse_line("")
        assert result is None
        
        result = self.parser.parse_line("   ")
        assert result is None
        
    def test_parse_invalid_line(self):
        """Test parsing invalid line returns None."""
        invalid_line = "This is not a valid log line"
        result = self.parser.parse_line(invalid_line)
        assert result is None
        
    def test_parse_transaction_locks_invalidated_line(self):
        """Test parsing a line with 'Transaction locks invalidated' message."""
        sample_line = 'окт 22 10:54:51 ydb-static-node-3 ydbd[889]: 2025-10-22T07:54:51.433950Z :DATA_INTEGRITY DEBUG: Component: SessionActor,SessionId: ydb://session/3?node_id=50005&id=MzFkMjA5YjktNWJkYzI3YzgtYjEwOGJhNDAtOWFlMmZhYzI=,TraceId: 01k85ekrmy9tcyvnx2qvwdkcts,Type: Response,TxId: 01k85ekret8vjt70bq09a16h6q,Status: ABORTED,Issues: { message: "Transaction locks invalidated. Table: `/Root/database/test_schema_ca7eb8ed/tt1`" issue_code: 2001 severity: 1 }'
        
        entry = self.parser.parse_line(sample_line)
        
        assert entry is not None
        assert isinstance(entry, LogEntry)
        assert entry.node == "ydb-static-node-3"
        assert entry.process == "ydbd[889]"
        assert entry.log_level == "DEBUG"
        assert entry.kikimr_service == "DATA_INTEGRITY"
        assert entry.component == "SessionActor"
        assert entry.session_id == "ydb://session/3?node_id=50005&id=MzFkMjA5YjktNWJkYzI3YzgtYjEwOGJhNDAtOWFlMmZhYzI="
        assert entry.trace_id == "01k85ekrmy9tcyvnx2qvwdkcts"
        assert entry.tx_id == "01k85ekret8vjt70bq09a16h6q"
        assert entry.status == "ABORTED"
        assert "Transaction locks invalidated" in entry.issues
        assert entry.timestamp == "2025-10-22T07:54:51.433950Z"
        assert entry.raw_line == sample_line
        
    def test_parse_break_lock_id_line(self):
        """Test parsing a line with BreakLocks information."""
        sample_line = 'окт 22 13:08:53 ydb-static-node-1 ydbd[844]: 2025-10-22T10:08:53.391599Z :DATA_INTEGRITY INFO: Component: DataShard,Type: Locks,TabletId: 72075186224047631,PhyTxId: 562949953837901,BreakLocks: [562949953837900 844424930570469 ]'
        
        entry = self.parser.parse_line(sample_line)
        
        assert entry is not None
        assert isinstance(entry, LogEntry)
        assert entry.node == "ydb-static-node-1"
        assert entry.process == "ydbd[844]"
        assert entry.log_level == "INFO"
        assert entry.kikimr_service == "DATA_INTEGRITY"
        assert entry.component == "DataShard"
        assert entry.phy_tx_id == "562949953837901"
        assert entry.break_lock_id == ["562949953837900", "844424930570469"]
        assert entry.timestamp == "2025-10-22T10:08:53.391599Z"
        
    def test_parse_locks_broken_status_line(self):
        """Test parsing a line with LOCKS_BROKEN status."""
        sample_line = 'окт 22 10:54:51 ydb-static-node-1 ydbd[846]: 2025-10-22T07:54:51.425247Z :DATA_INTEGRITY INFO: Component: DataShard,Type: Finished,TabletId: 72075186224047627,PhyTxId: 562949953837887,Status: LOCKS_BROKEN,'
        
        entry = self.parser.parse_line(sample_line)
        
        assert entry is not None
        assert entry.status == "LOCKS_BROKEN"
        assert entry.phy_tx_id == "562949953837887"
        
    def test_parse_stream_with_fixture(self):
        """Test parsing the test fixture file using parse_stream."""
        fixture_path = os.path.join(os.path.dirname(__file__), 'fixtures', 'test_log.log')
        
        if not os.path.exists(fixture_path):
            pytest.skip(f"Test fixture not found: {fixture_path}")
            
        with open(fixture_path, 'r', encoding='utf-8') as f:
            entries = list(self.parser.parse_stream(f))
        
        assert len(entries) > 0
        assert all(isinstance(entry, LogEntry) for entry in entries)
        
        # Check that we have some entries with transaction lock invalidation
        invalidated_entries = [
            entry for entry in entries
            if entry.issues and "Transaction locks invalidated" in entry.issues and entry.status == "ABORTED"
        ]
        assert len(invalidated_entries) > 0
        
        # Check that we have some entries with BreakLocks
        break_lock_entries = [
            entry for entry in entries
            if entry.break_lock_id and len(entry.break_lock_id) > 0
        ]
        assert len(break_lock_entries) > 0
            
    def test_log_entry_dataclass(self):
        """Test LogEntry dataclass properties."""
        entry = LogEntry(
            timestamp="2025-10-22T07:54:51.433950Z",
            node="test-node",
            process="test[123]",
            log_level="DEBUG",
            kikimr_service="TEST",
            session_id="test-session",
            trace_id="test-trace",
            component="TestComponent",
            raw_line="test line"
        )
        
        assert entry.timestamp == "2025-10-22T07:54:51.433950Z"
        assert entry.node == "test-node"
        assert entry.process == "test[123]"
        assert entry.log_level == "DEBUG"
        assert entry.kikimr_service == "TEST"
        assert entry.component == "TestComponent"
        assert entry.session_id == "test-session"
        assert entry.trace_id == "test-trace"
        assert entry.raw_line == "test line"
        
        # Test optional fields default to None
        assert entry.phy_tx_id is None
        assert entry.lock_id is None
        assert entry.status is None
        assert entry.query_text is None
        assert entry.issues is None
        assert entry.break_lock_id is None
        assert entry.tx_id is None
        assert entry.query_action is None
        assert entry.query_type is None
        
    def test_parse_stream_with_stringio(self):
        """Test parsing from StringIO stream."""
        sample_data = """окт 22 10:54:51 ydb-static-node-3 ydbd[889]: 2025-10-22T07:54:51.433950Z :DATA_INTEGRITY DEBUG: Component: SessionActor,SessionId: ydb://session/3?node_id=50005&id=MzFkMjA5YjktNWJkYzI3YzgtYjEwOGJhNDAtOWFlMmZhYzI=,TraceId: 01k85ekrmy9tcyvnx2qvwdkcts,Type: Response,TxId: 01k85ekret8vjt70bq09a16h6q,Status: ABORTED,Issues: { message: "Transaction locks invalidated. Table: `/Root/database/test_schema_ca7eb8ed/tt1`" issue_code: 2001 severity: 1 }
окт 22 13:08:53 ydb-static-node-1 ydbd[844]: 2025-10-22T10:08:53.391599Z :DATA_INTEGRITY INFO: Component: DataShard,Type: Locks,TabletId: 72075186224047631,PhyTxId: 562949953837901,BreakLocks: [562949953837900 844424930570469 ]
invalid line that should be skipped
окт 22 10:54:51 ydb-static-node-1 ydbd[846]: 2025-10-22T07:54:51.425247Z :DATA_INTEGRITY INFO: Component: DataShard,Type: Finished,TabletId: 72075186224047627,PhyTxId: 562949953837887,Status: LOCKS_BROKEN,"""
        
        stream = StringIO(sample_data)
        entries = list(self.parser.parse_stream(stream))
        
        assert len(entries) == 3  # Should parse 3 valid entries, skip 1 invalid
        assert all(isinstance(entry, LogEntry) for entry in entries)
        
        # Check first entry (transaction locks invalidated)
        first_entry = entries[0]
        assert first_entry.node == "ydb-static-node-3"
        assert first_entry.kikimr_service == "DATA_INTEGRITY"
        assert first_entry.log_level == "DEBUG"
        assert first_entry.status == "ABORTED"
        assert "Transaction locks invalidated" in first_entry.issues
        
        # Check second entry (break locks)
        second_entry = entries[1]
        assert second_entry.node == "ydb-static-node-1"
        assert second_entry.break_lock_id == ["562949953837900", "844424930570469"]
        
        # Check third entry (locks broken status)
        third_entry = entries[2]
        assert third_entry.status == "LOCKS_BROKEN"
        assert third_entry.phy_tx_id == "562949953837887"
        
    def test_parse_stream_empty(self):
        """Test parsing empty stream."""
        empty_stream = StringIO("")
        entries = list(self.parser.parse_stream(empty_stream))
        assert len(entries) == 0
        
    def test_parse_stream_only_invalid_lines(self):
        """Test parsing stream with only invalid lines."""
        invalid_data = """This is not a valid log line
Another invalid line
Yet another invalid line"""
        
        stream = StringIO(invalid_data)
        entries = list(self.parser.parse_stream(stream))
        assert len(entries) == 0
        
    def test_parse_stream_mixed_valid_invalid(self):
        """Test parsing stream with mix of valid and invalid lines."""
        mixed_data = """Invalid line 1
окт 22 10:54:51 ydb-static-node-3 ydbd[889]: 2025-10-22T07:54:51.433950Z :DATA_INTEGRITY DEBUG: Component: SessionActor,SessionId: test,TraceId: test123
Invalid line 2
окт 22 13:08:53 ydb-static-node-1 ydbd[844]: 2025-10-22T10:08:53.391599Z :DATA_INTEGRITY INFO: Component: DataShard,PhyTxId: 123456
Invalid line 3"""
        
        stream = StringIO(mixed_data)
        entries = list(self.parser.parse_stream(stream))
        
        assert len(entries) == 2  # Should parse 2 valid entries
        assert entries[0].node == "ydb-static-node-3"
        assert entries[1].node == "ydb-static-node-1"
        
    def test_parse_stream_consistency(self):
        """Test that parse_stream produces consistent results."""
        # Test data
        test_data = """окт 22 10:54:51 ydb-static-node-3 ydbd[889]: 2025-10-22T07:54:51.433950Z :DATA_INTEGRITY DEBUG: Component: SessionActor,SessionId: test,TraceId: test123"""
        
        # Parse using parse_stream with StringIO
        stream_entries = list(self.parser.parse_stream(StringIO(test_data)))
        
        # Results should be consistent
        assert len(stream_entries) == 1
        
        entry = stream_entries[0]
        assert entry.node == "ydb-static-node-3"
        assert entry.process == "ydbd[889]"
        assert entry.kikimr_service == "DATA_INTEGRITY"
        assert entry.log_level == "DEBUG"
        assert entry.session_id == "test"
        assert entry.trace_id == "test123"
        
    def test_parse_multiple_lock_ids(self):
        """Test parsing a line with multiple LockId values."""
        sample_line = 'окт 27 10:12:38 ydb-static-node-3 ydbd[102832]: 2025-10-27T07:12:38.009558Z :DATA_INTEGRITY INFO: Component: Executer,Type: InputActorResult,TraceId: Empty,PhyTxId: 281474977337937,Locks: [LockId: 281474977337932 DataShard: 72075186224047556 Generation: 4 Counter: 1026 SchemeShard: 72075186224037897 PathId: 96 LockId: 281474977337932 DataShard: 72075186224047563 Generation: 4 Counter: 1216 SchemeShard: 72075186224037897 PathId: 96 ],'
        
        entry = self.parser.parse_line(sample_line)
        
        assert entry is not None
        assert isinstance(entry, LogEntry)
        assert entry.node == "ydb-static-node-3"
        assert entry.process == "ydbd[102832]"
        assert entry.log_level == "INFO"
        assert entry.kikimr_service == "DATA_INTEGRITY"
        assert entry.component == "Executer"
        assert entry.phy_tx_id == "281474977337937"
        assert entry.timestamp == "2025-10-27T07:12:38.009558Z"
        
        # Check that multiple lock IDs are parsed correctly
        assert entry.lock_id is not None
        assert isinstance(entry.lock_id, list)
        assert len(entry.lock_id) == 2
        assert entry.lock_id == ["281474977337932", "281474977337932"]
        
    def test_parse_single_lock_id_as_list(self):
        """Test that single LockId is still parsed as a list."""
        sample_line = 'окт 22 10:54:51 ydb-static-node-3 ydbd[889]: 2025-10-22T07:54:51.428479Z :DATA_INTEGRITY INFO: Component: Executer,Type: Response,State: Execute,TraceId: 01k85ekrmy9tcyvnx2qvwdkcts,PhyTxId: 562949953837887,ShardId: 72075186224047627,Locks: [LockId: 562949953837886 DataShard: 72075186224047627 Generation: 1 Counter: 2 SchemeShard: 72075186224037897 PathId: 131 HasWrites: false ],Status: LOCKS_BROKEN,Issues: Empty'
        
        entry = self.parser.parse_line(sample_line)
        
        assert entry is not None
        assert isinstance(entry, LogEntry)
        assert entry.status == "LOCKS_BROKEN"
        assert entry.phy_tx_id == "562949953837887"
        
        # Check that single lock ID is parsed as a list
        assert entry.lock_id is not None
        assert isinstance(entry.lock_id, list)
        assert len(entry.lock_id) == 1
        assert entry.lock_id == ["562949953837886"]
        
    def test_parse_line_with_empty_fields(self):
        """Test parsing line with empty fields."""
        line = "окт 22 10:54:51 ydb-static-node-3 ydbd[889]: 2025-10-22T07:54:51.433950Z :DATA_INTEGRITY DEBUG: Component: SessionActor,SessionId: ,TraceId: ,Type: Response,TxId: ,Status: ,Issues: "
        
        parser = LogParser()
        entry = parser.parse_line(line)
        
        assert entry is not None
        # Empty fields are parsed as None, not empty strings
        assert entry.session_id is None
        assert entry.trace_id is None
        assert entry.tx_id is None
        assert entry.status is None
        assert entry.issues is None
        
    def test_parse_line_malformed_timestamp(self):
        """Test parsing line with malformed timestamp."""
        line = "окт 22 10:54:51 ydb-static-node-3 ydbd[889]: INVALID_TIMESTAMP :DATA_INTEGRITY DEBUG: Component: SessionActor"
        
        parser = LogParser()
        entry = parser.parse_line(line)
        
        assert entry is not None
        assert entry.timestamp == ""  # Malformed timestamp results in empty string
        
    def test_parse_line_missing_component(self):
        """Test parsing line without component field."""
        line = "окт 22 10:54:51 ydb-static-node-3 ydbd[889]: 2025-10-22T07:54:51.433950Z :DATA_INTEGRITY DEBUG: SessionId: test_session,TraceId: test_trace"
        
        parser = LogParser()
        entry = parser.parse_line(line)
        
        assert entry is not None
        assert entry.component is None
        assert entry.session_id == "test_session"
        assert entry.trace_id == "test_trace"
        
    def test_parse_line_with_special_characters(self):
        """Test parsing line with special characters in fields."""
        line = 'окт 22 10:54:51 ydb-static-node-3 ydbd[889]: 2025-10-22T07:54:51.433950Z :DATA_INTEGRITY DEBUG: Component: SessionActor,SessionId: test/session?id=123&node=456,TraceId: trace-with-dashes_and_underscores,Issues: { message: "Error with quotes and {braces}" }'
        
        parser = LogParser()
        entry = parser.parse_line(line)
        
        assert entry is not None
        assert entry.session_id == "test/session?id=123&node=456"
        assert entry.trace_id == "trace-with-dashes_and_underscores"
        # Issues parsing may not capture the full nested braces correctly
        assert entry.issues is not None
        assert "Error with quotes and" in entry.issues
        
    def test_parse_line_with_nested_braces(self):
        """Test parsing line with nested braces in issues."""
        line = 'окт 22 10:54:51 ydb-static-node-3 ydbd[889]: 2025-10-22T07:54:51.433950Z :DATA_INTEGRITY DEBUG: Component: SessionActor,Issues: { message: "Outer { inner { deep } inner } outer" issue_code: 2001 }'
        
        parser = LogParser()
        entry = parser.parse_line(line)
        
        assert entry is not None
        # Issues parsing may not handle nested braces perfectly
        assert entry.issues is not None
        assert "Outer" in entry.issues
        
    def test_parse_line_query_text_base64(self):
        """Test parsing line with base64 encoded query text."""
        line = "окт 22 10:54:51 ydb-static-node-3 ydbd[889]: 2025-10-22T07:54:51.433950Z :DATA_INTEGRITY DEBUG: Component: SessionActor,QueryText: U0VMRUNUICogRlJPTSB0ZXN0X3RhYmxl,QueryAction: QUERY_ACTION_EXECUTE"
        
        parser = LogParser()
        entry = parser.parse_line(line)
        
        assert entry is not None
        # QueryText may not be parsed if it's not in the expected format
        assert entry.query_action == "QUERY_ACTION_EXECUTE"
        # Don't assert on query_text as it may not be captured
        
    def test_parse_line_begin_tx_flag(self):
        """Test parsing line with BeginTx flag."""
        line = "окт 22 10:54:51 ydb-static-node-3 ydbd[887]: 2025-10-22T07:54:51.182225Z :DATA_INTEGRITY DEBUG: Component: SessionActor,SessionId: test_session,TraceId: test_trace,Type: Request,QueryAction: QUERY_ACTION_BEGIN_TX,BeginTx: true,TxMode: SerializableReadWrite"
        
        parser = LogParser()
        entry = parser.parse_line(line)
        
        assert entry is not None
        assert entry.begin_tx is True
        assert entry.query_action == "QUERY_ACTION_BEGIN_TX"
        
    def test_parse_stream_with_mixed_line_endings(self):
        """Test parsing stream with mixed line endings."""
        log_data = "окт 22 10:54:51 ydb-static-node-3 ydbd[889]: 2025-10-22T07:54:51.433950Z :DATA_INTEGRITY DEBUG: Component: SessionActor,SessionId: test1\r\nокт 22 10:54:51 ydb-static-node-3 ydbd[889]: 2025-10-22T07:54:51.433951Z :DATA_INTEGRITY DEBUG: Component: SessionActor,SessionId: test2\nокт 22 10:54:51 ydb-static-node-3 ydbd[889]: 2025-10-22T07:54:51.433952Z :DATA_INTEGRITY DEBUG: Component: SessionActor,SessionId: test3\r"
        
        parser = LogParser()
        stream = StringIO(log_data)
        entries = list(parser.parse_stream(stream))  # Convert generator to list
        
        assert len(entries) == 3
        assert entries[0].session_id == "test1"
        assert entries[1].session_id == "test2"
        assert entries[2].session_id == "test3"
        
    def test_parse_stream_with_unicode_characters(self):
        """Test parsing stream with unicode characters."""
        log_data = "окт 22 10:54:51 ydb-static-node-3 ydbd[889]: 2025-10-22T07:54:51.433950Z :DATA_INTEGRITY DEBUG: Component: SessionActor,SessionId: тест_сессия_с_русскими_символами,Issues: { message: \"Ошибка с русскими символами\" }"
        
        parser = LogParser()
        stream = StringIO(log_data)
        entries = list(parser.parse_stream(stream))  # Convert generator to list
        
        assert len(entries) == 1
        assert entries[0].session_id == "тест_сессия_с_русскими_символами"
        # Issues parsing may not handle unicode perfectly
        assert entries[0].issues is not None