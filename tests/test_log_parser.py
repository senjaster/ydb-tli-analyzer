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
        assert entry.message_type == "DATA_INTEGRITY"
        assert entry.component == "SessionActor"
        assert entry.session_id == "ydb://session/3?node_id=50005&id=MzFkMjA5YjktNWJkYzI3YzgtYjEwOGJhNDAtOWFlMmZhYzI="
        assert entry.trace_id == "01k85ekrmy9tcyvnx2qvwdkcts"
        assert entry.tx_id == "01k85ekret8vjt70bq09a16h6q"
        assert entry.status == "ABORTED"
        assert "Transaction locks invalidated" in entry.issues
        assert entry.timestamp == "2025-10-22T07:54:51.433950Z"
        assert entry.raw_line == sample_line
        
    def test_parse_break_locks_line(self):
        """Test parsing a line with BreakLocks information."""
        sample_line = 'окт 22 13:08:53 ydb-static-node-1 ydbd[844]: 2025-10-22T10:08:53.391599Z :DATA_INTEGRITY INFO: Component: DataShard,Type: Locks,TabletId: 72075186224047631,PhyTxId: 562949953837901,BreakLocks: [562949953837900 844424930570469 ]'
        
        entry = self.parser.parse_line(sample_line)
        
        assert entry is not None
        assert isinstance(entry, LogEntry)
        assert entry.node == "ydb-static-node-1"
        assert entry.process == "ydbd[844]"
        assert entry.log_level == "INFO"
        assert entry.message_type == "DATA_INTEGRITY"
        assert entry.component == "DataShard"
        assert entry.phy_tx_id == "562949953837901"
        assert entry.break_locks == ["562949953837900", "844424930570469"]
        assert entry.timestamp == "2025-10-22T10:08:53.391599Z"
        
    def test_parse_locks_broken_status_line(self):
        """Test parsing a line with LOCKS_BROKEN status."""
        sample_line = 'окт 22 10:54:51 ydb-static-node-1 ydbd[846]: 2025-10-22T07:54:51.425247Z :DATA_INTEGRITY INFO: Component: DataShard,Type: Finished,TabletId: 72075186224047627,PhyTxId: 562949953837887,Status: LOCKS_BROKEN,'
        
        entry = self.parser.parse_line(sample_line)
        
        assert entry is not None
        assert entry.status == "LOCKS_BROKEN"
        assert entry.phy_tx_id == "562949953837887"
        
    def test_parse_file_with_fixture(self):
        """Test parsing the test fixture file."""
        fixture_path = os.path.join(os.path.dirname(__file__), 'fixtures', 'test_log.log')
        
        if not os.path.exists(fixture_path):
            pytest.skip(f"Test fixture not found: {fixture_path}")
            
        entries = self.parser.parse_file(fixture_path)
        
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
            if entry.break_locks and len(entry.break_locks) > 0
        ]
        assert len(break_lock_entries) > 0
        
    def test_parse_file_not_found(self):
        """Test parsing non-existent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            self.parser.parse_file("non_existent_file.log")
            
    def test_log_entry_dataclass(self):
        """Test LogEntry dataclass properties."""
        entry = LogEntry(
            timestamp="2025-10-22T07:54:51.433950Z",
            node="test-node",
            process="test[123]",
            log_level="DEBUG",
            message_type="TEST",
            session_id="test-session",
            trace_id="test-trace",
            component="TestComponent",
            raw_line="test line"
        )
        
        assert entry.timestamp == "2025-10-22T07:54:51.433950Z"
        assert entry.node == "test-node"
        assert entry.process == "test[123]"
        assert entry.log_level == "DEBUG"
        assert entry.message_type == "TEST"
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
        assert entry.break_locks is None
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
        entries = self.parser.parse_stream(stream)
        
        assert len(entries) == 3  # Should parse 3 valid entries, skip 1 invalid
        assert all(isinstance(entry, LogEntry) for entry in entries)
        
        # Check first entry (transaction locks invalidated)
        first_entry = entries[0]
        assert first_entry.node == "ydb-static-node-3"
        assert first_entry.message_type == "DATA_INTEGRITY"
        assert first_entry.log_level == "DEBUG"
        assert first_entry.status == "ABORTED"
        assert "Transaction locks invalidated" in first_entry.issues
        
        # Check second entry (break locks)
        second_entry = entries[1]
        assert second_entry.node == "ydb-static-node-1"
        assert second_entry.break_locks == ["562949953837900", "844424930570469"]
        
        # Check third entry (locks broken status)
        third_entry = entries[2]
        assert third_entry.status == "LOCKS_BROKEN"
        assert third_entry.phy_tx_id == "562949953837887"
        
    def test_parse_stream_empty(self):
        """Test parsing empty stream."""
        empty_stream = StringIO("")
        entries = self.parser.parse_stream(empty_stream)
        assert len(entries) == 0
        
    def test_parse_stream_only_invalid_lines(self):
        """Test parsing stream with only invalid lines."""
        invalid_data = """This is not a valid log line
Another invalid line
Yet another invalid line"""
        
        stream = StringIO(invalid_data)
        entries = self.parser.parse_stream(stream)
        assert len(entries) == 0
        
    def test_parse_stream_mixed_valid_invalid(self):
        """Test parsing stream with mix of valid and invalid lines."""
        mixed_data = """Invalid line 1
окт 22 10:54:51 ydb-static-node-3 ydbd[889]: 2025-10-22T07:54:51.433950Z :DATA_INTEGRITY DEBUG: Component: SessionActor,SessionId: test,TraceId: test123
Invalid line 2
окт 22 13:08:53 ydb-static-node-1 ydbd[844]: 2025-10-22T10:08:53.391599Z :DATA_INTEGRITY INFO: Component: DataShard,PhyTxId: 123456
Invalid line 3"""
        
        stream = StringIO(mixed_data)
        entries = self.parser.parse_stream(stream)
        
        assert len(entries) == 2  # Should parse 2 valid entries
        assert entries[0].node == "ydb-static-node-3"
        assert entries[1].node == "ydb-static-node-1"
        
    def test_parse_file_uses_parse_stream(self):
        """Test that parse_file method uses parse_stream internally."""
        # Create a temporary test file
        test_data = """окт 22 10:54:51 ydb-static-node-3 ydbd[889]: 2025-10-22T07:54:51.433950Z :DATA_INTEGRITY DEBUG: Component: SessionActor,SessionId: test,TraceId: test123"""
        
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.log') as temp_file:
            temp_file.write(test_data)
            temp_file_path = temp_file.name
            
        try:
            # Parse using parse_file
            file_entries = self.parser.parse_file(temp_file_path)
            
            # Parse using parse_stream with the same data
            stream_entries = self.parser.parse_stream(StringIO(test_data))
            
            # Results should be identical
            assert len(file_entries) == len(stream_entries)
            assert len(file_entries) == 1
            
            # Compare the parsed entries
            file_entry = file_entries[0]
            stream_entry = stream_entries[0]
            
            assert file_entry.node == stream_entry.node
            assert file_entry.process == stream_entry.process
            assert file_entry.message_type == stream_entry.message_type
            assert file_entry.log_level == stream_entry.log_level
            assert file_entry.session_id == stream_entry.session_id
            assert file_entry.trace_id == stream_entry.trace_id
            
        finally:
            # Clean up temporary file
            os.unlink(temp_file_path)