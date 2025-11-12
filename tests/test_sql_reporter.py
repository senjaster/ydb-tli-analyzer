#!/usr/bin/env python3
"""
Unit tests for SQLReporter class.
"""

import pytest
import os
import tempfile
import sqlite3
from log_parser import LogEntry
from chain_models import LockInvalidationChain
from sql_reporter import SQLReporter


class TestSQLReporter:
    """Test cases for SQLReporter class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.reporter = SQLReporter()
        
        # Create sample chain for testing
        self.victim_entry = LogEntry(
            timestamp="2025-10-22T07:54:51.433950Z",
            node="ydb-static-node-3",
            process="ydbd[889]",
            log_level="DEBUG",
            kikimr_service="DATA_INTEGRITY",
            session_id="ydb://session/3?node_id=50005&id=victim_session",
            trace_id="victim_trace_id",
            tx_id="victim_tx_id",
            status="ABORTED",
            issues='message: "Transaction locks invalidated. Table: `/Root/database/test_schema/tt1`" issue_code: 2001 severity: 1',
            raw_line="victim log line"
        )
        
        self.culprit_entry = LogEntry(
            timestamp="2025-10-22T07:54:51.396819Z",
            node="ydb-static-node-1",
            process="ydbd[887]",
            log_level="INFO",
            kikimr_service="DATA_INTEGRITY",
            session_id="ydb://session/3?node_id=50003&id=culprit_session",
            trace_id="culprit_trace_id",
            tx_id="culprit_tx_id",
            raw_line="culprit log line"
        )
        
        self.sample_chain = LockInvalidationChain(
            victim_session_id="ydb://session/3?node_id=50005&id=victim_session",
            victim_trace_id="victim_trace_id",
            victim_tx_id="victim_tx_id",
            victim_entry=self.victim_entry,
            table_name="/Root/database/test_schema/tt1",
            lock_id="test_lock_id",
            culprit_phy_tx_id="culprit_phy_tx_id",
            culprit_trace_id="culprit_trace_id",
            culprit_session_id="ydb://session/3?node_id=50003&id=culprit_session",
            culprit_entry=self.culprit_entry,
            culprit_tx_id="culprit_tx_id",
            victim_queries=[
                LogEntry(
                    timestamp='2025-10-22T07:54:51.433950Z',
                    node='test-node',
                    process='test[123]',
                    log_level='DEBUG',
                    kikimr_service='TEST',
                    query_text='SELECT * FROM tt1',
                    query_action='QUERY_ACTION_EXECUTE',
                    trace_id='victim_trace_id',
                    query_type='QUERY_TYPE_SQL_GENERIC_QUERY',
                    raw_line='test line'
                )
            ],
            culprit_queries=[
                LogEntry(
                    timestamp='2025-10-22T07:54:51.396819Z',
                    node='test-node',
                    process='test[123]',
                    log_level='DEBUG',
                    kikimr_service='TEST',
                    query_text='UPDATE tt1 SET col=1',
                    query_action='QUERY_ACTION_EXECUTE',
                    trace_id='culprit_trace_id',
                    query_type='QUERY_TYPE_SQL_GENERIC_QUERY',
                    raw_line='test line'
                )
            ]
        )
        
    def test_reporter_initialization(self):
        """Test that reporter initializes correctly."""
        assert self.reporter is not None
        
    def test_write_sql_report_single_chain(self):
        """Test writing SQL report with single chain."""
        chains = [self.sample_chain]
        
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.sql', delete=False) as tmp_file:
            tmp_path = tmp_file.name
            
        try:
            with open(tmp_path, 'w', encoding='utf-8') as f:
                self.reporter.write_sql_report(chains, f)
            
            # Verify file was created
            assert os.path.exists(tmp_path)
            
            # Verify content
            with open(tmp_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            assert "YDB Transaction Lock Invalidation (TLI) Analysis Report" in content
            assert "TLI EVENT #1" in content
            assert "VICTIM TRANSACTION" in content
            assert "CULPRIT TRANSACTION" in content
            assert "victim_trace_id" in content
            assert "culprit_trace_id" in content
            assert "/Root/database/test_schema/tt1" in content
            
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
                
    def test_write_sql_report_multiple_chains(self):
        """Test writing SQL report with multiple chains."""
        # Create second chain
        second_chain = LockInvalidationChain(
            victim_session_id="victim_session_2",
            victim_trace_id="victim_trace_2",
            victim_tx_id="victim_tx_2",
            victim_entry=self.victim_entry,
            table_name="test_table_2",
            lock_id="lock_id_2"
        )
        
        chains = [self.sample_chain, second_chain]
        
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.sql', delete=False) as tmp_file:
            tmp_path = tmp_file.name
            
        try:
            with open(tmp_path, 'w', encoding='utf-8') as f:
                self.reporter.write_sql_report(chains, f)
            
            # Verify content
            with open(tmp_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            assert "Total invalidation events: 2" in content
            assert "TLI EVENT #1" in content
            assert "TLI EVENT #2" in content
            
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
                
    def test_write_sql_report_empty_chains(self):
        """Test writing SQL report with empty chains."""
        chains = []
        
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.sql', delete=False) as tmp_file:
            tmp_path = tmp_file.name
            
        try:
            with open(tmp_path, 'w', encoding='utf-8') as f:
                self.reporter.write_sql_report(chains, f)
            
            # Verify content
            with open(tmp_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            assert "No transaction lock invalidation events found" in content
            
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
                
    def test_write_sql_report_chain_without_queries(self):
        """Test writing chain without queries."""
        chain_no_queries = LockInvalidationChain(
            victim_session_id="victim_session",
            victim_trace_id="victim_trace",
            victim_tx_id="victim_tx",
            victim_entry=self.victim_entry,
            table_name="test_table",
            lock_id="lock_id"
        )
        
        chains = [chain_no_queries]
        
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.sql', delete=False) as tmp_file:
            tmp_path = tmp_file.name
            
        try:
            with open(tmp_path, 'w', encoding='utf-8') as f:
                self.reporter.write_sql_report(chains, f)
            
            # Verify content
            with open(tmp_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            assert "TLI EVENT #1" in content
            assert "VICTIM TRANSACTION" in content
            assert "CULPRIT TRANSACTION" in content
            
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
                
    def test_write_sql_report_invalid_path(self):
        """Test writing SQL report to invalid path raises exception."""
        chains = [self.sample_chain]
        invalid_path = "/invalid/path/that/does/not/exist/report.sql"
        
        with pytest.raises(Exception):
            with open(invalid_path, 'w') as f:
                self.reporter.write_sql_report(chains, f)
            
    def test_chain_serialization_with_optional_fields(self):
        """Test serialization of chain with all optional fields."""
        chains = [self.sample_chain]
        
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.sql', delete=False) as tmp_file:
            tmp_path = tmp_file.name
            
        try:
            with open(tmp_path, 'w', encoding='utf-8') as f:
                self.reporter.write_sql_report(chains, f)
            
            # Verify content includes all fields
            with open(tmp_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            assert "victim_trace_id" in content
            assert "culprit_trace_id" in content
            assert "/Root/database/test_schema/tt1" in content
            assert "SELECT * FROM tt1" in content
            assert "UPDATE tt1 SET col=1" in content
            
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
                
    def test_report_to_stdout(self):
        """Test writing report to stdout."""
        chains = [self.sample_chain]
        
        from io import StringIO
        output_buffer = StringIO()
        
        self.reporter.write_sql_report(chains, output_buffer)
        
        output = output_buffer.getvalue()
        
        assert "YDB Transaction Lock Invalidation (TLI) Analysis Report" in output
        assert "TLI EVENT #1" in output
        assert "VICTIM TRANSACTION" in output
        assert "CULPRIT TRANSACTION" in output