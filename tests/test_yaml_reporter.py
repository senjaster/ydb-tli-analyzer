#!/usr/bin/env python3
"""
Unit tests for YAMLReporter class.
"""

import pytest
import os
import tempfile
import yaml
from log_parser import LogEntry
from chain_tracer import LockInvalidationChain
from yaml_reporter import YAMLReporter


class TestYAMLReporter:
    """Test cases for YAMLReporter class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.reporter = YAMLReporter()
        
        # Create sample chain for testing
        self.victim_entry = LogEntry(
            timestamp="2025-10-22T07:54:51.433950Z",
            node="ydb-static-node-3",
            process="ydbd[889]",
            log_level="DEBUG",
            message_type="DATA_INTEGRITY",
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
            message_type="DATA_INTEGRITY",
            session_id="ydb://session/3?node_id=50003&id=culprit_session",
            trace_id="culprit_trace_id",
            tx_id="culprit_tx_id",
            raw_line="culprit log line"
        )
        
        self.sample_chain = LockInvalidationChain(
            victim_session_id="ydb://session/3?node_id=50005&id=victim_session",
            victim_trace_id="victim_trace_id",
            lock_id="test_lock_id",
            culprit_phy_tx_id="culprit_phy_tx_id",
            culprit_trace_id="culprit_trace_id",
            culprit_session_id="ydb://session/3?node_id=50003&id=culprit_session",
            victim_entry=self.victim_entry,
            culprit_entry=self.culprit_entry,
            victim_query_text="SELECT * FROM tt1",
            culprit_query_text="UPDATE tt1 SET col=1",
            table_name="/Root/database/test_schema/tt1",
            victim_tx_id="victim_tx_id",
            culprit_tx_id="culprit_tx_id",
            victim_queries=[
                LogEntry(
                    timestamp='2025-10-22T07:54:51.433950Z',
                    node='test-node',
                    process='test[123]',
                    log_level='DEBUG',
                    message_type='TEST',
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
                    message_type='TEST',
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
        
    def test_write_yaml_report(self):
        """Test writing YAML report to file."""
        chains = [self.sample_chain]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp_file:
            tmp_path = tmp_file.name
            
        try:
            # Write to file by opening it and passing as file parameter
            with open(tmp_path, 'w', encoding='utf-8') as f:
                self.reporter.write_yaml_report(chains, f)
            
            # Verify file was created
            assert os.path.exists(tmp_path)
            
            # Verify file content
            with open(tmp_path, 'r', encoding='utf-8') as f:
                content = yaml.safe_load(f)
                
            assert 'analysis_metadata' in content
            assert 'lock_invalidation_events' in content
            
            # Check metadata
            metadata = content['analysis_metadata']
            assert metadata['total_invalidation_events'] == 1
            assert 'generated_at' in metadata
            
            # Check events
            events_data = content['lock_invalidation_events']
            assert len(events_data) == 1
            
            event_data = events_data[0]
            assert event_data['victim']['session_id'] == "ydb://session/3?node_id=50005&id=victim_session"
            assert event_data['victim']['trace_id'] == "victim_trace_id"
            assert event_data['culprit']['session_id'] == "ydb://session/3?node_id=50003&id=culprit_session"
            assert event_data['culprit']['trace_id'] == "culprit_trace_id"
            assert event_data['lock_details']['lock_id'] == "test_lock_id"
            assert event_data['table'] == "/Root/database/test_schema/tt1"
            
        finally:
            # Clean up
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
                
    def test_write_yaml_report_empty_chains(self):
        """Test writing YAML report with empty chains."""
        chains = []
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp_file:
            tmp_path = tmp_file.name
            
        try:
            # Write to file by opening it and passing as file parameter
            with open(tmp_path, 'w', encoding='utf-8') as f:
                self.reporter.write_yaml_report(chains, f)
            
            # Verify file was created
            assert os.path.exists(tmp_path)
            
            # Verify file content
            with open(tmp_path, 'r', encoding='utf-8') as f:
                content = yaml.safe_load(f)
                
            assert content['analysis_metadata']['total_invalidation_events'] == 0
            assert len(content['lock_invalidation_events']) == 0
            
        finally:
            # Clean up
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
                
    def test_write_yaml_report_invalid_path(self):
        """Test writing YAML report to invalid path raises exception."""
        chains = [self.sample_chain]
        invalid_path = "/invalid/path/that/does/not/exist/report.yaml"
        
        with pytest.raises(Exception):
            with open(invalid_path, 'w') as f:
                self.reporter.write_yaml_report(chains, f)
            
    def test_write_yaml_report_to_stdout(self):
        """Test writing YAML report to stdout (default behavior)."""
        chains = [self.sample_chain]
        
        # Use StringIO to capture output instead of relying on capsys
        from io import StringIO
        output_buffer = StringIO()
        
        self.reporter.write_yaml_report(chains, output_buffer)
        
        output = output_buffer.getvalue()
        
        # Verify that YAML output was generated
        assert output.strip() != ""
        assert 'analysis_metadata:' in output
        assert 'lock_invalidation_events:' in output
        assert 'total_invalidation_events: 1' in output
        
        # Parse the YAML output
        content = yaml.safe_load(output)
        
        # Ensure content is not None
        assert content is not None
        assert 'analysis_metadata' in content
        assert 'lock_invalidation_events' in content
        assert content['analysis_metadata']['total_invalidation_events'] == 1
        assert len(content['lock_invalidation_events']) == 1
        
    def test_chain_serialization_with_queries(self):
        """Test that chains with queries are properly serialized."""
        chains = [self.sample_chain]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp_file:
            tmp_path = tmp_file.name
            
        try:
            # Write to file by opening it and passing as file parameter
            with open(tmp_path, 'w', encoding='utf-8') as f:
                self.reporter.write_yaml_report(chains, f)
            
            with open(tmp_path, 'r', encoding='utf-8') as f:
                content = yaml.safe_load(f)
                
            event_data = content['lock_invalidation_events'][0]
            
            # Check victim queries
            assert 'all_queries' in event_data['victim']
            victim_queries = event_data['victim']['all_queries']
            assert len(victim_queries) == 1
            assert victim_queries[0]['query_text'] == 'SELECT * FROM tt1'
            assert victim_queries[0]['query_action'] == 'QUERY_ACTION_EXECUTE'
            
            # Check culprit queries
            assert 'all_queries' in event_data['culprit']
            culprit_queries = event_data['culprit']['all_queries']
            assert len(culprit_queries) == 1
            assert culprit_queries[0]['query_text'] == 'UPDATE tt1 SET col=1'
            assert culprit_queries[0]['query_action'] == 'QUERY_ACTION_EXECUTE'
            
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
                
    def test_chain_serialization_without_optional_fields(self):
        """Test serialization of chain with minimal data."""
        minimal_chain = LockInvalidationChain(
            victim_session_id="victim_session",
            victim_trace_id="victim_trace",
            lock_id="lock_id",
            culprit_phy_tx_id="culprit_phy_tx",
            culprit_trace_id="culprit_trace",
            culprit_session_id="culprit_session",
            victim_entry=self.victim_entry
        )
        
        chains = [minimal_chain]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp_file:
            tmp_path = tmp_file.name
            
        try:
            # Write to file by opening it and passing as file parameter
            with open(tmp_path, 'w', encoding='utf-8') as f:
                self.reporter.write_yaml_report(chains, f)
            
            with open(tmp_path, 'r', encoding='utf-8') as f:
                content = yaml.safe_load(f)
                
            event_data = content['lock_invalidation_events'][0]
            
            # Should have basic fields
            assert event_data['victim']['session_id'] == "victim_session"
            assert event_data['culprit']['session_id'] == "culprit_session"
            assert event_data['lock_details']['lock_id'] == "lock_id"
            
            # Optional fields should be None or not present
            assert event_data.get('table') is None
            
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)