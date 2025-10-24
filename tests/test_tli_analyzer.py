#!/usr/bin/env python3
"""
Unit tests for tli_analyzer module.
"""

import pytest
import sys
import os
import tempfile
from io import StringIO
from unittest.mock import patch, MagicMock

# Add parent directory to path to import tli_analyzer
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tli_analyzer import analyze_logs, main
from log_parser import LogEntry


class TestTLIAnalyzer:
    """Test cases for TLI Analyzer main functionality."""
    
    @patch('yaml_reporter.YAMLReporter.write_yaml_report')
    def test_analyze_logs_with_file_input(self, mock_write_yaml):
        """Test analyze_logs function with file input."""
        # Create a temporary test file with sample log data
        test_data = """окт 22 10:54:51 ydb-static-node-3 ydbd[889]: 2025-10-22T07:54:51.433950Z :DATA_INTEGRITY DEBUG: Component: SessionActor,SessionId: ydb://session/3?node_id=50005&id=test,TraceId: 01k85ekrmy9tcyvnx2qvwdkcts,Type: Response,TxId: 01k85ekret8vjt70bq09a16h6q,Status: ABORTED,Issues: { message: "Transaction locks invalidated. Table: `/Root/database/test_schema_ca7eb8ed/tt1`" issue_code: 2001 severity: 1 }"""
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.log') as temp_file:
            temp_file.write(test_data)
            temp_file_path = temp_file.name
            
        try:
            # Test file input
            analyze_logs(temp_file_path)
            
            # Check that write_yaml_report was called
            mock_write_yaml.assert_called_once()
            
            # Check that chains were passed to the reporter
            args, kwargs = mock_write_yaml.call_args
            chains = args[0]
            assert len(chains) == 1
            assert chains[0].victim_session_id == "ydb://session/3?node_id=50005&id=test"
                
        finally:
            # Clean up temporary files
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
                
    @patch('yaml_reporter.YAMLReporter.write_yaml_report')
    def test_analyze_logs_with_stdin_input(self, mock_write_yaml):
        """Test analyze_logs function with stdin input."""
        test_data = """окт 22 10:54:51 ydb-static-node-3 ydbd[889]: 2025-10-22T07:54:51.433950Z :DATA_INTEGRITY DEBUG: Component: SessionActor,SessionId: ydb://session/3?node_id=50005&id=test,TraceId: 01k85ekrmy9tcyvnx2qvwdkcts,Type: Response,TxId: 01k85ekret8vjt70bq09a16h6q,Status: ABORTED,Issues: { message: "Transaction locks invalidated. Table: `/Root/database/test_schema_ca7eb8ed/tt1`" issue_code: 2001 severity: 1 }"""
        
        # Mock stdin with test data
        with patch('sys.stdin', StringIO(test_data)):
            analyze_logs(None)
            
        # Check that write_yaml_report was called
        mock_write_yaml.assert_called_once()
        
        # Check that chains were passed to the reporter
        args, kwargs = mock_write_yaml.call_args
        chains = args[0]
        assert len(chains) == 1
        assert chains[0].victim_session_id == "ydb://session/3?node_id=50005&id=test"
                
    def test_analyze_logs_empty_input(self, capsys):
        """Test analyze_logs function with empty input."""
        # Mock stdin with empty data
        with patch('sys.stdin', StringIO("")):
            analyze_logs(None)
            
        # Should produce no output for empty input
        captured = capsys.readouterr()
        assert captured.out == ""
                
    @patch('sys.argv', ['tli_analyzer.py', '--log-file', 'test.log'])
    @patch('os.path.exists')
    @patch('tli_analyzer.analyze_logs')
    def test_main_with_file_argument(self, mock_analyze, mock_exists):
        """Test main function with file argument."""
        mock_exists.return_value = True
        
        main()
        
        # Check that analyze_logs was called with correct arguments
        mock_analyze.assert_called_once()
        args, kwargs = mock_analyze.call_args
        assert args[0] == 'test.log'  # input_source
        assert args[1] == True  # sort_logs (default is now True)
        
    @patch('sys.argv', ['tli_analyzer.py'])
    @patch('sys.stdin.isatty')
    @patch('tli_analyzer.analyze_logs')
    def test_main_with_stdin_input(self, mock_analyze, mock_isatty):
        """Test main function with stdin input."""
        mock_isatty.return_value = False  # Simulate piped input
        
        main()
        
        # Check that analyze_logs was called with stdin
        mock_analyze.assert_called_once()
        args, kwargs = mock_analyze.call_args
        assert args[0] is None  # input_source (stdin)
        assert args[1] == True  # sort_logs (default is now True)
        
    @patch('sys.argv', ['tli_analyzer.py'])
    @patch('sys.stdin.isatty')
    def test_main_no_input_error(self, mock_isatty):
        """Test main function with no input raises error."""
        mock_isatty.return_value = True  # Simulate terminal input (no pipe)
        
        with pytest.raises(SystemExit) as exc_info:
            main()
            
        assert exc_info.value.code == 1
        
    @patch('sys.argv', ['tli_analyzer.py', '--log-file', 'nonexistent.log'])
    @patch('os.path.exists')
    def test_main_file_not_found_error(self, mock_exists):
        """Test main function with non-existent file raises error."""
        mock_exists.return_value = False
        
        with pytest.raises(SystemExit) as exc_info:
            main()
            
        assert exc_info.value.code == 1
        
    @patch('sys.argv', ['tli_analyzer.py', '--no-sort'])
    @patch('sys.stdin.isatty')
    @patch('tli_analyzer.analyze_logs')
    def test_main_with_no_sort_option(self, mock_analyze, mock_isatty):
        """Test main function with no-sort option."""
        mock_isatty.return_value = False  # Simulate piped input
        
        main()
        
        # Check that analyze_logs was called with correct arguments
        mock_analyze.assert_called_once()
        args, kwargs = mock_analyze.call_args
        assert args[0] is None  # input_source (stdin)
        assert args[1] == False  # sort_logs (disabled with --no-sort)
        
    def test_analyze_logs_no_verbose_output(self, capsys):
        """Test analyze_logs function produces no verbose output."""
        test_data = """окт 22 10:54:51 ydb-static-node-3 ydbd[889]: 2025-10-22T07:54:51.433950Z :DATA_INTEGRITY DEBUG: Component: SessionActor,SessionId: test,TraceId: test123"""
        
        # Mock stdin with test data
        with patch('sys.stdin', StringIO(test_data)):
            analyze_logs(None)
            
        # Check that no verbose output is produced
        captured = capsys.readouterr()
        assert "Step 1:" not in captured.out
        assert "Step 2:" not in captured.out
        assert "Analyzing YDB" not in captured.out


    def test_analyze_logs_with_sort_flag(self, capsys):
        """Test analyze_logs function with sort flag enabled."""
        # Create unsorted test data (older timestamp first)
        test_data = """окт 22 10:54:51 ydb-static-node-1 ydbd[846]: 2025-10-22T07:54:51.100000Z :DATA_INTEGRITY INFO: older entry
окт 22 10:54:51 ydb-static-node-2 ydbd[847]: 2025-10-22T07:54:51.300000Z :DATA_INTEGRITY DEBUG: newer entry"""
        
        # Mock stdin with unsorted test data
        with patch('sys.stdin', StringIO(test_data)):
            analyze_logs(None, sort_logs=True)
            
        # Should not produce verbose output even with sorting
        captured = capsys.readouterr()
        assert "Step 2:" not in captured.out
        assert "Analyzing YDB" not in captured.out
    
    @patch('sys.argv', ['tli_analyzer.py', '--no-sort'])
    @patch('sys.stdin.isatty')
    @patch('tli_analyzer.analyze_logs')
    def test_main_with_no_sort_flag(self, mock_analyze, mock_isatty):
        """Test main function with no-sort flag."""
        mock_isatty.return_value = False  # Simulate piped input
        
        main()
        
        # Check that analyze_logs was called with sort=False
        mock_analyze.assert_called_once()
        args, kwargs = mock_analyze.call_args
        assert args[0] is None  # input_source (stdin)
        assert args[1] == False  # sort_logs (disabled with --no-sort)


if __name__ == '__main__':
    pytest.main([__file__])