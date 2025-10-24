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
    
    def test_analyze_logs_with_file_input(self):
        """Test analyze_logs function with file input."""
        # Create a temporary test file with sample log data
        test_data = """окт 22 10:54:51 ydb-static-node-3 ydbd[889]: 2025-10-22T07:54:51.433950Z :DATA_INTEGRITY DEBUG: Component: SessionActor,SessionId: ydb://session/3?node_id=50005&id=test,TraceId: 01k85ekrmy9tcyvnx2qvwdkcts,Type: Response,TxId: 01k85ekret8vjt70bq09a16h6q,Status: ABORTED,Issues: { message: "Transaction locks invalidated. Table: `/Root/database/test_schema_ca7eb8ed/tt1`" issue_code: 2001 severity: 1 }"""
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.log') as temp_file:
            temp_file.write(test_data)
            temp_file_path = temp_file.name
            
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.yaml') as output_file:
            output_file_path = output_file.name
            
        try:
            # Test file input
            analyze_logs(temp_file_path, output_file_path, verbose=False)
            
            # Check that output file was created
            assert os.path.exists(output_file_path)
            
            # Check that output file has content
            with open(output_file_path, 'r') as f:
                content = f.read()
                assert len(content) > 0
                assert 'analysis_metadata' in content
                
        finally:
            # Clean up temporary files
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
            if os.path.exists(output_file_path):
                os.unlink(output_file_path)
                
    def test_analyze_logs_with_stdin_input(self):
        """Test analyze_logs function with stdin input."""
        test_data = """окт 22 10:54:51 ydb-static-node-3 ydbd[889]: 2025-10-22T07:54:51.433950Z :DATA_INTEGRITY DEBUG: Component: SessionActor,SessionId: ydb://session/3?node_id=50005&id=test,TraceId: 01k85ekrmy9tcyvnx2qvwdkcts,Type: Response,TxId: 01k85ekret8vjt70bq09a16h6q,Status: ABORTED,Issues: { message: "Transaction locks invalidated. Table: `/Root/database/test_schema_ca7eb8ed/tt1`" issue_code: 2001 severity: 1 }"""
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.yaml') as output_file:
            output_file_path = output_file.name
            
        try:
            # Mock stdin with test data
            with patch('sys.stdin', StringIO(test_data)):
                analyze_logs(None, output_file_path, verbose=False)
                
            # Check that output file was created
            assert os.path.exists(output_file_path)
            
            # Check that output file has content
            with open(output_file_path, 'r') as f:
                content = f.read()
                assert len(content) > 0
                assert 'analysis_metadata' in content
                
        finally:
            # Clean up temporary file
            if os.path.exists(output_file_path):
                os.unlink(output_file_path)
                
    def test_analyze_logs_empty_input(self):
        """Test analyze_logs function with empty input."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.yaml') as output_file:
            output_file_path = output_file.name
            
        try:
            # Mock stdin with empty data
            with patch('sys.stdin', StringIO("")):
                analyze_logs(None, output_file_path, verbose=False)
                
            # Should still create output file even with no entries
            assert os.path.exists(output_file_path)
            
        finally:
            # Clean up temporary file
            if os.path.exists(output_file_path):
                os.unlink(output_file_path)
                
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
        assert args[1] == 'tli_analysis_report.yaml'  # output_file
        assert args[2] == False  # verbose
        
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
        assert args[1] == 'tli_analysis_report.yaml'  # output_file
        assert args[2] == False  # verbose
        
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
        
    @patch('sys.argv', ['tli_analyzer.py', '--verbose', '--output', 'custom.yaml'])
    @patch('sys.stdin.isatty')
    @patch('tli_analyzer.analyze_logs')
    def test_main_with_verbose_and_custom_output(self, mock_analyze, mock_isatty):
        """Test main function with verbose and custom output options."""
        mock_isatty.return_value = False  # Simulate piped input
        
        main()
        
        # Check that analyze_logs was called with correct arguments
        mock_analyze.assert_called_once()
        args, kwargs = mock_analyze.call_args
        assert args[0] is None  # input_source (stdin)
        assert args[1] == 'custom.yaml'  # output_file
        assert args[2] == True  # verbose
        
    def test_analyze_logs_verbose_output(self, capsys):
        """Test analyze_logs function with verbose output."""
        test_data = """окт 22 10:54:51 ydb-static-node-3 ydbd[889]: 2025-10-22T07:54:51.433950Z :DATA_INTEGRITY DEBUG: Component: SessionActor,SessionId: test,TraceId: test123"""
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.yaml') as output_file:
            output_file_path = output_file.name
            
        try:
            # Mock stdin with test data
            with patch('sys.stdin', StringIO(test_data)):
                analyze_logs(None, output_file_path, verbose=True)
                
            # Check verbose output
            captured = capsys.readouterr()
            assert "Step 1: Setting up log parser..." in captured.out
            assert "Step 2: Streaming log analysis and tracing chains..." in captured.out
            assert "Analyzing YDB log data from stdin..." in captured.out
            
        finally:
            # Clean up temporary file
            if os.path.exists(output_file_path):
                os.unlink(output_file_path)


    def test_analyze_logs_with_sort_flag(self, capsys):
        """Test analyze_logs function with sort flag enabled."""
        # Create unsorted test data (older timestamp first)
        test_data = """окт 22 10:54:51 ydb-static-node-1 ydbd[846]: 2025-10-22T07:54:51.100000Z :DATA_INTEGRITY INFO: older entry
окт 22 10:54:51 ydb-static-node-2 ydbd[847]: 2025-10-22T07:54:51.300000Z :DATA_INTEGRITY DEBUG: newer entry"""
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.yaml') as output_file:
            output_file_path = output_file.name
            
        try:
            # Mock stdin with unsorted test data
            with patch('sys.stdin', StringIO(test_data)):
                analyze_logs(None, output_file_path, verbose=True, sort_logs=True)
                
            # Check verbose output mentions sorting
            captured = capsys.readouterr()
            assert "Step 2: Sorting and streaming log analysis..." in captured.out
            assert "Analyzing YDB log data from stdin..." in captured.out
            
        finally:
            # Clean up temporary file
            if os.path.exists(output_file_path):
                os.unlink(output_file_path)
    
    @patch('sys.argv', ['tli_analyzer.py', '--sort', '--verbose'])
    @patch('sys.stdin.isatty')
    @patch('tli_analyzer.analyze_logs')
    def test_main_with_sort_flag(self, mock_analyze, mock_isatty):
        """Test main function with sort flag."""
        mock_isatty.return_value = False  # Simulate piped input
        
        main()
        
        # Check that analyze_logs was called with sort=True
        mock_analyze.assert_called_once()
        args, kwargs = mock_analyze.call_args
        assert args[0] is None  # input_source (stdin)
        assert args[1] == 'tli_analysis_report.yaml'  # output_file
        assert args[2] == True  # verbose
        assert args[3] == True  # sort


if __name__ == '__main__':
    pytest.main([__file__])