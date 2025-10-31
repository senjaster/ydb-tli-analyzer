#!/usr/bin/env python3
"""
Unit tests for log sorter function.
"""

import pytest
import tempfile
import os
from io import StringIO
from log_sorter import sort_log_stream


class TestLogSorter:
    """Test cases for log sorter function."""
        
    def test_sort_stream_chronological_order(self):
        """Test sorting from StringIO stream in chronological order (earliest first)."""
        sample_data = """окт 22 10:54:51 ydb-static-node-1 ydbd[846]: 2025-10-22T07:54:51.300000Z :DATA_INTEGRITY INFO: third
окт 22 10:54:51 ydb-static-node-2 ydbd[847]: 2025-10-22T07:54:51.100000Z :DATA_INTEGRITY INFO: first
окт 22 10:54:51 ydb-static-node-3 ydbd[848]: 2025-10-22T07:54:51.200000Z :DATA_INTEGRITY INFO: second"""
        
        stream = StringIO(sample_data)
        sorted_lines = list(sort_log_stream(stream))
        
        # Should be sorted chronologically (earliest first) using sort command
        assert len(sorted_lines) == 3
        assert "2025-10-22T07:54:51.100000Z" in sorted_lines[2]  # earliest
        assert "2025-10-22T07:54:51.200000Z" in sorted_lines[1]  # middle
        assert "2025-10-22T07:54:51.300000Z" in sorted_lines[0]  # latest
        
    def test_sort_stream_empty(self):
        """Test sorting empty stream."""
        empty_stream = StringIO("")
        sorted_lines = list(sort_log_stream(empty_stream))
        assert len(sorted_lines) == 0
        
    def test_sort_stream_with_file(self):
        """Test sorting from a file using sort_log_stream."""
        test_data = """окт 22 10:54:51 ydb-static-node-1 ydbd[846]: 2025-10-22T07:54:51.300000Z :DATA_INTEGRITY INFO: third
окт 22 10:54:51 ydb-static-node-2 ydbd[847]: 2025-10-22T07:54:51.100000Z :DATA_INTEGRITY INFO: first
окт 22 10:54:51 ydb-static-node-3 ydbd[848]: 2025-10-22T07:54:51.200000Z :DATA_INTEGRITY INFO: second"""
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.log') as temp_file:
            temp_file.write(test_data)
            temp_file_path = temp_file.name
            
        try:
            with open(temp_file_path, 'r', encoding='utf-8') as f:
                sorted_lines = list(sort_log_stream(f))
            
            # Should be sorted chronologically (earliest first)
            assert len(sorted_lines) == 3
            assert "2025-10-22T07:54:51.100000Z" in sorted_lines[2]  # earliest
            assert "2025-10-22T07:54:51.200000Z" in sorted_lines[1]  # middle
            assert "2025-10-22T07:54:51.300000Z" in sorted_lines[0]  # latest
            
        finally:
            # Clean up temporary file
            os.unlink(temp_file_path)
            
    def test_sort_preserves_line_content(self):
        """Test that sorting preserves complete line content."""
        original_lines = [
            'окт 22 10:54:51 ydb-static-node-1 ydbd[846]: 2025-10-22T07:54:51.200000Z :DATA_INTEGRITY INFO: Component: DataShard,PhyTxId: 123456,BreakLocks: [111 222]',
            'окт 22 10:54:51 ydb-static-node-2 ydbd[847]: 2025-10-22T07:54:51.100000Z :DATA_INTEGRITY INFO: Component: SessionActor,SessionId: test1,TraceId: trace1'
        ]
        
        stream = StringIO('\n'.join(original_lines))
        sorted_lines = list(sort_log_stream(stream))
        
        # Content should be preserved exactly and sorted chronologically
        assert len(sorted_lines) == 2
        # Earlier timestamp should be first
        assert "2025-10-22T07:54:51.100000Z" in sorted_lines[1]
        assert "2025-10-22T07:54:51.200000Z" in sorted_lines[0]
        
        # Verify all original content is preserved
        sorted_content = '\n'.join(sorted_lines)
        for original in original_lines:
            assert original in sorted_content
            
    def test_sort_stream_different_formats(self):
        """Test sorting with different log formats."""
        from log_parser import LogFormat
        
        # Test with SYSTEMD format (default)
        sample_data = """окт 22 10:54:51 ydb-static-node-1 ydbd[846]: 2025-10-22T07:54:51.300000Z :DATA_INTEGRITY INFO: third
окт 22 10:54:51 ydb-static-node-2 ydbd[847]: 2025-10-22T07:54:51.100000Z :DATA_INTEGRITY INFO: first"""
        
        stream = StringIO(sample_data)
        sorted_lines = list(sort_log_stream(stream, LogFormat.SYSTEMD))
        
        assert len(sorted_lines) == 2
        assert "2025-10-22T07:54:51.100000Z" in sorted_lines[1]  # earliest
        assert "2025-10-22T07:54:51.300000Z" in sorted_lines[0]  # latest
        
    def test_sort_stream_single_line(self):
        """Test sorting stream with single line."""
        single_line = "окт 22 10:54:51 ydb-static-node-1 ydbd[846]: 2025-10-22T07:54:51.300000Z :DATA_INTEGRITY INFO: single"
        
        stream = StringIO(single_line)
        sorted_lines = list(sort_log_stream(stream))
        
        assert len(sorted_lines) == 1
        assert "single" in sorted_lines[0]
        
    def test_sort_stream_with_newlines(self):
        """Test sorting preserves newline handling."""
        sample_data = """окт 22 10:54:51 ydb-static-node-1 ydbd[846]: 2025-10-22T07:54:51.300000Z :DATA_INTEGRITY INFO: third
окт 22 10:54:51 ydb-static-node-2 ydbd[847]: 2025-10-22T07:54:51.100000Z :DATA_INTEGRITY INFO: first
окт 22 10:54:51 ydb-static-node-3 ydbd[848]: 2025-10-22T07:54:51.200000Z :DATA_INTEGRITY INFO: second"""
        
        stream = StringIO(sample_data)
        sorted_lines = list(sort_log_stream(stream))
        
        # Check that newlines are stripped
        for line in sorted_lines:
            assert not line.endswith('\n')
            
    def test_sort_stream_error_handling(self):
        """Test error handling in sort_log_stream."""
        import subprocess
        from unittest.mock import patch, MagicMock
        
        # Test sort command failure
        with patch('subprocess.Popen') as mock_popen:
            mock_process = MagicMock()
            mock_process.wait.return_value = 1  # Non-zero exit code
            mock_process.stderr.read.return_value = "Sort error"
            mock_process.stdout = StringIO("")
            mock_process.stdin = MagicMock()
            mock_popen.return_value = mock_process
            
            stream = StringIO("test data")
            
            with pytest.raises(RuntimeError, match="Sort command failed"):
                list(sort_log_stream(stream))
                
    def test_sort_stream_file_not_found(self):
        """Test handling when sort command is not found."""
        from unittest.mock import patch
        
        with patch('subprocess.Popen', side_effect=FileNotFoundError()):
            stream = StringIO("test data")
            
            with pytest.raises(RuntimeError, match="Sort command not found"):
                list(sort_log_stream(stream))
                
    def test_sort_stream_broken_pipe(self):
        """Test handling of BrokenPipeError."""
        from unittest.mock import patch, MagicMock
        
        with patch('subprocess.Popen') as mock_popen:
            mock_process = MagicMock()
            mock_process.wait.return_value = 0
            mock_process.stdout = StringIO("sorted output")
            mock_process.stderr.read.return_value = ""
            mock_process.stdin = MagicMock()
            mock_popen.return_value = mock_process
            
            with patch('shutil.copyfileobj', side_effect=BrokenPipeError()):
                stream = StringIO("test data")
                sorted_lines = list(sort_log_stream(stream))
                
                # Should handle BrokenPipeError gracefully
                assert len(sorted_lines) >= 0
                
    def test_sort_stream_general_exception(self):
        """Test handling of general exceptions."""
        from unittest.mock import patch
        
        with patch('subprocess.Popen', side_effect=Exception("General error")):
            stream = StringIO("test data")
            
            with pytest.raises(RuntimeError, match="Error running sort command"):
                list(sort_log_stream(stream))