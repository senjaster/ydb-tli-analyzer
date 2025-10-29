#!/usr/bin/env python3
"""
Анализатор инвалидации блокировок транзакций YDB

Утилита для анализа логов транзакций YDB и поиска всех событий инвалидации
блокировок транзакций вместе с их первопричинами (виновными сессиями).

Использование:
    python tli_analyzer.py --log-file <path> [--no-sort]
"""

import argparse
import sys
import os
from typing import List, Optional

from log_parser import LogParser, LogEntry, LogFormat
from chain_tracer_single_pass import ChainTracerSinglePass
from chain_models import LockInvalidationChain
from yaml_reporter import YAMLReporter
from sql_reporter import SQLReporter
from log_sorter import sort_log_stream


def main():
    """Главная точка входа для TLI анализатора."""
    
    parser = argparse.ArgumentParser(
        description="Analyze YDB transaction logs for lock invalidation events",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # YAML output (default):
    python tli_analyzer.py --log-file docs/22_1.log
    python tli_analyzer.py --log-file docs/22_1.log > report.yaml
    python tli_analyzer.py --log-file docs/22_1_sorted.log --no-sort > report.yaml
    
    # SQL-script-like output:
    python tli_analyzer.py --log-file docs/22_1.log --output-format sql
    python tli_analyzer.py --log-file docs/22_1.log -o sql > report.sql
    
    # Using stdin with grep pre-filtering:
    grep "Transaction locks invalidated\\|Acquire lock\\|Break locks" docs/22_1.log | python tli_analyzer.py -o sql
    cat docs/22_1.log | python tli_analyzer.py --output-format yaml > report.yaml
    cat docs/22_1_sorted.log | python tli_analyzer.py --no-sort -o sql > report.sql
        """
    )
    
    parser.add_argument(
        '--log-file',
        help='Path to the YDB log file to analyze (if not provided, reads from stdin)'
    )
    
    parser.add_argument(
        '--log-format', '-f',
        help='Specify the input log format. "systemd" for logs from journalctl (default), "raw" for direct ydbd log files',
        choices=['systemd', 'raw'],
        default='systemd'
    )

    parser.add_argument(
        '--no-sort',
        action='store_true',
        help='Disable sorting of log lines by timestamp (logs are sorted by default)'
    )
    
    parser.add_argument(
        '--output-format', '-o',
        help='Output format for the report',
        choices=['yaml', 'sql'],
        default='yaml'
    )
    
    args = parser.parse_args()
    
    # Validate input - either file or stdin
    if args.log_file:
        if not os.path.exists(args.log_file):
            print(f"Error: Log file not found: {args.log_file}", file=sys.stderr)
            sys.exit(1)
        input_source = args.log_file
    else:
        # Check if stdin has data
        if sys.stdin.isatty():
            print("Error: No input provided. Either specify --log-file or pipe data to stdin.", file=sys.stderr)
            print("Use --help for usage examples.", file=sys.stderr)
            sys.exit(1)
        input_source = None  # Will use stdin

    format = LogFormat(args.log_format or 'systemd')
    
    try:
        analyze_logs(input_source, not args.no_sort, format, args.output_format)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def get_input_stream(input_source: Optional[str], sort_logs: bool, format: LogFormat):
    if input_source:
        # File input
        file_stream = open(input_source, 'r', encoding='utf-8')
        if sort_logs:
            return sort_log_stream(file_stream, format)
        return file_stream
    else:
        # Stdin input
        if sort_logs:
            return sort_log_stream(sys.stdin, format)
        return sys.stdin


def analyze_logs(input_source: Optional[str], sort_logs: bool = True, format: LogFormat = LogFormat.SYSTEMD, output_format: str = 'yaml') -> None:
    """Анализирует лог (из файла или stdin) и генерирует отчет."""
    
    parser = LogParser(format)
    
    try:
        input_stream = get_input_stream(input_source, sort_logs, format)
        log_entries_stream = parser.parse_stream(input_stream)
        tracer = ChainTracerSinglePass(log_entries_stream)
        chains = tracer.find_all_invalidation_chains()
        
    except Exception as e:
        print(e)
        raise Exception(f"Failed to analyze log data: {e}")
    
    if not chains:
        return
    
    # Choose reporter based on output format
    if output_format == 'sql':
        reporter = SQLReporter()
        try:
            reporter.write_sql_report(chains)
        except Exception as e:
            raise Exception(f"Failed to generate SQL report: {e}")
    else:  # default to yaml
        reporter = YAMLReporter()
        try:
            reporter.write_yaml_report(chains)
        except Exception as e:
            raise Exception(f"Failed to generate YAML report: {e}")


if __name__ == "__main__":
    main()