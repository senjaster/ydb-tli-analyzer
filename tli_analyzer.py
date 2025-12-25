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
import logging
from typing import List, Optional

from log_parser import LogParser, LogEntry, LogFormat
from chain_tracer_single_pass import ChainTracerSinglePass
from chain_models import LockInvalidationChain
from yaml_reporter import YAMLReporter
from sql_reporter import SQLReporter
from summary_reporter import SummaryReporter
from log_sorter import sort_log_stream


def main():
    """Главная точка входа для TLI анализатора."""
    
    parser = argparse.ArgumentParser(
        description="Analyze YDB transaction logs for lock invalidation events",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Analyze log and write all three reports to current directory:
    python tli_analyzer.py --log-file docs/22_1.log
    
    # Analyze log and write reports to specific folder:
    python tli_analyzer.py --log-file docs/22_1.log --output-folder ./results
    python tli_analyzer.py --log-file docs/22_1.log -o ./results
    
    # Using stdin with grep pre-filtering:
    grep "Transaction locks invalidated\\|Acquire lock\\|Break locks" docs/22_1.log | python tli_analyzer.py
    cat docs/22_1.log | python tli_analyzer.py --output-folder ./results
    cat docs/22_1_sorted.log | python tli_analyzer.py --no-sort -o ./results
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
        default='raw'
    )

    parser.add_argument(
        '--no-sort',
        action='store_true',
        help='Disable sorting of log lines by timestamp (logs are sorted by default)'
    )
    
    parser.add_argument(
        '--output-folder', '-o',
        help='Output folder for the report files (default: current directory)',
        default='.'
    )
    
    parser.add_argument(
        '--collect-details', '-d',
        action='store_true',
        help='Collect detailed log lines for each chain (may increase memory usage and processing time)'
    )
    
    # Create mutually exclusive group for verbosity options
    verbosity_group = parser.add_mutually_exclusive_group()
    
    verbosity_group.add_argument(
        '-v', '--verbose',
        action='count',
        default=0,
        help='Increase verbosity level. Use -v for INFO, -vv for DEBUG, -vvv for more detailed DEBUG messages'
    )
    
    verbosity_group.add_argument(
        '-q', '--quiet',
        action='store_true',
        help='Suppress all output except errors'
    )

    args = parser.parse_args()
    
    # Configure logging based on verbosity level

    if args.quiet:
        verbosity = -1
    else:
        verbosity = args.verbose

    _configure_logging(verbosity)
    
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
        analyze_logs(input_source, not args.no_sort, format, args.output_folder, args.collect_details)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def _configure_logging(verbosity: int) -> None:
    """Configure logging based on verbosity level."""
    if verbosity == -1:
        # Quiet
        level = logging.ERROR
    elif verbosity == 0:
        # Default: only show info and warnings
        level = logging.INFO
    else:
        # -v: show all messages
        level = logging.DEBUG
    
    # Configure the root logger
    logging.basicConfig(
        level=level,
        format='%(levelname)s: %(message)s',
        stream=sys.stderr
    )
    
    # For very verbose mode, show more detailed format
    if verbosity >= 3:
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            stream=sys.stderr,
            force=True  # Override previous configuration
        )


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


def analyze_logs(input_source: Optional[str], sort_logs: bool = True, format: LogFormat = LogFormat.SYSTEMD, output_folder: str = '.', collect_details: bool = False) -> None:
    """Анализирует лог (из файла или stdin) и генерирует отчет."""

    parser = LogParser(format)
    
    try:
        logging.info("Log annalyzis started")
        input_stream = get_input_stream(input_source, sort_logs, format)
        log_entries_stream = parser.parse_stream(input_stream)
        tracer = ChainTracerSinglePass(log_entries_stream)
        chains = tracer.find_all_invalidation_chains(collect_details=collect_details)
        logging.info("Log annalyzis completed")
        
    except Exception as e:
        logging.exception(f"Failed to analyze log data: {e}")
        raise e
    
    if not chains:
        logging.error("No transaction lock invalidation (TLI) events found in the logs.")
        logging.error("This could mean:")
        logging.error("  - The logs don't contain TLI events")
        logging.error("  - The log format is incorrect (try --log-format raw or systemd)")
        logging.error("  - The logs are missing required DATA_INTEGRITY entries")
        logging.error("  - The time period filtered doesn't contain TLI events")
        return
    
    # Create output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)
    
    # Write all three report formats to files
    sql_path = os.path.join(output_folder, 'report.sql')
    yaml_path = os.path.join(output_folder, 'report.yaml')
    summary_path = os.path.join(output_folder, 'summary.txt')
    
    # Generate SQL report
    try:
        logging.info(f"Writing SQL report to {sql_path}")
        with open(sql_path, 'w', encoding='utf-8') as f:
            sql_reporter = SQLReporter()
            sql_reporter.write_sql_report(chains, f)
    except Exception as e:
        logging.exception(f"Failed to generate SQL report: {e}")
        raise e
    
    # Generate YAML report
    try:
        logging.info(f"Writing YAML report to {yaml_path}")
        with open(yaml_path, 'w', encoding='utf-8') as f:
            yaml_reporter = YAMLReporter()
            yaml_reporter.write_yaml_report(chains, f)
    except Exception as e:
        logging.exception(f"Failed to generate YAML report: {e}")
        raise e
    
    # Generate summary report
    try:
        logging.info(f"Writing summary report to {summary_path}")
        with open(summary_path, 'w', encoding='utf-8') as f:
            summary_reporter = SummaryReporter()
            summary_reporter.write_summary_report(chains, f)
    except Exception as e:
        logging.exception(f"Failed to generate summary report: {e}")
        raise e
    
    logging.info(f"All reports written successfully to {output_folder}")


if __name__ == "__main__":
    main()