#!/usr/bin/env python3
"""
Анализатор инвалидации блокировок транзакций YDB

Утилита для анализа логов транзакций YDB и поиска всех событий инвалидации
блокировок транзакций вместе с их первопричинами (виновными сессиями).

Использование:
    python tli_analyzer.py --log-file <path> [--output <path>] [--verbose]
"""

import argparse
import sys
import os
from typing import List, Optional

from log_parser import LogParser, LogEntry
from chain_tracer_single_pass import ChainTracerSinglePass
from chain_models import LockInvalidationChain
from yaml_reporter import YAMLReporter


def main():
    """Главная точка входа для TLI анализатора."""
    
    parser = argparse.ArgumentParser(
        description="Analyze YDB transaction logs for lock invalidation events",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python tli_analyzer.py --log-file docs/22_1_sorted.log
    python tli_analyzer.py --log-file docs/22_1_sorted.log --output report.yaml
    python tli_analyzer.py --log-file docs/22_1_sorted.log --output report.yaml --verbose
    
    # Using stdin with grep pre-filtering:
    grep "Transaction locks invalidated\\|Acquire lock\\|Break locks" docs/22_1_sorted.log | python tli_analyzer.py
    cat docs/22_1_sorted.log | python tli_analyzer.py --output report.yaml --verbose
        """
    )
    
    parser.add_argument(
        '--log-file', '-f',
        help='Path to the YDB log file to analyze (if not provided, reads from stdin)'
    )
    
    parser.add_argument(
        '--output', '-o',
        default='tli_analysis_report.yaml',
        help='Output file for the YAML report (default: tli_analysis_report.yaml)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )
    
    args = parser.parse_args()
    
    # Validate input - either file or stdin
    if args.log_file:
        if not os.path.exists(args.log_file):
            print(f"Error: Log file not found: {args.log_file}")
            sys.exit(1)
        input_source = args.log_file
    else:
        # Check if stdin has data
        if sys.stdin.isatty():
            print("Error: No input provided. Either specify --log-file or pipe data to stdin.")
            print("Use --help for usage examples.")
            sys.exit(1)
        input_source = None  # Will use stdin
    
    try:
        analyze_logs(input_source, args.output, args.verbose)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


def analyze_logs(input_source: Optional[str], output_file: str, verbose: bool = False) -> None:
    """Анализирует лог (из файла или stdin) и генерирует отчет."""
    
    if input_source:
        print(f"Analyzing YDB log file: {input_source}")
        source_description = input_source
    else:
        print("Analyzing YDB log data from stdin...")
        source_description = "stdin"
    
    # Шаг 1: Парсинг лога
    if verbose:
        print("Step 1: Parsing log data...")
    
    parser = LogParser()
    try:
        if input_source:
            log_entries = parser.parse_file(input_source)
        else:
            log_entries = parser.parse_stream(sys.stdin)
        print(f"Parsed {len(log_entries)} log entries")
    except Exception as e:
        raise Exception(f"Failed to parse log data: {e}")
    
    if not log_entries:
        print("No valid log entries found in the file.")
        return
    
    # Шаг 2: Трассировка цепочек для поиска виновников
    if verbose:
        print("Step 2: Tracing chains to find culprit sessions...")
    
    tracer = ChainTracerSinglePass(log_entries)
    
    # Сначала найти инвалидированные записи для подсчета
    invalidated_entries = tracer.find_invalidated_entries()
    print(f"Found {len(invalidated_entries)} 'Transaction locks invalidated' events")
    
    if not invalidated_entries:
        print("No transaction lock invalidation events found.")
        return
    
    chains = tracer.find_all_invalidation_chains()
    
    print(f"Successfully traced {len(chains)} complete chains")
    
    if len(chains) < len(invalidated_entries):
        failed_count = len(invalidated_entries) - len(chains)
        print(f"Warning: Failed to trace {failed_count} chains (incomplete data)")
    
    # Шаг 3: Генерация YAML отчета
    if verbose:
        print("Step 3: Generating YAML report...")
    
    reporter = YAMLReporter()
    
    try:
        reporter.write_yaml_report(chains, source_description, output_file)
    except Exception as e:
        raise Exception(f"Failed to generate report: {e}")
    
    # Шаг 4: Вывод сводки
    reporter.print_summary(chains)
    
    print(f"\nAnalysis complete! Report saved to: {output_file}")


if __name__ == "__main__":
    main()