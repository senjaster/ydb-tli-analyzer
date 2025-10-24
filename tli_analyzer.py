#!/usr/bin/env python3
"""
Анализатор инвалидации блокировок транзакций YDB

Утилита для анализа логов транзакций YDB и поиска всех событий инвалидации
блокировок транзакций вместе с их первопричинами (виновными сессиями).

Использование:
    python tli_analyzer.py --log-file <path> [--output <path>] [--verbose] [--sort]
"""

import argparse
import sys
import os
from typing import List, Optional

from log_parser import LogParser, LogEntry
from chain_tracer_single_pass import ChainTracerSinglePass
from chain_models import LockInvalidationChain
from yaml_reporter import YAMLReporter
from log_sorter import sort_log_stream


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
    python tli_analyzer.py --log-file docs/22_1.log --sort --output report.yaml --verbose
    
    # Using stdin with grep pre-filtering:
    grep "Transaction locks invalidated\\|Acquire lock\\|Break locks" docs/22_1_sorted.log | python tli_analyzer.py
    cat docs/22_1_sorted.log | python tli_analyzer.py --output report.yaml --verbose
    cat docs/22_1.log | python tli_analyzer.py --sort --output report.yaml --verbose
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
    
    parser.add_argument(
        '--sort', '-s',
        action='store_true',
        help='Sort log lines by timestamp in reverse chronological order before processing'
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
        analyze_logs(input_source, args.output, args.verbose, args.sort)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


def get_input_stream(input_source: Optional[str], sort_logs: bool):
    if input_source:
        # File input
        file_stream = open(input_source, 'r', encoding='utf-8')
        if sort_logs:
            return sort_log_stream(file_stream)
        return file_stream
    else:
        # Stdin input
        if sort_logs:
            return sort_log_stream(sys.stdin)
        return sys.stdin


def analyze_logs(input_source: Optional[str], output_file: str, verbose: bool = False, sort_logs: bool = False) -> None:
    """Анализирует лог (из файла или stdin) и генерирует отчет."""
    
    if input_source:
        print(f"Analyzing YDB log file: {input_source}")
        source_description = input_source
    else:
        print("Analyzing YDB log data from stdin...")
        source_description = "stdin"
    
    # Шаг 1: Создание парсера
    if verbose:
        print("Step 1: Setting up log parser...")
    
    parser = LogParser()
    
    # Шаг 2: Трассировка цепочек для поиска виновников (стриминг)
    if verbose:
        if sort_logs:
            print("Step 2: Sorting and streaming log analysis...")
        else:
            print("Step 2: Streaming log analysis and tracing chains...")
    
    try:
        input_stream = get_input_stream(input_source, sort_logs)
        log_entries_stream = parser.parse_stream(input_stream)
        tracer = ChainTracerSinglePass(log_entries_stream)
        chains = tracer.find_all_invalidation_chains()
        
    except Exception as e:
        raise Exception(f"Failed to analyze log data: {e}")
    
    print(f"Successfully traced {len(chains)} complete chains")
    
    if not chains:
        print("No transaction lock invalidation events found.")
        return
    
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