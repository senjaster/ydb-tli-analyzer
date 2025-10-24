#!/usr/bin/env python3
"""
YAML репортер для анализа инвалидации блокировок транзакций YDB.
Генерирует структурированные YAML отчеты из проанализированных цепочек.
"""

import yaml
import sys
from typing import List, Dict, Any
from datetime import datetime
from chain_tracer import LockInvalidationChain


class QueryTextStr(str):
    """Пользовательский класс строки для текста запроса для включения свернутого YAML форматирования."""
    pass


def query_text_representer(dumper, data):
    """Пользовательский YAML представитель для текста запроса с использованием свернутого стиля."""
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='>')


# Регистрирует пользовательский представитель
yaml.add_representer(QueryTextStr, query_text_representer)


class YAMLReporter:
    """Генерирует YAML отчеты из цепочек инвалидации блокировок."""
    
    def __init__(self):
        pass
    
    def generate_report(self, chains: List[LockInvalidationChain], log_file_path: str) -> Dict[str, Any]:
        """Генерирует полный YAML отчет из проанализированных цепочек."""
        
        report = {
            'analysis_metadata': {
                'generated_at': datetime.now().isoformat(),
                'log_file': log_file_path,
                'total_invalidation_events': len(chains),
            },
            'lock_invalidation_events': []
        }
        
        for i, chain in enumerate(chains, 1):
            event = self._format_chain_as_event(chain, i)
            report['lock_invalidation_events'].append(event)
        
        return report
    
    def _format_chain_as_event(self, chain: LockInvalidationChain, event_id: int) -> Dict[str, Any]:
        """Форматирует одну цепочку как структуру YAML события."""
        
        event = {
            'event_id': event_id,
            'timestamp': chain.victim_entry.timestamp,
            'table': chain.table_name,
            'victim': {
                'session_id': chain.victim_session_id,
                'trace_id': chain.victim_trace_id,
                'node': chain.victim_entry.node,
                'process': chain.victim_entry.process
            },
            'culprit': {
                'session_id': chain.culprit_session_id,
                'trace_id': chain.culprit_trace_id,
                'phy_tx_id': chain.culprit_phy_tx_id
            },
            'lock_details': {
                'lock_id': chain.lock_id
            }
        }
        
        # Добавляет ID транзакций, если доступны
        if chain.victim_tx_id:
            event['victim']['tx_id'] = chain.victim_tx_id
        
        if chain.culprit_tx_id:
            event['culprit']['tx_id'] = chain.culprit_tx_id
        
        # Добавляет узел/процесс виновника, если доступны
        if chain.culprit_entry:
            event['culprit']['node'] = chain.culprit_entry.node
            event['culprit']['process'] = chain.culprit_entry.process
        
        # Добавляет тексты запросов, если доступны (для обратной совместимости)
        if chain.victim_query_text:
            event['victim']['query_text'] = QueryTextStr(chain.victim_query_text)
        
        if chain.culprit_query_text:
            event['culprit']['query_text'] = QueryTextStr(chain.culprit_query_text)
        
        # Добавляет все запросы для каждой транзакции
        if chain.victim_queries:
            formatted_victim_queries = []
            for query_entry in chain.victim_queries:
                formatted_query = {
                    'query_text': QueryTextStr(query_entry.query_text) if query_entry.query_text else None,
                    'query_action': query_entry.query_action,
                    'trace_id': query_entry.trace_id or '',
                    'timestamp': query_entry.timestamp or '',
                    'query_type': query_entry.query_type or ''
                }
                formatted_victim_queries.append(formatted_query)
            event['victim']['all_queries'] = formatted_victim_queries

        if chain.culprit_queries:
            formatted_culprit_queries = []
            for query_entry in chain.culprit_queries:
                formatted_query = {
                    'query_text': QueryTextStr(query_entry.query_text) if query_entry.query_text else None,
                    'query_action': query_entry.query_action,
                    'trace_id': query_entry.trace_id or '',
                    'timestamp': query_entry.timestamp or '',
                    'query_type': query_entry.query_type or ''
                }
                formatted_culprit_queries.append(formatted_query)
            event['culprit']['all_queries'] = formatted_culprit_queries
        
        # Добавляет сырые записи лога для справки
        event['raw_entries'] = {
            'victim_log_line': chain.victim_entry.raw_line.strip()
        }
        
        if chain.culprit_entry:
            event['raw_entries']['culprit_log_line'] = chain.culprit_entry.raw_line.strip()
        
        return event
    
    def write_yaml_report(self, chains: List[LockInvalidationChain],
                         log_file_path: str, output_file: str) -> None:
        """Записывает YAML отчет в файл."""
        
        report = self.generate_report(chains, log_file_path)
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                yaml.dump(report, f, 
                         default_flow_style=False, 
                         allow_unicode=True,
                         sort_keys=False,
                         indent=2)
            print(f"Report written to: {output_file}", file=sys.stderr)
        except Exception as e:
            raise Exception(f"Failed to write YAML report to {output_file}: {e}")
    
    def write_yaml_report_to_stdout(self, chains: List[LockInvalidationChain], log_file_path: str) -> None:
        """Записывает YAML отчет в stdout."""
        
        report = self.generate_report(chains, log_file_path)
        
        try:
            import yaml
            yaml.dump(report, sys.stdout,
                     default_flow_style=False,
                     allow_unicode=True,
                     sort_keys=False,
                     indent=2)
        except Exception as e:
            raise Exception(f"Failed to write YAML report to stdout: {e}")
    
    def print_summary(self, chains: List[LockInvalidationChain]) -> None:
        """Выводит сводку результатов анализа."""
        
        print(f"\n=== YDB Transaction Lock Invalidation Analysis Summary ===")
        print(f"Total invalidation events found: {len(chains)}")
        
        if not chains:
            print("No lock invalidation events found in the log.")
            return
        
        print(f"\nEvents breakdown:")
        
        # Группирует по таблицам
        tables = {}
        for chain in chains:
            table = chain.table_name or "Unknown"
            tables[table] = tables.get(table, 0) + 1
        
        for table, count in tables.items():
            print(f"  {table}: {count} events")
        
        print(f"\nFirst few events:")
        for i, chain in enumerate(chains[:3], 1):
            print(f"  Event {i}:")
            print(f"    Timestamp: {chain.victim_entry.timestamp}")
            print(f"    Table: {chain.table_name}")
            print(f"    Victim: {self._format_session_short(chain.victim_session_id)}")
            print(f"    Culprit: {self._format_session_short(chain.culprit_session_id)}")
        
        if len(chains) > 3:
            print(f"    ... and {len(chains) - 3} more events")
    
    def print_summary_to_stderr(self, chains: List[LockInvalidationChain]) -> None:
        """Выводит сводку результатов анализа в stderr."""
        
        print(f"\n=== YDB Transaction Lock Invalidation Analysis Summary ===", file=sys.stderr)
        print(f"Total invalidation events found: {len(chains)}", file=sys.stderr)
        
        if not chains:
            print("No lock invalidation events found in the log.", file=sys.stderr)
            return
        
        print(f"\nEvents breakdown:", file=sys.stderr)
        
        # Группирует по таблицам
        tables = {}
        for chain in chains:
            table = chain.table_name or "Unknown"
            tables[table] = tables.get(table, 0) + 1
        
        for table, count in tables.items():
            print(f"  {table}: {count} events", file=sys.stderr)
        
        print(f"\nFirst few events:", file=sys.stderr)
        for i, chain in enumerate(chains[:3], 1):
            print(f"  Event {i}:", file=sys.stderr)
            print(f"    Timestamp: {chain.victim_entry.timestamp}", file=sys.stderr)
            print(f"    Table: {chain.table_name}", file=sys.stderr)
            print(f"    Victim: {self._format_session_short(chain.victim_session_id)}", file=sys.stderr)
            print(f"    Culprit: {self._format_session_short(chain.culprit_session_id)}", file=sys.stderr)
        
        if len(chains) > 3:
            print(f"    ... and {len(chains) - 3} more events", file=sys.stderr)
    
    def _format_session_short(self, session_id: str) -> str:
        """Форматирует ID сессии для краткого отображения."""
        if not session_id:
            return "Unknown"
        
        # Извлекает только узел и короткий ID для читаемости
        parts = session_id.split('?')
        if len(parts) > 1:
            node_part = parts[1].split('&')[0] if '&' in parts[1] else parts[1]
            id_part = parts[1].split('id=')[1][:8] + "..." if 'id=' in parts[1] else ""
            return f"{node_part} ({id_part})"
        
        return session_id[:50] + "..." if len(session_id) > 50 else session_id

