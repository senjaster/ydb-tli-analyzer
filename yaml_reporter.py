#!/usr/bin/env python3
"""
YAML репортер для анализа инвалидации блокировок транзакций YDB.
Генерирует структурированные YAML отчеты из проанализированных цепочек.
"""

import yaml
import sys
from typing import List, Dict, Any
from datetime import datetime
from chain_models import LockInvalidationChain
from datetime import datetime


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
    
    def generate_report(self, chains: List[LockInvalidationChain]) -> Dict[str, Any]:
        """Генерирует полный YAML отчет из проанализированных цепочек."""
        
        report = {
            'analysis_metadata': {
                'generated_at': datetime.now().isoformat(),
                'total_invalidation_events': len(chains),
            },
            'lock_invalidation_events': []
        }

        sorted_chains = sorted(chains, key=lambda x:datetime.fromisoformat(x.victim_entry.timestamp))
        
        for i, chain in enumerate(sorted_chains, 1):
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
            },
            'victim_committed': chain.is_victim_committed
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


        
        if chain.log_details:
            # Добавляет детальные записи лога в обратном порядке, если доступны
            event['raw_entries'] = {
                 'detailed_log_lines': [line.strip() for line in reversed(chain.log_details)]
            }
        else:
            # А если нет - то только первую и последнюю строку в цепочке
            event['raw_entries'] = {
                'victim_log_line': chain.victim_entry.raw_line.strip()
            }
            
            if chain.culprit_entry:
                event['raw_entries']['culprit_log_line'] = chain.culprit_entry.raw_line.strip()
            

        
        return event
    
    def write_yaml_report(self, chains: List[LockInvalidationChain], file=sys.stdout) -> None:
        """Записывает YAML отчет в stdout."""
        
        report = self.generate_report(chains)
        
        try:
            import yaml
            yaml.dump(report, file,
                     default_flow_style=False,
                     allow_unicode=True,
                     sort_keys=False,
                     indent=2)
        except Exception as e:
            raise Exception(f"Failed to write YAML report to stdout: {e}")
  