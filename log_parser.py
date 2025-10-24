#!/usr/bin/env python3
"""
Парсер логов для логов транзакций YDB.
Извлекает структурированные данные из записей логов YDB.
"""

import re
from typing import Dict, Optional, List, Iterator, TextIO
from dataclasses import dataclass
from datetime import datetime


@dataclass
class LogEntry:
    """Разобранная на части строка лога"""
    timestamp: str
    node: str
    process: str
    message_type: str
    log_level: str
    session_id: Optional[str] = None
    trace_id: Optional[str] = None
    phy_tx_id: Optional[str] = None
    lock_id: Optional[str] = None
    status: Optional[str] = None
    query_text: Optional[str] = None
    key: Optional[str] = None
    issues: Optional[str] = None
    break_locks: Optional[List[str]] = None
    tx_id: Optional[str] = None
    query_action: Optional[str] = None
    query_type: Optional[str] = None
    component: Optional[str] = None
    raw_line: str = ""
    

class LogParser:
    """Парсер лога"""
    
    def __init__(self):
        self.patterns = {
            'timestamp': r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)',  # UTC !!!
            'session_id': r'SessionId:\s*([^,\s]+)',
            'trace_id': r'TraceId:\s*([^,\s]+)',
            'phy_tx_id': r'PhyTxId:\s*(\d+)',
            'lock_id': r'LockId:\s*(\d+)',
            'status': r'Status:\s*([^,\s]+)',
            'query_text': r'QueryText:\s*([^,\s]+)',
            'key': r'Key:\s*([^,\s]+)',
            'issues': r'Issues:\s*\{([^}]+)\}',
            'break_locks': r'BreakLocks:\s*\[([^\]]+)\]',
            'component': r'Component:\s*([^,\s]+)',
            'tx_id': r'TxId:\s*([^,\s]+)',
            'query_action': r'QueryAction:\s*([^,\s]+)',
            'query_type': r'QueryType:\s*([^,\s]+)',
        }
        
        # Лучше заранее скомпилировать, так как данных может быть много
        self.compiled_patterns = {
            key: re.compile(pattern) for key, pattern in self.patterns.items()
        }
    
    def parse_line(self, line: str) -> Optional[LogEntry]:
        """Парсит одну строку лога"""
        if not line.strip():
            return None
            
        # Извлекаем обязательные поля, которые должны быть в любой строке
        # Формат: date time node process[pid]: timestamp :MESSAGE_TYPE LEVEL: ...
        # Дату, указанную в начале строки игнорируем - там нет миллисекунд
        basic_match = re.match(
            r'\w+\s+\d+\s+\d+:\d+:\d+\s+([^\s]+)\s+([^[]+)\[(\d+)\]:\s*(.+)',
            line
        )
        
        if not basic_match:
            return None
            
        node, process, pid, content = basic_match.groups()
        
        level_match = re.search(r':(\w+)\s+(\w+):', content)
        if not level_match:
            return None
            
        message_type, log_level = level_match.groups()
        
        entry = LogEntry(
            timestamp="",
            node=node,
            process=f"{process}[{pid}]",
            log_level=log_level,
            message_type=message_type,
            raw_line=line
        )
        
        # Извлекаем остальные поля
        for field, pattern in self.compiled_patterns.items():
            match = pattern.search(content)
            if match:
                value = match.group(1).strip()
                if field == 'break_locks':
                    # Список сломанных блокировок
                    lock_ids = [lock_id.strip() for lock_id in value.split()]
                    setattr(entry, field, lock_ids)
                else:
                    setattr(entry, field, value)
        
        return entry
    
    def parse_stream(self, stream: TextIO) -> Iterator[LogEntry]:
        """Парсит поток данных (файл или stdin) и возвращает генератор LogEntry объектов"""
        try:
            for line_num, line in enumerate(stream, 1):
                try:
                    entry = self.parse_line(line)
                    if entry:
                        yield entry
                except Exception as e:
                    print(f"Warning: Failed to parse line {line_num}: {e}")
                    continue
        except Exception as e:
            raise Exception(f"Error reading from stream: {e}")
    

