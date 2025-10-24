#!/usr/bin/env python3
"""
Для того, чтобы понять, кто сломал лок, нужно, начиная с строки в которой указано что случился TLI, 
найти несколько связных с ней предыдущих строк.
Получается цепочка связных друг с другом сообщений в логе. 
"""

from typing import List, Optional, Dict, Tuple
from log_parser import LogEntry
from chain_models import TraceInfo, LockInvalidationChain


class ChainTracer:
    """Ищет в логе все данные о TLI"""
    
    def __init__(self, log_entries: List[LogEntry]):
        self.log_entries = log_entries
        self._build_indexes()
    
    def _build_indexes(self):
        """Строит индексы для быстрого поиска записей по различным ID."""
        self.by_trace_id: Dict[str, List[LogEntry]] = {}  # Для поиска всех строк по TraceId
        self.by_tx_id: Dict[str, List[LogEntry]] = {}  # Для поиска всех строк по TxId
        self.py_tx_by_broken_lock_id: Dict[str, str] = {}   # Для поиска PhyTxId по LockId 
        self.trace_id_by_py_tx: Dict[str, str] = {}  # Для поиска TraceId по PhTxId
        
        for entry in self.log_entries:
            if entry.trace_id:
                self.by_trace_id.setdefault(entry.trace_id, []).append(entry)    
            if entry.tx_id:
                self.by_tx_id.setdefault(entry.tx_id, []).append(entry)      
            if entry.break_locks:
                for broken_lock_id in entry.break_locks:
                    self.py_tx_by_broken_lock_id[broken_lock_id] = entry.phy_tx_id
            if entry.phy_tx_id and entry.trace_id and entry.trace_id != 'Empty':
                self.trace_id_by_py_tx[entry.phy_tx_id] = entry.trace_id
        
    
    def find_all_invalidation_chains(self) -> List[LockInvalidationChain]:
        """Находит все цепочки TLI в логе."""
        chains = []
        
        # Ищем все ошибки TLI
        invalidated_entries = self.find_invalidated_entries()
        
        # Для каждой ошибки ищем причину
        for victim_entry in invalidated_entries:
            try:
                chain = self._trace_single_chain(victim_entry)
                if chain:
                    chains.append(chain)
            except Exception as e:
                print(f"Warning: Failed to trace chain for victim {victim_entry.session_id}: {e}")
                continue
        
        return chains
    
    def find_invalidated_entries(self) -> List[LogEntry]:
        """Находит все записи с сообщениями 'Transaction locks invalidated'."""
        invalidated_entries = []

        # Этот поиск делается один раз, поэтому создавать индекс не нужно
        
        for entry in self.log_entries:
            if (entry.issues and 
                "Transaction locks invalidated" in entry.issues and
                entry.status == "ABORTED"):
                invalidated_entries.append(entry)
        
        return invalidated_entries
    
    def _trace_single_chain(self, victim_entry: LogEntry) -> Optional[LockInvalidationChain]:
        """
        Ищет причину TLI по следующей цепочке:
        SessionId (жертва) → TraceId (жертва) → LockId → PhyTxId (виновник) → TraceId (виновник) → SessionId (виновник)
        Каждый шаг в этой цепочке - отдельное сообщение в логе, поэтому искать приходится последовательно
        """
        
        # Шаг 1: сессия, в которой случился TLI
        victim_session_id = victim_entry.session_id
        if not victim_session_id:
            return None
        
        # Шаг 2: Извлекаем TraceId запроса который упал с TLI
        victim_trace_id = victim_entry.trace_id
        if not victim_trace_id:
            return None
        
        # Шаг 3: Извлекаем всю информацию по trace_id жертвы
        victim_info = self._extract_trace_info(victim_entry.trace_id)
        lock_id = victim_info.lock_id
        if not lock_id:
            return None
        
        # Шаг 4: Ищем запись BreakLocks с этим LockId  - это сообщение о том, что кто-то сломал лок
        culprit_phy_tx_id = self.py_tx_by_broken_lock_id.get(lock_id)
        if not culprit_phy_tx_id:
            return None
        
        # Шаг 5: По PhyTxId ищем TraceId выражения, которое сломало лок
        culprit_trace_id = self.trace_id_by_py_tx.get(culprit_phy_tx_id)
        if not culprit_trace_id:
            return None
        
        # Шаг 6: Извлекаем всю информацию по culprit trace_id за один проход
        culprit_info = self._extract_trace_info(culprit_trace_id)
        culprit_session_id = culprit_info.session_id
        if not culprit_session_id:
            return None
        
        # Дополнительная информация
        table_name = self._extract_table_name(victim_entry.issues)
        
        # Используем уже извлеченную информацию (victim_info и culprit_info уже получены выше)
        victim_query_text = victim_info.query_text
        victim_tx_id = victim_info.tx_id
        culprit_query_text = culprit_info.query_text
        culprit_tx_id = culprit_info.tx_id
        culprit_entry = culprit_info.representative_entry
        
        # Извлекаем все запросы, выполненные в транзакции
        victim_queries = self._collect_all_queries_for_tx(victim_tx_id) if victim_tx_id else None
        culprit_queries = self._collect_all_queries_for_tx(culprit_tx_id) if culprit_tx_id else None
        
        return LockInvalidationChain(
            victim_session_id=victim_session_id,
            victim_trace_id=victim_trace_id,
            lock_id=lock_id,
            culprit_phy_tx_id=culprit_phy_tx_id,
            culprit_trace_id=culprit_trace_id,
            culprit_session_id=culprit_session_id,
            victim_entry=victim_entry,
            culprit_entry=culprit_entry,
            victim_query_text=victim_query_text,
            culprit_query_text=culprit_query_text,
            table_name=table_name,
            victim_tx_id=victim_tx_id,
            culprit_tx_id=culprit_tx_id,
            victim_queries=victim_queries,
            culprit_queries=culprit_queries
        )
    
    def _extract_trace_info(self, trace_id: str) -> TraceInfo:
        """
        Извлекает всю необходимую информацию по trace_id за один проход.
        Предполагается, что у каждого запроса будет свой TraceId
        Если это не так, то этот метод будет работать неправильно
        """
        trace_entries = self.by_trace_id.get(trace_id, [])
        info = TraceInfo()
        
        for entry in trace_entries:
            # Извлекаем query_text (первый найденный)
            if not info.query_text and entry.query_text:
                info.query_text = entry.query_text
                info.representative_entry = entry
            
            # Извлекаем tx_id (первый валидный найденный)
            if not info.tx_id and entry.tx_id and entry.tx_id != "Empty":
                info.tx_id = entry.tx_id
            
            # Извлекаем session_id (первый найденный)
            if not info.session_id and entry.session_id:
                info.session_id = entry.session_id
            
            # Извлекаем lock_id для LOCKS_BROKEN записей
            if not info.lock_id and entry.status == "LOCKS_BROKEN" and entry.lock_id:
                info.lock_id = entry.lock_id
            
        return info
    
    def _extract_table_name(self, issues: Optional[str]) -> Optional[str]:
        """Извлекает имя таблицы из описания TLI."""
        if not issues:
            return None
        
        # Ищет путь к таблице в сообщении о проблемах
        import re
        table_match = re.search(r'Table:\s*`([^`]+)`', issues)
        if table_match:
            return table_match.group(1)
        
        return None
    
    
    def _collect_all_queries_for_tx(self, tx_id: str) -> List[LogEntry]:
        """Ищем все запросы в транзакции"""
        if not tx_id:
            return []
        
        tx_entries = self.by_tx_id.get(tx_id, [])
        queries = []
        
        for entry in tx_entries:
            if entry.query_text and entry.query_action:
                queries.append(entry)
        
        # Сортируем по времени
        queries.sort(key=lambda x: x.timestamp or '')
        return queries

