#!/usr/bin/env python3
"""
Альтернативная реализация ChainTracer с использованием алгоритма одного прохода.

Главная идея - отсортировать лог в обратном порядке и строить все цепочки за один проход.
Записи в логе должны появляться строго в обратном порядке, начиная с жертвы (последнее сообщение) 
и заканчивая виновником (первое сообщение), поэтому достаточно будет одного скана.
"""

from typing import List, Optional, Dict, Iterable
from log_parser import LogEntry
from chain_models import LockInvalidationChain
import re
import logging


class ChainTracerSinglePass:
    """Ищет в логе все данные о TLI за один проход по отсортированному в обратном порядке логу."""
    
    def __init__(self, log_entries: Iterable[LogEntry]):
        self.log_entries = log_entries
        # Словарь для хранения всех цепочек, ключ - trace_id жертвы
        self.chains: Dict[str, LockInvalidationChain] = {}
        # Словарь для сбора запросов по транзакциям
        self.queries_by_tx: Dict[str, List[LogEntry]] = {}
        # Словари для быстрого поиска цепочек по различным ID
        self.chains_by_culprit_trace_id: Dict[str, LockInvalidationChain] = {}
        self.chains_by_lock_id: Dict[str, LockInvalidationChain] = {}
        self.chains_by_culprit_phy_tx_id: Dict[str, LockInvalidationChain] = {}
        # Кэш для entries по trace_id (для получения culprit_entry и query_text)
        self.entries_by_trace: Dict[str, LogEntry] = {}
    
    def find_all_invalidation_chains(self) -> List[LockInvalidationChain]:
        """Находит все цепочки TLI в логе за один проход.
        
        Предполагается, что log_entries уже отсортированы в обратном хронологическом порядке.
        """
        # Перебираем строки последовательно и строим цепочки
        for entry in self.log_entries:
            self._process_entry(entry)
        
        # Завершить все незавершенные цепочки
        self._complete_remaining_chains()
        
        # Заполнить victim_queries и culprit_queries для найденных цепочек
        self._populate_queries()
        
        # Проверить, что все цепочки полностью заполнены
        self._validate_chains()
        
        return list(self.chains.values())
    
    
    def _process_entry(self, entry: LogEntry):
        """Обрабатывает одну запись лога и обновляет цепочки."""
        # Собираем запросы для транзакций
        self._collect_query_if_needed(entry)
        
        # Кэшируем entries для всех trace_id, которые могут понадобиться
        if entry.trace_id and entry.trace_id != 'Empty':
            if entry.trace_id not in self.entries_by_trace:
                # Сохраняем первую встреченную запись
                self.entries_by_trace[entry.trace_id] = entry
            elif entry.session_id and not self.entries_by_trace[entry.trace_id].session_id:
                # Заменяем на запись с session_id, если текущая его не имеет
                self.entries_by_trace[entry.trace_id] = entry
            elif entry.query_text and not self.entries_by_trace[entry.trace_id].query_text:
                # Заменяем на запись с query_text, если текущая его не имеет
                self.entries_by_trace[entry.trace_id] = entry
        
        # Если в строке есть Transaction locks invalidated - создаем новую цепочку
        if self._is_transaction_locks_invalidated(entry):
            self._create_new_chain(entry)
        
        # Если в строке есть интересующий нас TraceId (victim) и LockId - заполняем lock_id в цепочке
        if entry.trace_id in self.chains and entry.status == "LOCKS_BROKEN" and entry.lock_id:
            self._fill_lock_id(entry)
        
        # Если в строке есть интересующий нас break_lock_id - заполняем culprit_phy_tx_id в цепочке
        if entry.break_lock_id:
            self._fill_culprit_phy_tx_id(entry)
        
        # Если в строке есть интересующий нас PhyTxId и TraceId не пустой - заполняем culprit_trace_id в цепочке
        if entry.phy_tx_id in self.chains_by_culprit_phy_tx_id and entry.trace_id and entry.trace_id != 'Empty':
            self._fill_culprit_trace_id(entry)
        
        # Если в строке есть интересующий нас TraceId и SessionId не пустой - заполняем culprit_session_id в цепочке
        if entry.trace_id in self.chains_by_culprit_trace_id and entry.session_id:
            self._fill_culprit_session_id(entry)
        
        # Если в строке есть интересующий нас TraceId и TxId - заполняем culprit_tx_id в цепочке
        if entry.trace_id in self.chains_by_culprit_trace_id and entry.tx_id and entry.tx_id != 'Empty':
            self._fill_culprit_tx_id(entry)
    
    def _is_transaction_locks_invalidated(self, entry: LogEntry) -> bool:
        """Проверяет, содержит ли запись сообщение о TLI."""
        return (entry.issues and 
                "Transaction locks invalidated" in entry.issues and
                entry.status == "ABORTED")
    
    def _create_new_chain(self, entry: LogEntry):
        """Создает новую цепочку для TLI."""
        if not entry.trace_id or not entry.session_id:
            return
        
        # Извлекаем имя таблицы
        table_name = self._extract_table_name(entry.issues)
        
        # Создаем новую цепочку
        chain = LockInvalidationChain(
            victim_session_id=entry.session_id,
            victim_trace_id=entry.trace_id,
            lock_id="",  # Будет заполнено позже
            culprit_phy_tx_id="",  # Будет заполнено позже
            culprit_trace_id="",  # Будет заполнено позже
            culprit_session_id="",  # Будет заполнено позже
            victim_entry=entry,
            table_name=table_name,
            victim_tx_id=entry.tx_id if entry.tx_id != 'Empty' else None
        )
        
        self.chains[entry.trace_id] = chain
    
    def _fill_lock_id(self, entry: LogEntry):
        """Заполняет lock_id в цепочке."""
        chain = self.chains.get(entry.trace_id)
        if not chain:
            logging.warning(f"Expected to find chain for victim trace_id {entry.trace_id}, but not found")
            return
        
        if chain.lock_id:
            logging.warning(f"Chain for trace_id {entry.trace_id} already has lock_id {chain.lock_id}, ignoring new lock_id {entry.lock_id}")
            return
            
        if not entry.lock_id:
            logging.warning(f"Expected lock_id in LOCKS_BROKEN entry for trace_id {entry.trace_id}, but not found")
            return
            
        # Пока что я не видело логов, в которых в строке с break_lock_id было бы несколько lock_id
        # Поэтому просто берем первый элемент списка - он же и единственный

        if len(chain.lock_id) > 1:
            logging.warning(f"There are several LockId in break_lock_id row. This case is not handled.")

        first_lock_id = entry.lock_id[0] if isinstance(entry.lock_id, list) else entry.lock_id
        chain.lock_id = first_lock_id
        self.chains_by_lock_id[first_lock_id] = chain
    
    def _fill_culprit_phy_tx_id(self, entry: LogEntry):
        """Заполняет culprit_phy_tx_id в цепочке по break_lock_id."""
        if not entry.break_lock_id:
            return
        
        for lock_id in entry.break_lock_id:
            chain = self.chains_by_lock_id.get(lock_id)
            if not chain:
                # Это нормально - не все break_lock_id относятся к нашим цепочкам
                continue
                
            if chain.culprit_phy_tx_id and chain.culprit_phy_tx_id != entry.phy_tx_id:
                logging.warning(f"Chain for lock_id {lock_id} already has culprit_phy_tx_id {chain.culprit_phy_tx_id}, ignoring new phy_tx_id {entry.phy_tx_id}")
                continue
                
            if not entry.phy_tx_id:
                logging.warning(f"Expected phy_tx_id in break_lock_id entry for lock_id {lock_id}, but not found")
                continue
                
            chain.culprit_phy_tx_id = entry.phy_tx_id
            self.chains_by_culprit_phy_tx_id[entry.phy_tx_id] = chain
    
    def _fill_culprit_trace_id(self, entry: LogEntry):
        """Заполняет culprit_trace_id в цепочке."""
        chain = self.chains_by_culprit_phy_tx_id.get(entry.phy_tx_id)
        if not chain:
            logging.warning(f"Expected to find chain for culprit phy_tx_id {entry.phy_tx_id}, but not found")
            return
            
        if chain.culprit_trace_id and chain.culprit_trace_id != entry.trace_id:
            logging.warning(f"Chain for phy_tx_id {entry.phy_tx_id} already has culprit_trace_id {chain.culprit_trace_id}, ignoring new trace_id {entry.trace_id}")
            return
            
        if not entry.trace_id or entry.trace_id == 'Empty':
            logging.warning(f"Expected valid trace_id for phy_tx_id {entry.phy_tx_id}, but got {entry.trace_id}")
            return
            
        chain.culprit_trace_id = entry.trace_id
        # Добавляем culprit trace_id в целевые для дальнейшего поиска
        self.chains_by_culprit_trace_id[entry.trace_id] = chain
    
    def _fill_culprit_session_id(self, entry: LogEntry):
        """Заполняет culprit_session_id в цепочке."""
        chain = self.chains_by_culprit_trace_id.get(entry.trace_id)
        if not chain:
            logging.warning(f"Expected to find chain for culprit trace_id {entry.trace_id}, but not found")
            return
            
        if chain.culprit_session_id:
            # Это нормально - может быть несколько записей с одним session_id
            logging.debug(f"Chain for trace_id {entry.trace_id} already has culprit_session_id {chain.culprit_session_id}, ignoring duplicate")
            return
            
        if not entry.session_id:
            logging.warning(f"Expected session_id for culprit trace_id {entry.trace_id}, but not found")
            return
            
        chain.culprit_session_id = entry.session_id
    
    def _fill_culprit_tx_id(self, entry: LogEntry):
        """Заполняет culprit_tx_id в цепочке."""
        chain = self.chains_by_culprit_trace_id.get(entry.trace_id)
        if not chain:
            logging.warning(f"Expected to find chain for culprit trace_id {entry.trace_id}, but not found")
            return
            
        if chain.culprit_tx_id:
            # Это нормально - может быть несколько tx_id для одного trace_id
            logging.debug(f"Chain for trace_id {entry.trace_id} already has culprit_tx_id {chain.culprit_tx_id}, ignoring additional tx_id")
            return
            
        if not entry.tx_id or entry.tx_id == 'Empty':
            logging.warning(f"Expected valid tx_id for culprit trace_id {entry.trace_id}, but got {entry.tx_id}")
            return
            
        # Берем первый валидный tx_id, который не является phy_tx_id
        if entry.tx_id == chain.culprit_phy_tx_id:
            logging.debug(f"tx_id {entry.tx_id} is same as phy_tx_id for trace_id {entry.trace_id}, skipping")
            return
            
        chain.culprit_tx_id = entry.tx_id
    
    def _collect_query_if_needed(self, entry: LogEntry):
        """Собирает запросы для транзакций."""
        if entry.tx_id and entry.tx_id != 'Empty' and entry.query_text and entry.query_action:
            if entry.tx_id not in self.queries_by_tx:
                self.queries_by_tx[entry.tx_id] = []
            self.queries_by_tx[entry.tx_id].append(entry)
    
    def _populate_queries(self):
        """Заполняет victim_queries и culprit_queries для найденных цепочек."""
        for chain in self.chains.values():
            # Заполняем victim_queries
            if chain.victim_tx_id:
                chain.victim_queries = self._get_sorted_queries(chain.victim_tx_id)
            
            # Заполняем culprit_queries
            if chain.culprit_tx_id:
                chain.culprit_queries = self._get_sorted_queries(chain.culprit_tx_id)
    
    def _get_sorted_queries(self, tx_id: str) -> List[LogEntry]:
        """Возвращает отсортированные по времени запросы для транзакции."""
        queries = self.queries_by_tx.get(tx_id, [])
        return sorted(queries, key=lambda x: x.timestamp or '')
    
    def _complete_remaining_chains(self):
        """Завершает все цепочки, заполняя недостающую информацию."""
        for chain in self.chains.values():
            # Используем кэшированные данные для заполнения недостающих полей
            culprit_entry = self.entries_by_trace.get(chain.culprit_trace_id)
            victim_entry = self.entries_by_trace.get(chain.victim_trace_id)
            
            # Заполняем culprit_entry если не заполнено
            if not chain.culprit_entry and culprit_entry:
                chain.culprit_entry = culprit_entry
            
            # Заполняем query_text если не заполнено
            if not chain.victim_query_text and victim_entry and victim_entry.query_text:
                chain.victim_query_text = victim_entry.query_text
            
            if not chain.culprit_query_text and culprit_entry and culprit_entry.query_text:
                chain.culprit_query_text = culprit_entry.query_text
    
    def _validate_chains(self):
        """Проверяет, что все цепочки имеют все необходимые поля заполненными."""
        for chain in self.chains.values():
            missing_fields = []
            
            # Все поля являются обязательными для полной цепочки
            if not chain.victim_session_id:
                missing_fields.append("victim_session_id")
            if not chain.victim_trace_id:
                missing_fields.append("victim_trace_id")
            if not chain.lock_id:
                missing_fields.append("lock_id")
            if not chain.culprit_phy_tx_id:
                missing_fields.append("culprit_phy_tx_id")
            if not chain.culprit_trace_id:
                missing_fields.append("culprit_trace_id")
            if not chain.culprit_session_id:
                missing_fields.append("culprit_session_id")
            if not chain.victim_entry:
                missing_fields.append("victim_entry")
            if not chain.culprit_entry:
                missing_fields.append("culprit_entry")
            if not chain.table_name:
                missing_fields.append("table_name")
            if not chain.victim_tx_id:
                missing_fields.append("victim_tx_id")
            if not chain.culprit_tx_id:
                missing_fields.append("culprit_tx_id")
            if not chain.victim_query_text:
                missing_fields.append("victim_query_text")
            if not chain.culprit_query_text:
                missing_fields.append("culprit_query_text")
            if not chain.victim_queries:
                missing_fields.append("victim_queries")
            if not chain.culprit_queries:
                missing_fields.append("culprit_queries")
            
            # Логируем предупреждения для любых отсутствующих полей
            if missing_fields:
                logging.warning(f"Incomplete chain for victim trace_id {chain.victim_trace_id} - missing fields: {', '.join(missing_fields)}")
    
    
    def _extract_table_name(self, issues: Optional[str]) -> Optional[str]:
        """Извлекает имя таблицы из описания TLI."""
        if not issues:
            return None
        
        # Ищет путь к таблице в сообщении о проблемах
        table_match = re.search(r'Table:\s*`([^`]+)`', issues)
        if table_match:
            return table_match.group(1)
        
        return None
    
