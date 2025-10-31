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
from ordered_set import OrderedSet
import re
import logging
from collections import defaultdict


class ChainTracerSinglePass:
    """Ищет в логе все данные о TLI за один проход по отсортированному в обратном порядке логу."""
    
    def __init__(self, log_entries: Iterable[LogEntry]):
        self.log_entries = log_entries
        # Словарь для хранения всех цепочек, ключ - trace_id жертвы
        self.chains: Dict[str, LockInvalidationChain] = {}
        # Словарь для сбора запросов по транзакциям
        self.queries_by_tx: Dict[str, List[LogEntry]] = defaultdict(list)
        # Словарь для отслеживания текущей транзакции в сессии.
        self._session_current_tx: dict[str, str] = {}
        # Словари для быстрого поиска цепочек по различным ID
        self.chains_by_lock_id: Dict[str, LockInvalidationChain] = {}  # лок уникальный, поэтому тут 1:1

        # Каждая PhyTxId может сломать множество транзакций, поэтому список
        self.chains_by_culprit_phy_tx_id: Dict[str, list[LockInvalidationChain]] = defaultdict(list)  
        
        # Из-за того что PhyTxId может сломать множество транзакций, тут тоже должен быть список
        self.chains_by_culprit_trace_id: Dict[str, list[LockInvalidationChain]] = defaultdict(list)  

        
        # Кэш для entries по trace_id (для получения culprit_entry и query_text)
        self.entries_by_trace: Dict[str, LogEntry] = {}
    
    def find_all_invalidation_chains(self, collect_details: bool = False) -> List[LockInvalidationChain]:
        """Находит все цепочки TLI в логе за один проход.
        
        Предполагается, что log_entries уже отсортированы в обратном хронологическом порядке.
        
        Args:
            collect_details: Если True, собирает детальную информацию о всех строках лога,
                           которые относятся к каждой цепочке в поле log_details.
        """
        # Перебираем строки последовательно и строим цепочки
        for entry in self.log_entries:
            self._process_entry(entry, collect_details)

        # Заполнить victim_queries и culprit_queries для найденных цепочек
        self._populate_queries()

        # Завершить все незавершенные цепочки
        self._complete_remaining_chains()
        
        # Проверить, что все цепочки полностью заполнены
        self._validate_chains()
        
        return [v for v in self.chains.values()]
    
    
    def _process_entry(self, entry: LogEntry, collect_details: bool):
        """Обрабатывает одну запись лога и обновляет цепочки."""
        # Собираем запросы для транзакций
        if entry.query_action:
            self._collect_transaction_queries(entry)

        # Запоминаем какая сейчас транзакция в сессии
        self._collect_session_tx(entry)
        
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
            self._create_new_chain(entry, collect_details)

        # Если в строке есть интересующий нас TraceId (victim)  и LOCKS_BROKEN - заполняем phy_tx_id для жертвы
        if entry.trace_id in self.chains and entry.status == "LOCKS_BROKEN" and entry.phy_tx_id:
            self._fill_victim_phy_tx_id(entry)

        # Если в строке есть интересующий нас TraceId (victim) и LOCKS_BROKEN - заполняем lock_id
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
    
    def _create_new_chain(self, entry: LogEntry, collect_details: bool = False):
        """Создает новую цепочку для TLI."""
        if not entry.trace_id or not entry.session_id:
            return
        
        # Извлекаем имя таблицы
        table_name = self._extract_table_name(entry.issues)
        
        # Создаем новую цепочку
        chain = LockInvalidationChain(
            victim_session_id=entry.session_id,
            victim_trace_id=entry.trace_id,
            victim_entry=entry,
            table_name=table_name,
            victim_tx_id=entry.tx_id if entry.tx_id != 'Unknown' else None,
            log_details=OrderedSet([entry.raw_line]) if collect_details else None
        )
        
        self.chains[entry.trace_id] = chain
        logging.debug(f"{chain.victim_trace_id} - Created new TLI chain for table {table_name}")
    
    def _fill_lock_id(self, entry: LogEntry):
        """Заполняет lock_id в цепочке."""
        chain = self.chains.get(entry.trace_id)
        if not chain:
            logging.warning(f"Expected to find chain for victim TraceId {entry.trace_id}, but not found")
            return
        
        if chain.log_details is not None:
            chain.log_details.add(entry.raw_line)
        
        if chain.lock_id:
            logging.warning(f"{chain.victim_trace_id} - Chain for TraceId {entry.trace_id} already has LockId {chain.lock_id}, ignoring new LockId {entry.lock_id}")
            return
            
        if not entry.lock_id:
            logging.warning(f"{chain.victim_trace_id} - Expected LockId in LOCKS_BROKEN entry for TraceId {entry.trace_id}, but not found")
            return
            
        # Пока что я не видел логов, в которых в строке с break_lock_id было бы несколько lock_id
        # Поэтому просто берем первый элемент списка - он же и единственный

        if len(entry.lock_id) > 1:
            logging.warning(f"{chain.victim_trace_id} - There are several LockId in BreakLocks row. This case is not handled.")

        first_lock_id = entry.lock_id[0] if isinstance(entry.lock_id, list) else entry.lock_id
        # Only log if we're actually setting a new value (chain.lock_id was empty)
        if not chain.lock_id:
            chain.lock_id = first_lock_id
            self.chains_by_lock_id[first_lock_id] = chain
            logging.debug(f"{chain.victim_trace_id} - Found lock_id {first_lock_id}")

    def _fill_victim_phy_tx_id(self, entry: LogEntry):
        """Заполняет victim_phy_tx_id в цепочке."""
        chain = self.chains.get(entry.trace_id)
        if not chain:
            logging.warning(f"Expected to find chain for victim TraceId {entry.trace_id}, but not found")
            return
        
        if chain.log_details is not None:
            chain.log_details.add(entry.raw_line)
        
        if chain.victim_phy_tx_id:
            logging.warning(f"{chain.victim_trace_id} - Chain for TraceId {entry.trace_id} already has victim PhyTxId {chain.victim_phy_tx_id}, ignoring new PhyTxId {entry.phy_tx_id}")
            return
            
        if not entry.phy_tx_id:
            logging.warning(f"{chain.victim_trace_id} - Expected PhyTxId in LOCKS_BROKEN entry for TraceId {entry.trace_id}, but not found")
            return
        
        # Only log if we're actually setting a new value (chain.victim_phy_tx_id was empty)
        if not chain.victim_phy_tx_id:
            chain.victim_phy_tx_id = entry.phy_tx_id
            logging.debug(f"{chain.victim_trace_id} - Found victim_phy_tx_id {entry.phy_tx_id}")

    def _fill_culprit_phy_tx_id(self, entry: LogEntry):
        """Заполняет culprit_phy_tx_id в цепочке по break_lock_id."""
        if not entry.break_lock_id:
            return
        
        if not entry.phy_tx_id:
            logging.warning(f"Expected PhyTxId in BreakLocks entry, but not found")
            return

        for lock_id in entry.break_lock_id:
            chain = self.chains_by_lock_id.get(lock_id)
            if not chain:
                # Это нормально - не все break_lock_id относятся к нашим цепочкам
                continue

            if chain.log_details is not None:
                chain.log_details.add(entry.raw_line)

            if chain.victim_phy_tx_id and chain.victim_phy_tx_id == entry.phy_tx_id:
                # Когда транзакция обнаруживает, что ее лок сломан, она логирует это и в BROKEN_LOCKS  и в LocksBroken
                # Поэтому приходится игнорировать часть строк. Не логирую, т.к. это совсем не интересно
                logging.debug(f"{chain.victim_trace_id} - skipping wrong LocksBroken entry")
                continue
                
            if chain.culprit_phy_tx_id and chain.culprit_phy_tx_id != entry.phy_tx_id:
                logging.warning(f"{chain.victim_trace_id} - Chain for LockId {lock_id} already has culprit PhyTxId {chain.culprit_phy_tx_id}, ignoring new PhyTxId {entry.phy_tx_id}. Only one culprit is included in the report")
                continue
            
            # Only log if we're actually setting a new value (chain.culprit_phy_tx_id was empty)
            if not chain.culprit_phy_tx_id:
                chain.culprit_phy_tx_id = entry.phy_tx_id
                self.chains_by_culprit_phy_tx_id[entry.phy_tx_id].append(chain)
                logging.debug(f"{chain.victim_trace_id} - Found culprit_phy_tx_id {entry.phy_tx_id} for lock_id {lock_id}")
    
    def _fill_culprit_trace_id(self, entry: LogEntry):
        """Заполняет culprit_trace_id в цепочке."""
        victim_chains = self.chains_by_culprit_phy_tx_id.get(entry.phy_tx_id)
        
        if not victim_chains:
            logging.warning(f"Expected to find chains for culprit PhyTxId {entry.phy_tx_id}, but not found")
            return

        for chain in victim_chains:
            if chain.log_details is not None:
                chain.log_details.add(entry.raw_line)
            
            if chain.culprit_trace_id and chain.culprit_trace_id != entry.trace_id:
                logging.warning(f"{chain.victim_trace_id} - Chain for PhyTxId {entry.phy_tx_id} already has culprit TraceId {chain.culprit_trace_id}, ignoring new TraceId {entry.trace_id}")
                return
                
            if not entry.trace_id or entry.trace_id == 'Empty':
                logging.warning(f"{chain.victim_trace_id} - Expected valid TraceId for PhyTxId {entry.phy_tx_id}, but got {entry.trace_id}")
                return
            
            # Only log if we're actually setting a new value (chain.culprit_trace_id was empty)
            if not chain.culprit_trace_id:
                chain.culprit_trace_id = entry.trace_id
                # Добавляем culprit trace_id в целевые для дальнейшего поиска
                self.chains_by_culprit_trace_id[entry.trace_id].append(chain)
                logging.debug(f"{chain.victim_trace_id} - Found culprit_trace_id {entry.trace_id}")
    
    def _fill_culprit_session_id(self, entry: LogEntry):
        """Заполняет culprit_session_id в цепочке."""
        victim_chains = self.chains_by_culprit_trace_id.get(entry.trace_id)
        if not victim_chains:
            logging.warning(f"Expected to find chain for culprit TraceId {entry.trace_id}, but not found")
            return
        
        for chain in victim_chains:
            if chain.log_details is not None:
                chain.log_details.add(entry.raw_line)
            
            if not entry.session_id:
                logging.warning(f"{chain.victim_trace_id} - Expected SessionId for culprit TraceId {entry.trace_id}, but not found")
                return
            
            if chain.culprit_session_id and chain.culprit_session_id != entry.session_id:
                # Это нормально - может быть несколько записей с одним session_id
                logging.warning(f"{chain.victim_trace_id} - Chain for trace_id {entry.trace_id} already has culprit_session_id {chain.culprit_session_id}, ignoring new SessionId {entry.session_id}")
                return
                  
            # Only log if we're actually setting a new value (chain.culprit_session_id was empty)
            if not chain.culprit_session_id:
                chain.culprit_session_id = entry.session_id
                logging.debug(f"{chain.victim_trace_id} - Found culprit_session_id {entry.session_id}")
    
    def _fill_culprit_tx_id(self, entry: LogEntry):
        """Заполняет culprit_tx_id в цепочке."""
        victim_chains = self.chains_by_culprit_trace_id.get(entry.trace_id)
        if not victim_chains:
            logging.warning(f"Expected to find chain for culprit TraceId {entry.trace_id}, but not found")
            return

        for chain in victim_chains:
            if chain.log_details is not None:
                chain.log_details.add(entry.raw_line)
            
            if not entry.tx_id or entry.tx_id == 'Empty':
                logging.warning(f"{chain.victim_trace_id} - Expected valid TxId, but got {entry.tx_id}")
                return

            if chain.culprit_tx_id == entry.tx_id:
                # Это нормально - может быть несколько строк лога
                return
            
            if chain.culprit_tx_id and chain.culprit_tx_id != entry.tx_id:
                # Обнаружился другой TxId - это ненормально, игнорируем его
                logging.debug(f"{chain.victim_trace_id} - Chain already has culprit_tx_id {chain.culprit_tx_id}, ignoring different tx_id {entry.tx_id}")
                return
                    
            # В поле TxId в некоторых случаях пишется PyTxId. Нужно игнорировать такие значения
            if entry.tx_id == chain.culprit_phy_tx_id:
                logging.debug(f"{chain.victim_trace_id} - Value {entry.tx_id} is not a real TxId. Skipping")
                return
            
            # Only log if we're actually setting a new value (chain.culprit_tx_id was empty)
            if not chain.culprit_tx_id:
                chain.culprit_tx_id = entry.tx_id
                logging.debug(f"{chain.victim_trace_id} - Found culprit_tx_id {entry.tx_id}")
    
    def _collect_session_tx(self, entry: LogEntry):
        """Запоминает, какая транзакция сейчас в сессии"""
        if entry.session_id and entry.tx_id:
            self._session_current_tx[entry.session_id] = entry.tx_id
        elif entry.session_id and entry.begin_tx:
            # Удаляю TxId т.к. этот запрос начал эту транзакцию
            self._session_current_tx.pop(entry.session_id, None)

    def _collect_transaction_queries(self, entry: LogEntry):
        """Собирает запросы для транзакций."""
        if entry.query_action:
            if entry.tx_id and entry.tx_id != 'Empty':
                self.queries_by_tx[entry.tx_id].append(entry)
            elif entry.session_id:
                # Если у запроса нет TxId то пытаемся найти текущую транзакцию для сессии
                # Поскольку мы идем по логу в обратном порядке, мы это можем сделать
                
                inferred_tx = self._session_current_tx.get(entry.session_id)
                if inferred_tx:
                    self.queries_by_tx[inferred_tx].append(entry)
                else:
                    logging.warning(f"Query has no TxId and unable to infer TxId from SessionId {entry.session_id}")
            else:
                logging.warning(f"Query entry has QueryText and QueryAction but no TxId and no SessionId")

    
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
    
    def _validate_chains(self):
        """Проверяет, что все цепочки имеют все необходимые поля заполненными."""
        incomplete_count = 0
        
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
            if not chain.victim_queries:
                missing_fields.append("victim_queries")
            if not chain.culprit_queries:
                missing_fields.append("culprit_queries")
            
            # Логируем предупреждения для любых отсутствующих полей
            if missing_fields:
                incomplete_count += 1
                logging.warning(f"{chain.victim_trace_id} - Incomplete chain. Missing fields: {', '.join(missing_fields)}")

        logging.info(f"Chain analysis complete: found {len(self.chains)} TLI chains, {incomplete_count} incomplete")
    
    def _extract_table_name(self, issues: Optional[str]) -> Optional[str]:
        """Извлекает имя таблицы из описания TLI."""
        if not issues:
            return None
        
        # Ищет путь к таблице в сообщении о проблемах
        table_match = re.search(r'Table:\s*`([^`]+)`', issues)
        if table_match:
            return table_match.group(1)
        
        return None
    
