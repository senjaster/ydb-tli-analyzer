# Анализатор TLI в YDB

Утилита для поиска причин ошибок "Transaction locks invalidated" по серверным логам ydb

Собирает из серверных логов полную картину причин, по которым возникает данная ошибка

## Установка

## Настройка сервера

По умолчанию ydb не логирует информацию об ошибках TLI и выполняемых запросах.
Чтобы включить сбор этой информации нужно добавить в динамический конфиг следующие настройки:

```yaml
      log_config: !inherit
        entry: !append
          - component: DATA_INTEGRITY
            level: 8
      data_integrity_trails_config: !inherit
        query_text_log_mode: ORIGINAL
        keys_log_mode: ORIGINAL
```

После применения конфига необходимо перезагрузить узлы базы данных.

Включать секцию `data_integrity_trails_config` не обязательно, по-умолчанию в лог вместо текста запроса будут выводиться его хэш.
В целом, если клиентское приложение может логировать запросы самостоятельно, лучше не писать их в лог сервера.

ВАЖНО: после применения этих настроек в лог будет писаться очень подробная информация обо ВСЕХ выполняемых запросах и блокировках.
На каждый отправляемый запрос в лог будет записано около 10 строк. Настролько подробное логирование отрицательно сказывается на производительности
сервера и сильно увеличивает размер логов. Не рекомендуется включать эту настройки в прод-окружении!

## Использование

На вход приложения нужно подать объединенные в один файл логи со всех серверов. Для ускорения анализа нужно оставить только строки, содержащие DATA_INTEGRITY.
Желательно дополнительно ограничить период времени, за который анализируются логи. 

Сортировать строки не требуется. 

Пример использования: 
```bash
grep DATA_INTEGRITY *.log | grep "2025-10-22T12:06" | python tli_analyzer.py > report.yaml

```

## Формат вывода

Инструмент генерирует YAML отчет со следующей структурой:

```yaml
analysis_metadata:
  generated_at: '2025-10-24T14:05:40.437463'
  log_file: logs/filtered.log
  total_invalidation_events: 13
lock_invalidation_events:
- event_id: 1
  timestamp: '2025-10-23T07:45:21.649187Z'
  table: /Root/database/test_schema_c0af7263/tt1
  victim:
    session_id: ydb://session/3?node_id=50003&id=NzU5NzhkYzctNWY1NDIxMDktMTI5MGI4NjgtOGI3YzY0NWE=
    trace_id: tx2_1761205521
    node: ydb-static-node-3
    process: ydbd[102832]
    tx_id: 01k880f31b5sejzffqsp773w8d
    query_text: >-
      47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=
    all_queries:
    - query_text: >-
        OdbIZxoO/+WWcr5N4x+Tp6aSfkNxHwLulRCIn9b9GdM=
      query_action: QUERY_ACTION_EXECUTE
      trace_id: tx2_1761205521
      timestamp: '2025-10-23T07:45:21.513843Z'
      query_type: QUERY_TYPE_SQL_GENERIC_QUERY
    - query_text: >-
        iVPJ7oBYRFVetXpP3b0/QId3Y8nvg777DNWMe5bylNs=
      query_action: QUERY_ACTION_EXECUTE
      trace_id: tx2_1761205521
      timestamp: '2025-10-23T07:45:21.592397Z'
      query_type: QUERY_TYPE_SQL_GENERIC_QUERY
    - query_text: >-
        47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=
      query_action: QUERY_ACTION_COMMIT_TX
      trace_id: tx2_1761205521
      timestamp: '2025-10-23T07:45:21.646663Z'
      query_type: QUERY_TYPE_UNDEFINED
  culprit:
    session_id: ydb://session/3?node_id=50002&id=OWUxOWU3Ny02ZWE0YzhlOS1mNzlhNWY3Yi03YzVkYzA1YQ==
    trace_id: tx1_1761205521
    phy_tx_id: '281474977265671'
    tx_id: 01k880f2zycb4xbqhf8kf75f7t
    node: ydb-static-node-1
    process: ydbd[50910]
    query_text: >-
      47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=
    all_queries:
    - query_text: >-
        OdbIZxoO/+WWcr5N4x+Tp6aSfkNxHwLulRCIn9b9GdM=
      query_action: QUERY_ACTION_EXECUTE
      trace_id: tx1_1761205521
      timestamp: '2025-10-23T07:45:21.469543Z'
      query_type: QUERY_TYPE_SQL_GENERIC_QUERY
    - query_text: >-
        l8H2guoHrp6itYGveGJ7g6CDPeVVqiyLkae9kUr3JxU=
      query_action: QUERY_ACTION_EXECUTE
      trace_id: tx1_1761205521
      timestamp: '2025-10-23T07:45:21.592126Z'
      query_type: QUERY_TYPE_SQL_GENERIC_QUERY
    - query_text: >-
        47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=
      query_action: QUERY_ACTION_COMMIT_TX
      trace_id: tx1_1761205521
      timestamp: '2025-10-23T07:45:21.619480Z'
      query_type: QUERY_TYPE_UNDEFINED
  lock_details:
    lock_id: '281474977285657'
  raw_entries:
    victim_log_line: 'окт 23 10:45:21 ydb-static-node-3 ydbd[102832]: 2025-10-23T07:45:21.649187Z
      :DATA_INTEGRITY DEBUG: Component: SessionActor,SessionId: ydb://session/3?node_id=50003&id=NzU5NzhkYzctNWY1NDIxMDktMTI5MGI4NjgtOGI3YzY0NWE=,TraceId:
      tx2_1761205521,Type: Response,TxId: 01k880f31b5sejzffqsp773w8d,Status: ABORTED,Issues:
      { message: "Transaction locks invalidated. Table: `/Root/database/test_schema_c0af7263/tt1`"
      issue_code: 2001 severity: 1 }'
    culprit_log_line: 'окт 23 10:45:21 ydb-static-node-1 ydbd[50910]: 2025-10-23T07:45:21.619480Z
      :DATA_INTEGRITY DEBUG: Component: SessionActor,SessionId: ydb://session/3?node_id=50002&id=OWUxOWU3Ny02ZWE0YzhlOS1mNzlhNWY3Yi03YzVkYzA1YQ==,TraceId:
      tx1_1761205521,Type: Request,QueryAction: QUERY_ACTION_COMMIT_TX,QueryType:
      QUERY_TYPE_UNDEFINED,QueryText: 47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=,TxId:
      01k880f2zycb4xbqhf8kf75f7t,NeedCommitTx: true,'
```

Здесь для примера вместо текста запросов выведены хэши.
