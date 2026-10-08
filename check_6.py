# -*- coding: utf-8 -*-
"""
Диагностика: проверка наличия данных между 15:59 и 16:05
"""

import clickhouse_connect
import pandas as pd

# ==================== НАСТРОЙКИ ====================
CH_HOST     = '135.106.148.141'
CH_PORT     = 8123
CH_USER     = 'default'
CH_PASSWORD = 'YourSuperStrongP@ssw0rd!2026'
CH_DATABASE = 'default'

TABLE    = 'hist_creyt_timeseries_data'
COL_REC  = 'data_record_sk'
COL_PROP = 'h_object_property_sk'
COL_VAL  = 'value'
COL_TS   = 'timestamp'

# Каналы: колонка в parquet -> h_object_property_sk
CHANNELS = {
    'x1':    '040dcfb3-edaa-4430-d826-77a8dec1e635',
    'y1':    'f256d1f9-6841-1e5b-9ec2-bd6e5fba67d7',
    'x2':    '65301f9f-3de1-16a0-faf0-3c026eefe952',
    'y2':    'c82e154c-b53d-792b-5829-37d777c16000',
    'phase': '7379afbe-c0c0-9ab9-fab8-1b449266e124',
}

START_DT = '2026-07-31 15:58:00.000'
END_DT   = '2026-07-31 16:10:00.000'


def get_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASSWORD,
        database=CH_DATABASE,
        send_receive_timeout=300,
    )


def check_data_in_range(client):
    """Проверяет, какие данные есть в диапазоне 15:59 - 16:05"""
    
    print(f"\n{'='*80}")
    print(f"ПРОВЕРКА ДАННЫХ В ДИАПАЗОНЕ: {START_DT} - {END_DT}")
    print(f"{'='*80}\n")
    
    # 1. Общее количество записей в диапазоне
    q1 = f"""
        SELECT count() AS total_records
        FROM {TABLE}
        WHERE {COL_TS} >= '{START_DT}' 
          AND {COL_TS} < '{END_DT}'
    """
    result = client.query(q1)
    total = result.result_rows[0][0]
    print(f"1. ВСЕГО ЗАПИСЕЙ в диапазоне: {total:,}\n")
    
    if total == 0:
        print("   >>> В ЭТОМ ДИАПАЗОНЕ НЕТ ДАННЫХ! <<<\n")
        return
    
    # 2. Распределение по каналам
    q2 = f"""
        SELECT {COL_PROP} AS prop,
               count()    AS cnt,
               min({COL_TS}) AS min_ts,
               max({COL_TS}) AS max_ts
        FROM {TABLE}
        WHERE {COL_TS} >= '{START_DT}' 
          AND {COL_TS} < '{END_DT}'
        GROUP BY {COL_PROP}
        ORDER BY cnt DESC
    """
    df_props = client.query_df(q2)
    print("2. РАСПРЕДЕЛЕНИЕ ПО КАНАЛАМ:")
    print(df_props.to_string(index=False))
    print()
    
    # 3. Группировка по data_record_sk (сессиям)
    q3 = f"""
        SELECT {COL_REC} AS rec,
               {COL_PROP} AS prop,
               min({COL_TS}) AS first_ts,
               max({COL_TS}) AS last_ts,
               count() AS cnt
        FROM {TABLE}
        WHERE {COL_TS} >= '{START_DT}' 
          AND {COL_TS} < '{END_DT}'
        GROUP BY {COL_REC}, {COL_PROP}
        ORDER BY first_ts ASC
    """
    df_sessions = client.query_df(q3)
    
    print(f"3. НАЙДЕНО УНИКАЛЬНЫХ data_record_sk: {df_sessions['rec'].nunique()}")
    print(f"   НАЙДЕНО УНИКАЛЬНЫХ first_ts: {df_sessions['first_ts'].nunique()}\n")
    
    # 4. Группируем по first_ts и считаем количество каналов в каждой сессии
    print("4. АНАЛИЗ СЕССИЙ (группировка по min(timestamp)):")
    print("-" * 80)
    
    sessions_by_ts = {}
    for _, row in df_sessions.iterrows():
        ts = row['first_ts']
        if ts not in sessions_by_ts:
            sessions_by_ts[ts] = {'props': set(), 'recs': set(), 'total_records': 0}
        sessions_by_ts[ts]['props'].add(str(row['prop']))
        sessions_by_ts[ts]['recs'].add(str(row['rec']))
        sessions_by_ts[ts]['total_records'] += row['cnt']
    
    wanted_props = set(CHANNELS.values())
    
    for ts in sorted(sessions_by_ts.keys()):
        info = sessions_by_ts[ts]
        n_channels = len(info['props'])
        is_complete = info['props'] == wanted_props
        
        status = "✓ ПОЛНАЯ (5 каналов)" if is_complete else f"✗ НЕПОЛНАЯ ({n_channels}/5 каналов)"
        
        print(f"\n   Время: {ts}")
        print(f"   Статус: {status}")
        print(f"   Записей: {info['total_records']:,}")
        print(f"   Каналы: {n_channels}")
        
        # Показываем, каких каналов не хватает
        if not is_complete:
            missing = wanted_props - info['props']
            if missing:
                print(f"   НЕ ХВАТАЕТ:")
                for prop_uuid in missing:
                    # Находим имя канала
                    channel_name = next((k for k, v in CHANNELS.items() if v == prop_uuid), 'unknown')
                    print(f"      - {channel_name} ({prop_uuid})")
        
        if n_channels > 0:
            print(f"   Присутствуют:")
            for prop_uuid in sorted(info['props']):
                channel_name = next((k for k, v in CHANNELS.items() if v == prop_uuid), 'unknown')
                print(f"      ✓ {channel_name}")
    
    print(f"\n{'='*80}")
    print("ВЫВОД:")
    print(f"{'='*80}")
    
    complete_sessions = [ts for ts, info in sessions_by_ts.items() 
                         if info['props'] == wanted_props]
    
    if complete_sessions:
        print(f"\n✓ Найдено {len(complete_sessions)} полных сессий в диапазоне:")
        for ts in sorted(complete_sessions):
            print(f"   - {ts}")
    else:
        print(f"\n✗ Полных сессий (со всеми 5 каналами) в диапазоне НЕ НАЙДЕНО!")
        print(f"\nВозможные причины:")
        print(f"   1. Данные записываются не непрерывно (есть пропуски)")
        print(f"   2. Некоторые каналы не записались в этот период")
        print(f"   3. Сессии начинаются позже (первая полная сессия в 16:05)")


def check_earliest_data(client):
    """Проверяет самые ранние данные после TARGET_DT"""
    
    print(f"\n{'='*80}")
    print(f"ПОИСК ПЕРВЫХ ПОЛНЫХ СЕССИЙ ПОСЛЕ {START_DT}")
    print(f"{'='*80}\n")
    
    q = f"""
        SELECT first_ts, count(DISTINCT prop) AS n_channels
        FROM (
            SELECT {COL_REC} AS rec,
                   {COL_PROP} AS prop,
                   min({COL_TS}) AS first_ts
            FROM {TABLE}
            WHERE {COL_TS} >= '{START_DT}'
            GROUP BY {COL_REC}, {COL_PROP}
        )
        GROUP BY first_ts
        ORDER BY first_ts ASC
        LIMIT 20
    """
    
    df = client.query_df(q)
    
    if df.empty:
        print("Нет данных после указанной даты!")
        return
    
    print("Первые 20 временных меток сессий:")
    print("-" * 80)
    
    wanted_n = 5
    for _, row in df.iterrows():
        ts = row['first_ts']
        n = row['n_channels']
        status = "✓ ПОЛНАЯ" if n == wanted_n else f"({n}/5)"
        print(f"   {ts}  {status}")


def main():
    client = get_client()
    print(f"Подключено к {CH_HOST}:{CH_PORT}, БД {CH_DATABASE}")
    
    check_data_in_range(client)
    check_earliest_data(client)
    
    print(f"\n{'='*80}")
    print("ДИАГНОСТИКА ЗАВЕРШЕНА")
    print(f"{'='*80}\n")


if __name__ == '__main__':
    main()