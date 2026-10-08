# -*- coding: utf-8 -*-
"""
A) Сетка 10-секундных окон 15:47:18–16:14:18 (UTC) по каналу x1: где данные, где ПУСТО.
B) Проверка гипотезы UTC+3: ищем сессию в 12:48:11.562 UTC (= 15:48:11.562 местного).
"""

import clickhouse_connect
import pandas as pd

CH_HOST     = '135.106.148.141'
CH_PORT     = 8123
CH_USER     = 'default'
CH_PASSWORD = 'YourSuperStrongP@ssw0rd!2026'
CH_DATABASE = 'default'

TABLE    = 'hist_creyt_timeseries_data'
COL_REC  = 'data_record_sk'
COL_PROP = 'h_object_property_sk'
COL_TS   = 'timestamp'

TEST_PROP = '040dcfb3-edaa-4430-d826-77a8dec1e635'   # x1


def main():
    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER,
        password=CH_PASSWORD, database=CH_DATABASE,
        send_receive_timeout=300,
    )
    print(f'Подключено к {CH_HOST}:{CH_PORT}, БД {CH_DATABASE} (TZ сервера UTC)\n')

    # ---------- A) сетка окон 15:47:18 – 16:14:18 ----------
    print('A) ОКНА ПО 10 СЕК В ДИАПАЗОНЕ 15:47:18 – 16:14:18 (UTC):')
    rows = client.query(f"""
        SELECT toString(toStartOfInterval({COL_TS}, INTERVAL 10 second)) AS bucket,
               count() AS cnt
        FROM {TABLE}
        WHERE {COL_PROP} = '{TEST_PROP}'
          AND {COL_TS} >= '2026-07-31 15:47:18'
          AND {COL_TS} <= '2026-07-31 16:14:18'
        GROUP BY bucket ORDER BY bucket
    """).result_rows
    have = {r[0]: r[1] for r in rows}

    grid = pd.date_range('2026-07-31 15:47:18', '2026-07-31 16:14:18', freq='10s')
    empty_start = None
    for ts in grid:
        key = str(ts.floor('10s'))
        cnt = have.get(key, 0)
        if cnt == 0:
            if empty_start is None:
                empty_start = ts
        else:
            if empty_start is not None:
                print(f'   {empty_start} – {ts - pd.Timedelta(seconds=10)}  ->  ПУСТО')
                empty_start = None
            print(f'   {key}  строк: {cnt}')
    if empty_start is not None:
        print(f'   {empty_start} – {grid[-1]}  ->  ПУСТО')

    # ---------- B) гипотеза UTC+3 ----------
    print('\nB) ПРОВЕРКА ГИПОТЕЗЫ UTC+3:')
    print('   Ищем сессии по тому же каналу в 12:47:18–13:20:00 UTC '
          '(= 15:47:18–16:20:00 местного):')
    rows = client.query(f"""
        SELECT toString(min({COL_TS})) AS t, count() AS rows
        FROM {TABLE}
        WHERE {COL_PROP} = '{TEST_PROP}'
          AND {COL_TS} >= '2026-07-31 12:47:18'
          AND {COL_TS} <  '2026-07-31 13:20:00'
        GROUP BY {COL_REC}
        ORDER BY t
        LIMIT 40
    """).result_rows
    if not rows:
        print('   пусто — гипотеза не подтвердилась')
    for r in rows:
        mark = '  <<< ВОТ ОНА, 15:48:11.562 местного!' if r[0].startswith('2026-07-31 12:48:11') else ''
        print(f'   старт сессии (UTC): {r[0]}  строк: {r[1]}{mark}')

    print('\nГотово.')


if __name__ == '__main__':
    main()