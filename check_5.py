# -*- coding: utf-8 -*-
"""
Подробный анализ активности 15:20–16:20:
 1) по минутам: сколько строк и сколько уникальных каналов;
 2) топ-каналы в ключевых окнах;
 3) есть ли наши 5 каналов в каждом окне.
"""

import clickhouse_connect

CH_HOST     = '135.106.148.141'
CH_PORT     = 8123
CH_USER     = 'default'
CH_PASSWORD = 'YourSuperStrongP@ssw0rd!2026'
CH_DATABASE = 'default'

TABLE    = 'hist_creyt_timeseries_data'
COL_REC  = 'data_record_sk'
COL_PROP = 'h_object_property_sk'
COL_TS   = 'timestamp'

# Наши каналы
WANTED = {
    'x1':    '040dcfb3-edaa-4430-d826-77a8dec1e635',
    'y1':    'f256d1f9-6841-1e5b-9ec2-bd6e5fba67d7',
    'x2':    '65301f9f-3de1-16a0-faf0-3c026eefe952',
    'y2':    'c82e154c-b53d-792b-5829-37d777c16000',
    'phase': '7379afbe-c0c0-9ab9-fab8-1b449266e124',
}
WANTED_SET = set(WANTED.values())


def main():
    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER,
        password=CH_PASSWORD, database=CH_DATABASE,
        send_receive_timeout=300,
    )
    print(f'Подключено к {CH_HOST}:{CH_PORT}, БД {CH_DATABASE}\n')

    # 1) Активность по минутам 15:20–16:20
    print('1) АКТИВНОСТЬ ПО МИНУТАМ 15:20 – 16:20:')
    rows = client.query(f"""
        SELECT toString(toStartOfInterval({COL_TS}, INTERVAL 1 minute)) AS bucket,
               count() AS cnt,
               uniqExact({COL_PROP}) AS n_props
        FROM {TABLE}
        WHERE {COL_TS} >= '2026-07-31 15:20:00'
          AND {COL_TS} <  '2026-07-31 16:20:00'
        GROUP BY bucket
        ORDER BY bucket
    """).result_rows
    for r in rows:
        print(f'   {r[0]}  строк={r[1]:>9}  уник.каналов={r[2]}')

    # 2) Топ-каналы в ключевых окнах
    windows = [
        ('15:25–15:35 (перед обрывом)', '15:25:00', '15:35:00'),
        ('15:35–15:45 (дырка)',         '15:35:00', '15:45:00'),
        ('15:45–15:55 (дырка)',         '15:45:00', '15:55:00'),
        ('15:55–16:05 (перед возобн.)', '15:55:00', '16:05:00'),
        ('16:05–16:15 (после возобн.)', '16:05:00', '16:15:00'),
    ]

    for title, t_start, t_end in windows:
        print(f'\n2) ОКНО {title} ({t_start}–{t_end}):')
        rows = client.query(f"""
            SELECT toString({COL_PROP}) AS prop,
                   count() AS cnt
            FROM {TABLE}
            WHERE {COL_TS} >= '2026-07-31 {t_start}'
              AND {COL_TS} <  '2026-07-31 {t_end}'
            GROUP BY prop
            ORDER BY cnt DESC
            LIMIT 15
        """).result_rows
        if not rows:
            print('   ПУСТО')
        for r in rows:
            prop = r[0]
            name = next((k for k, v in WANTED.items() if v == prop), None)
            mark = f' <- наш ({name})' if name else ''
            print(f'   {prop}  строк={r[1]:>9}{mark}')

    # 3) Проверка: есть ли наши 5 каналов в каждом окне
    print('\n3) НАШИ 5 КАНАЛОВ В КАЖДОМ ОКНЕ:')
    for title, t_start, t_end in windows:
        rows = client.query(f"""
            SELECT toString({COL_PROP}) AS prop, count() AS cnt
            FROM {TABLE}
            WHERE {COL_TS} >= '2026-07-31 {t_start}'
              AND {COL_TS} <  '2026-07-31 {t_end}'
              AND {COL_PROP} IN (
                  '040dcfb3-edaa-4430-d826-77a8dec1e635',
                  'f256d1f9-6841-1e5b-9ec2-bd6e5fba67d7',
                  '65301f9f-3de1-16a0-faf0-3c026eefe952',
                  'c82e154c-b53d-792b-5829-37d777c16000',
                  '7379afbe-c0c0-9ab9-fab8-1b449266e124'
              )
            GROUP BY prop
        """).result_rows
        found = {r[0]: r[1] for r in rows}
        present = [name for name, uuid in WANTED.items() if uuid in found]
        print(f'   {title}: найдено {len(present)}/5 — {present if present else "НИ ОДНОГО"}')

    print('\nГотово.')


if __name__ == '__main__':
    main()