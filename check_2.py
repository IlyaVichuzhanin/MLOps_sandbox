# -*- coding: utf-8 -*-
"""
Проверка непрерывности данных по одному UUID на всём промежутке.
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

# Берём один канал для проверки
TEST_PROP = '040dcfb3-edaa-4430-d826-77a8dec1e635'   # x1


def section(title):
    print('\n' + '=' * 90)
    print(title)
    print('=' * 90)


def main():
    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER,
        password=CH_PASSWORD, database=CH_DATABASE,
        send_receive_timeout=300,
    )
    print(f'Подключено к {CH_HOST}:{CH_PORT}, БД {CH_DATABASE}')

    # 1. Общее количество записей по этому UUID
    section(f'1) Всего записей по {TEST_PROP}')
    r = client.query(f"""
        SELECT count(), min({COL_TS}), max({COL_TS})
        FROM {TABLE}
        WHERE {COL_PROP} = '{TEST_PROP}'
    """).result_rows[0]
    print(f'   всего={r[0]}  min={r[1]}  max={r[2]}')

    # 2. Проверка непрерывности: записи в 10-секундных окнах в промежутке 15:47-16:14
    section(f'2) Непрерывность в окне 15:47:18 - 16:14:40 (по 10 секунд)')
    rows = client.query(f"""
        SELECT
            toStartOfInterval({COL_TS}, INTERVAL 10 second) AS bucket,
            count() AS cnt,
            uniqExact({COL_REC}) AS n_recs
        FROM {TABLE}
        WHERE {COL_PROP} = '{TEST_PROP}'
          AND {COL_TS} >= '2026-07-31 15:47:18'
          AND {COL_TS} <= '2026-07-31 16:14:40'
        GROUP BY bucket
        ORDER BY bucket
    """).result_rows

    print(f'{"окно":<26} {"строк":>8} {"уник. rec":>10}')
    prev = None
    gaps = 0
    for r in rows:
        bucket, cnt, n_recs = r
        # проверяем, есть ли пропуск между окнами
        if prev and (bucket - prev).total_seconds() > 11:
            print(f'   ... пропуск ...')
            gaps += 1
        print(f'{str(bucket):<26} {cnt:>8} {n_recs:>10}')
        prev = bucket
    print(f'\nВсего окон: {len(rows)}, пропусков > 10 сек: {gaps}')

    # 3. Сколько ВСЕГО разных каналов (UUID) в каждой "сверх-сессии" по data_record_sk
    # Берём конкретный rec из окна 15:48
    section('3) Сколько каналов в конкретном data_record_sk из окна 15:48')
    r = client.query(f"""
        SELECT {COL_REC}, min({COL_TS}) AS t
        FROM {TABLE}
        WHERE {COL_PROP} = '{TEST_PROP}'
          AND {COL_TS} >= '2026-07-31 15:48:00'
          AND {COL_TS} <= '2026-07-31 15:49:00'
        GROUP BY {COL_REC}
        ORDER BY t
        LIMIT 1
    """).result_rows
    if r:
        rec, t = r[0]
        print(f'   rec = {rec}  (первая запись {t})')
        
        all_props = client.query(f"""
            SELECT toString({COL_PROP}) AS prop, count() AS cnt
            FROM {TABLE}
            WHERE {COL_REC} = '{rec}'
            GROUP BY prop
            ORDER BY cnt DESC
        """).result_rows
        print(f'   Всего разных каналов в этом rec: {len(all_props)}')
        print(f'   Топ-10:')
        for p in all_props[:10]:
            print(f'      {p[0]}  (строк: {p[1]})')
        
        # есть ли среди них наши 5?
        wanted = {
            'x1': '040dcfb3-edaa-4430-d826-77a8dec1e635',
            'y1': 'f256d1f9-6841-1e5b-9ec2-bd6e5fba67d7',
            'x2': '65301f9f-3de1-16a0-faf0-3c026eefe952',
            'y2': 'c82e154c-b53d-792b-5829-37d777c16000',
            'phase': '7379afbe-c0c0-9ab9-fab8-1b449266e124',
        }
        found = [name for name, u in wanted.items() if u in [p[0] for p in all_props]]
        print(f'   Из наших 5 каналов в этом rec есть: {found}')

    # 4. Сколько rec'ов за минуту 15:48:00 - 15:49:00
    section('4) Сколько data_record_sk за минуту 15:48-15:49')
    r = client.query(f"""
        SELECT uniqExact({COL_REC}), count()
        FROM {TABLE}
        WHERE {COL_PROP} = '{TEST_PROP}'
          AND {COL_TS} >= '2026-07-31 15:48:00'
          AND {COL_TS} <= '2026-07-31 15:49:00'
    """).result_rows[0]
    print(f'   уник. rec: {r[0]}, всего строк: {r[1]}')

    # 5. Сравним с "хорошей" минутой 16:06
    section('5) Сколько data_record_sk за минуту 16:06-16:07 (где работало)')
    r = client.query(f"""
        SELECT uniqExact({COL_REC}), count()
        FROM {TABLE}
        WHERE {COL_PROP} = '{TEST_PROP}'
          AND {COL_TS} >= '2026-07-31 16:06:00'
          AND {COL_TS} <= '2026-07-31 16:07:00'
    """).result_rows[0]
    print(f'   уник. rec: {r[0]}, всего строк: {r[1]}')

    print('\nГотово.')


if __name__ == '__main__':
    main()