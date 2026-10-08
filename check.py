# -*- coding: utf-8 -*-
"""
Проверка: есть ли в hist_creyt_timeseries_data данные по 5 каналам
после 2026-07-31 15:47:18.929, и что пишется в таблице дальше.
ВАЖНО: дата только в ISO-формате (DateTime64 не принимает '31.07.2026 ...').
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

TARGET_DT = '2026-07-31 15:47:18.929'   # ISO-формат!
LAST_SEEN = '2026-07-31 16:14:40'      # до сюда наши UUID точно пишут

CHANNELS = {
    'x1':    '040dcfb3-edaa-4430-d826-77a8dec1e635',
    'y1':    'f256d1f9-6841-1e5b-9ec2-bd6e5fba67d7',
    'x2':    '65301f9f-3de1-16a0-faf0-3c026eefe952',
    'y2':    'c82e154c-b53d-792b-5829-37d777c16000',
    'phase': '7379afbe-c0c0-9ab9-fab8-1b449266e124',
}


def section(title):
    print('\n' + '-' * 90)
    print(title)
    print('-' * 90)


def main():
    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER,
        password=CH_PASSWORD, database=CH_DATABASE,
        send_receive_timeout=300,
    )
    print(f'Подключено к {CH_HOST}:{CH_PORT}, БД {CH_DATABASE}')
    srv = client.query('SELECT toString(now()), timezone()').result_rows[0]
    print(f'Сервер: now() = {srv[0]}, timezone сервера = {srv[1]}')

    uuids = ', '.join(f"'{u}'" for u in CHANNELS.values())
    inv = {v: k for k, v in CHANNELS.items()}

    # ---- 0. Максимальная дата в таблице ВООБЩЕ ----
    section('0) МАКСИМАЛЬНАЯ timestamp В ТАБЛИЦЕ ВООБЩЕ')
    try:
        r = client.query(f'SELECT toString(max({COL_TS})) FROM {TABLE}').result_rows[0]
        print(f'   max(timestamp) = {r[0]}')
    except Exception as e:
        print('   Ошибка:', e)

    # ---- 1. Сводка по каждому из 5 каналов ----
    section('1) ПО КАЖДОМУ КАНАЛУ: всего / после TARGET / диапазон "после"')
    try:
        q1 = f"""
            SELECT toString({COL_PROP}) AS prop,
                   count()                             AS total,
                   countIf({COL_TS} >= '{TARGET_DT}')  AS after_cnt,
                   toString(minIf({COL_TS}, {COL_TS} >= '{TARGET_DT}')) AS after_min,
                   toString(maxIf({COL_TS}, {COL_TS} >= '{TARGET_DT}')) AS after_max
            FROM {TABLE}
            WHERE {COL_PROP} IN ({uuids})
            GROUP BY {COL_PROP}
        """
        for r in client.query(q1).result_rows:
            print(f'   {inv.get(r[0], "?"):>5}  всего={r[1]:>9}  после={r[2]:>9}  '
                  f'[{r[3]} .. {r[4]}]')
    except Exception as e:
        print('   Ошибка:', e)

    # ---- 2. Полных сессий после TARGET ----
    section('2) ПОЛНЫХ СЕССИЙ (все 5 каналов) ПОСЛЕ TARGET')
    try:
        q2 = f"""
            SELECT count()
            FROM (
                SELECT {COL_REC}
                FROM {TABLE}
                WHERE {COL_PROP} IN ({uuids}) AND {COL_TS} >= '{TARGET_DT}'
                GROUP BY {COL_REC}
                HAVING uniqExact({COL_PROP}) = 5
            )
        """
        print('  ', client.query(q2).result_rows[0][0])
    except Exception as e:
        print('   Ошибка:', e)

    # ---- 3. Крайние записи после TARGET по этим каналам ----
    section('3) КРАЙНИЕ ЗАПИСИ ПОСЛЕ TARGET (эти 5 каналов)')
    try:
        q3 = f"""
            SELECT toString({COL_TS}) AS ts, toString({COL_PROP}) AS prop
            FROM {TABLE}
            WHERE {COL_PROP} IN ({uuids}) AND {COL_TS} >= '{TARGET_DT}'
            ORDER BY {COL_TS} ASC LIMIT 3
        """
        for r in client.query(q3).result_rows:
            print(f'   первая: {r[0]}  {inv.get(r[1], r[1])}')
        q4 = f"""
            SELECT toString({COL_TS}) AS ts, toString({COL_PROP}) AS prop
            FROM {TABLE}
            WHERE {COL_PROP} IN ({uuids}) AND {COL_TS} >= '{TARGET_DT}'
            ORDER BY {COL_TS} DESC LIMIT 3
        """
        for r in client.query(q4).result_rows:
            print(f'   посл-я: {r[0]}  {inv.get(r[1], r[1])}')
    except Exception as e:
        print('   Ошибка:', e)

    # ---- 4. Кто пишется после LAST_SEEN (любые каналы) ----
    section(f'4) КАНАЛЫ, КОТОРЫЕ ПИШУТСЯ ПОСЛЕ {LAST_SEEN} (любые, топ-15)')
    try:
        q5 = f"""
            SELECT toString({COL_PROP}) AS prop,
                   count()                 AS cnt,
                   toString(min({COL_TS})) AS mn,
                   toString(max({COL_TS})) AS mx
            FROM {TABLE}
            WHERE {COL_TS} > '{LAST_SEEN}'
            GROUP BY {COL_PROP}
            ORDER BY cnt DESC
            LIMIT 15
        """
        rows = client.query(q5).result_rows
        if not rows:
            print('   НИЧЕГО не пишется позже — в таблице нет данных после этого времени.')
        for r in rows:
            mark = inv.get(r[0])
            tag = f'  <- наш канал ({mark})' if mark else ''
            print(f'   {r[0]}  строк={r[1]:>9}  [{r[2]} .. {r[3]}]{tag}')
    except Exception as e:
        print('   Ошибка:', e)

    print('\nГотово.')


if __name__ == '__main__':
    main()