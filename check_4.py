# -*- coding: utf-8 -*-
"""
Финальная разведка:
 1) есть ли ХОТЬ КАКИЕ-ТО данные в дырке 15:47:18–16:05:11 (любые UUID);
 2) общая картина активности таблицы за 31.07.2026 по 10-минутным окнам.
"""

import clickhouse_connect

CH_HOST     = '135.106.148.141'
CH_PORT     = 8123
CH_USER     = 'default'
CH_PASSWORD = 'YourSuperStrongP@ssw0rd!2026'
CH_DATABASE = 'default'

TABLE    = 'hist_creyt_timeseries_data'
COL_PROP = 'h_object_property_sk'
COL_TS   = 'timestamp'


def main():
    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER,
        password=CH_PASSWORD, database=CH_DATABASE,
        send_receive_timeout=300,
    )
    print(f'Подключено к {CH_HOST}:{CH_PORT}, БД {CH_DATABASE}\n')

    print('1) ЛЮБЫЕ каналы в окне 15:47:18 – 16:05:11:')
    rows = client.query(f"""
        SELECT toString({COL_PROP}) AS prop,
               count()                 AS cnt,
               toString(min({COL_TS})) AS mn,
               toString(max({COL_TS})) AS mx
        FROM {TABLE}
        WHERE {COL_TS} >= '2026-07-31 15:47:18'
          AND {COL_TS} <  '2026-07-31 16:05:11'
        GROUP BY prop
        ORDER BY cnt DESC
        LIMIT 30
    """).result_rows
    if not rows:
        print('   ПУСТО. В таблице НЕТ никаких данных в этом окне —\n'
              '   значит, просмотрщик читает 15:48:11 откуда-то ещё\n'
              '   (другая таблица / другая БД / другой сервер).')
    else:
        print('   Данные ЕСТЬ, но под другими UUID (ротация sk):')
        for r in rows:
            print(f'   {r[0]}  строк={r[1]:>9}  [{r[2]} .. {r[3]}]')

    print('\n2) Активность таблицы за 31.07.2026 (10-минутные окна):')
    rows = client.query(f"""
        SELECT toString(toStartOfInterval({COL_TS}, INTERVAL 10 minute)) AS bucket,
               count() AS cnt
        FROM {TABLE}
        WHERE {COL_TS} >= '2026-07-31 00:00:00'
          AND {COL_TS} <  '2026-08-01 00:00:00'
        GROUP BY bucket
        ORDER BY bucket
    """).result_rows
    for r in rows:
        print(f'   {r[0]}  строк: {r[1]:>9}')

    print('\nГотово.')


if __name__ == '__main__':
    main()