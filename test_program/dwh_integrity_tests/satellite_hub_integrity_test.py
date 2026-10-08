import psycopg2

# ==============================================================================
# Конфигурация подключения к DWH (Greenplum/PostgreSQL)
# ==============================================================================
DWH_HOST = 'localhost'
DWH_PORT = 7000
DWH_DB = 'dwh'
DWH_USER = 'gpadmin'
DWH_PASSWORD = ''

# ==============================================================================
# Конфиг исключений (Ignored Foreign Keys)
# Формат: (sat_table, sat_column, hub_table, hub_column)
# Добавьте сюда известные проблемы, если они появятся, чтобы тест не падал.
# ==============================================================================
IGNORED_FKS = set()

def check_sat_to_hub_integrity():
    """
    Динамически находит все Foreign Keys из Satellite-таблиц (s_*) в Hub-таблицы (h_*)
    и проверяет, что все значения внешних ключей в Сателлитах существуют в соответствующих Хабах.
    """
    conn = None
    ignored_count = 0
    ok_count = 0
    failures = []
    
    print("=" * 80)
    print("🛰️  ОТЧЕТ: Проверка ссылочной целостности (Satellite -> Hub)")
    print("=" * 80)
    print(f"🌐 Подключение: {DWH_USER}@{DWH_HOST}:{DWH_PORT}/{DWH_DB}\n")
    
    try:
        conn = psycopg2.connect(
            host=DWH_HOST, port=DWH_PORT, dbname=DWH_DB,
            user=DWH_USER, password=DWH_PASSWORD
        )
        cur = conn.cursor()
        
        # 1. Динамический поиск всех FK из Satellite (s_*) в Hub (h_*) через pg_catalog
        print("⏳ Поиск всех Foreign Keys (s_* -> h_*) в системных каталогах...")
        
        fk_query = """
            SELECT
                tc.relname AS sat_table,
                (SELECT attname FROM pg_attribute WHERE attrelid = con.conrelid AND attnum = con.conkey[1]) AS sat_column,
                fc.relname AS hub_table,
                (SELECT attname FROM pg_attribute WHERE attrelid = con.confrelid AND attnum = con.confkey[1]) AS hub_column
            FROM pg_constraint con
            JOIN pg_class tc ON tc.oid = con.conrelid
            JOIN pg_namespace tnsp ON tnsp.oid = tc.relnamespace
            JOIN pg_class fc ON fc.oid = con.confrelid
            JOIN pg_namespace fnsp ON fnsp.oid = fc.relnamespace
            WHERE con.contype = 'f'
              AND tnsp.nspname = 'public'
              AND fnsp.nspname = 'public'
              AND tc.relname LIKE 's!_%' ESCAPE '!'
              AND fc.relname LIKE 'h!_%' ESCAPE '!';
        """
        cur.execute(fk_query)
        fks = cur.fetchall()
        
        if not fks:
            print("⚠️ [WARNING] Не найдено ни одного FK из Satellite-таблиц в Hub-таблицы.")
            return
            
        print(f"✅ Найдено {len(fks)} внешних ключей (Satellite -> Hub) для анализа.\n")
        print("-" * 80)
        
        # 2. Проверка каждого FK
        for sat_table, sat_column, hub_table, hub_column in fks:
            fk_tuple = (sat_table, sat_column, hub_table, hub_column)
            
            # Проверка на наличие в списке исключений
            if fk_tuple in IGNORED_FKS:
                print(f"⏭️  [IGNORED] {sat_table}.{sat_column} -> {hub_table}.{hub_column}")
                ignored_count += 1
                continue

            # Используем NOT EXISTS для быстрой проверки с использованием индексов (PK хаба)
            check_query = f"""
                SELECT count(1)
                FROM public.{sat_table} s
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM public.{hub_table} h
                    WHERE h.{hub_column} = s.{sat_column}
                );
            """
            
            cur.execute(check_query)
            orphan_count = cur.fetchone()[0]
            
            if orphan_count > 0:
                failures.append({
                    'sat_table': sat_table,
                    'sat_column': sat_column,
                    'hub_table': hub_table,
                    'hub_column': hub_column,
                    'orphans': orphan_count
                })
                print(f"❌ [FAILED] {sat_table}.{sat_column} -> {hub_table}.{hub_column} | Сирот: {orphan_count}")
            else:
                print(f"✅ [OK]     {sat_table}.{sat_column} -> {hub_table}.{hub_column}")
                ok_count += 1
                
        # 3. Итоговый отчет
        print("\n" + "=" * 80)
        print("📊 ИТОГОВАЯ СТАТИСТИКА")
        print("=" * 80)
        print(f"🛰️  Всего FK проверено : {len(fks)}")
        print(f"✅ Успешно (OK)       : {ok_count}")
        print(f"⏭️  Исключено (Ignore): {ignored_count}")
        print(f"❌ Ошибок (Failed)    : {len(failures)}")
        print("-" * 80)
        
        if not failures:
            print("🎉 [SUCCESS] Тест пройден!")
            print("   Все записи в Satellite-таблицах корректно ссылаются на существующие Hub-записи.")
        else:
            print(f"⚠️  [FAILED] Обнаружено {len(failures)} нарушенных ссылочных целостностей.")
            print("\n   Детали ошибок:")
            for f in failures:
                print(f"   - {f['orphans']} записей в {f['sat_table']}.{f['sat_column']} не найдены в {f['hub_table']}.{f['hub_column']}")
        print("=" * 80)

    except psycopg2.Error as e:
        print(f"\n❌ Ошибка базы данных: {e}")
    finally:
        if conn:
            conn.close()
            print("\n🔌 Соединение с БД закрыто.")

if __name__ == "__main__":
    check_sat_to_hub_integrity()
    