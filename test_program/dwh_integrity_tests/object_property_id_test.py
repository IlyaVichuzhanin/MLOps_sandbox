import psycopg2
import sys

# ==============================================================================
# Конфигурация подключения к DWH (Greenplum/PostgreSQL)
# ==============================================================================
DWH_HOST = 'localhost'
DWH_PORT = 7000
DWH_DB = 'dwh'
DWH_USER = 'gpadmin'
DWH_PASSWORD = ''

def check_all_links_coverage():
    """
    Динамически находит ВСЕ Link-таблицы (l_*) во всей базе данных, 
    которые ссылаются на public.h_object_properties, и проверяет покрытие.
    """
    conn = None
    try:
        print(f"Подключение к {DWH_HOST}:{DWH_PORT}/{DWH_DB}...")
        conn = psycopg2.connect(
            host=DWH_HOST, port=DWH_PORT, dbname=DWH_DB,
            user=DWH_USER, password=DWH_PASSWORD
        )
        cur = conn.cursor()
        
        # 1. Динамический поиск всех Link-таблиц через системные каталоги PostgreSQL
        # Это гарантирует, что мы проверим ВСЕ линки во всей схеме (General, IO, Rules и т.д.)
        print("Поиск всех Link-таблиц (l_*) с FK на h_object_properties через pg_catalog...")
        
        fk_query = """
            SELECT
                tc.relname AS table_name,
                att.attname AS column_name
            FROM pg_constraint con
            JOIN pg_class tc ON tc.oid = con.conrelid
            JOIN pg_namespace nsp ON nsp.oid = tc.relnamespace
            JOIN pg_attribute att ON att.attnum = ANY(con.conkey) AND att.attrelid = con.conrelid
            JOIN pg_class fc ON fc.oid = con.confrelid
            JOIN pg_namespace fnsp ON fnsp.oid = fc.relnamespace
            WHERE con.contype = 'f'
              AND nsp.nspname = 'public'
              AND fnsp.nspname = 'public'
              AND fc.relname = 'h_object_properties'
              AND tc.relname LIKE 'l\_%';
        """
        cur.execute(fk_query)
        link_tables = cur.fetchall()
        
        if not link_tables:
            print("❌ [WARNING] Не найдено ни одной Link-таблицы с FK на h_object_properties.")
            return False
            
        print(f"✅ Найдено {len(link_tables)} Link-таблиц во всей схеме DWH:")
        for tbl, col in link_tables:
            print(f"   - public.{tbl} (колонка: {col})")
            
        # 2. Формируем UNION всех найденных таблиц
        # Используем ALIAS, чтобы привести разные имена колонок (например, h_timer_property_id_sk) 
        # к единому знаменателю для корректной работы EXCEPT
        union_parts = [
            f"SELECT {col} AS h_object_property_sk FROM public.{table}" 
            for table, col in link_tables
        ]
        union_sql = " UNION ".join(union_parts)
        
        # 3. Итоговый запрос с EXCEPT
        test_query = f"""
            SELECT h_object_property_sk 
            FROM public.h_object_properties
            EXCEPT
            (
                {union_sql}
            );
        """
        
        print("\nВыполнение проверки целостности (Hub vs ALL Links)...")
        cur.execute(test_query)
        orphans = cur.fetchall()
        
        if not orphans:
            print("\n✅ [SUCCESS] Тест пройден.")
            print("   Все записи в public.h_object_properties имеют ссылки хотя бы в одной Link-таблице.")
            return True
        else:
            print(f"\n❌ [FAILED] Обнаружено {len(orphans)} записей в Hub без ссылок в Link-таблицах.")
            print("   Примеры 'сиротских' h_object_property_sk:")
            for row in orphans[:10]:
                print(f"   - {row[0]}")
            if len(orphans) > 10:
                print(f"   ... и еще {len(orphans) - 10} записей.")
            return False

    except psycopg2.Error as e:
        print(f"❌ Ошибка базы данных: {e}")
        return False
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    success = check_all_links_coverage()
    sys.exit(0 if success else 1)