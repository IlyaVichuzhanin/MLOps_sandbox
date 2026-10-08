import sqlite3

def test_property_id_intersection():
    conn = sqlite3.connect('D:\\ml-sandbox\\test_data\\Kriogen\\Kriogen.db')
    cursor = conn.cursor()
    
    # Проверяем DiagnosticAlarms_v2
    cursor.execute("""
        SELECT COUNT(*) 
        FROM DiagnosticAlarms_v2 da
        WHERE EXISTS (
            SELECT 1 FROM ObjectProperties op 
            WHERE op.PropertyID = da.PropertyID
        )
    """)
    alarms_count = cursor.fetchone()[0]
    
    # Проверяем DiagnosticAlarmHistory_v2
    cursor.execute("""
        SELECT COUNT(*) 
        FROM DiagnosticAlarmHistory_v2 dah
        WHERE EXISTS (
            SELECT 1 FROM ObjectProperties op 
            WHERE op.PropertyID = dah.PropertyID
        )
    """)
    history_count = cursor.fetchone()[0]
    
    print(f"DiagnosticAlarms_v2: {alarms_count} пересекающихся PropertyId")
    print(f"DiagnosticAlarmHistory_v2: {history_count} пересекающихся PropertyId")
    
    conn.close()
    
    return alarms_count, history_count

if __name__ == '__main__':
    test_property_id_intersection()