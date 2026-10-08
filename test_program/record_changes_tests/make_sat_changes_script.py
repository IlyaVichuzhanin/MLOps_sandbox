import sqlite3
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any


class DataAuditManager:
    """Управление аудитом изменений данных для Data Vault 2.0"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.changes_log = []
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Маппинг имен ID колонок для каждой таблицы
        self.id_columns = {
            'Bearings': 'BearingID',
            'MeasureGroups': 'ID',
            'MeasureUnits': 'ID',
            'ObjectPropertyDescriptorNodes': 'ID',
            'ObjectPropertyDescriptors': 'PropertyDescriptorID',
            'Objects': 'ID',
            'ModelTemplates': 'ID',
            'ObjectTypes': 'ID'
        }
    
    def connect(self) -> sqlite3.Connection:
        """Подключение к базе данных"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def get_table_columns(self, conn: sqlite3.Connection, table_name: str) -> List[str]:
        """Получение списка колонок таблицы"""
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [row['name'] for row in cursor.fetchall()]
        return columns
    
    def get_blob_id(self, conn: sqlite3.Connection, table_name: str, rowid: int) -> str:
        """Получение GUID (BLOB ID) по ROWID"""
        cursor = conn.cursor()
        
        # Получаем правильное имя колонки ID
        id_column = self.id_columns.get(table_name, 'ID')
        
        # Проверяем существование колонки
        columns = self.get_table_columns(conn, table_name)
        
        if id_column not in columns:
            print(f"Warning: Column '{id_column}' not found in table '{table_name}'. Available: {columns}")
            # Пробуем найти любую колонку с ID в имени
            for col in columns:
                if 'ID' in col.upper():
                    id_column = col
                    print(f"Debug: Using alternative column '{id_column}'")
                    break
        
        try:
            cursor.execute(f"SELECT {id_column} FROM {table_name} WHERE rowid = ?", (rowid,))
            result = cursor.fetchone()
            if result:
                blob_id = result[id_column]
                if blob_id:
                    if isinstance(blob_id, bytes):
                        return blob_id.hex()
                    else:
                        return str(blob_id)
        except Exception as e:
            print(f"Error getting ID from {table_name} rowid={rowid}: {e}")
        
        return None
    
    def get_current_data(self, conn: sqlite3.Connection, table_name: str, rowid: int) -> Dict[str, Any]:
        """Получение текущих данных записи"""
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name} WHERE rowid = ?", (rowid,))
        result = cursor.fetchone()
        if result:
            return dict(result)
        return {}
    
    def log_change(self, table_name: str, rowid: int, guid: str, 
                   old_data: Dict[str, Any], new_data: Dict[str, Any],
                   changed_fields: List[str], sql_query: str):
        """Логирование изменения"""
        change_record = {
            'timestamp': datetime.now().isoformat(),
            'table_name': table_name,
            'rowid': rowid,
            'guid': guid,
            'changed_fields': ','.join(changed_fields),
            'old_data_json': json.dumps(old_data, default=str, ensure_ascii=False),
            'new_data_json': json.dumps(new_data, default=str, ensure_ascii=False),
            'sql_query': sql_query
        }
        self.changes_log.append(change_record)
    
    def update_bearings(self, conn: sqlite3.Connection):
        """Изменения в таблице Bearings"""
        cursor = conn.cursor()
        
        changes = [
            (1, {'ServiceLife': 50000, 'ContactAngle': 15.0}),
            (2, {'OuterRace_D': 35.5, 'InnerRace_D': 17.5}),
            (3, {'RollingElement_Count': 8, 'RollingElement_D': 3.5}),
            (4, {'BPFi': 10.2, 'BPFO': 8.5, 'BSF': 5.0, 'FTF': 0.46}),
            (5, {'ServiceLife': 60000}),
            (6, {'Number': '16018A', 'ContactAngle': 12.5}),
            (7, {'OuterRace_D': 42.0, 'RollingElement_D': 5.5}),
            (8, {'BPFi': 11.0, 'BPFO': 9.1, 'BSF': 5.5, 'FTF': 0.460}),
            (9, {'ServiceLife': 45000, 'ContactAngle': 10.0}),
            (10, {'RollingElement_Count': 10, 'OuterRace_D': 5.0, 'InnerRace_D': 2.5, 'RollingElement_D': 2.5}),
        ]
        
        for rowid, updates in changes:
            updates['DateModified'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            guid = self.get_blob_id(conn, 'Bearings', rowid)
            old_data = self.get_current_data(conn, 'Bearings', rowid)
            
            if not guid:
                print(f"Warning: GUID not found for Bearings rowid={rowid}")
                continue
            
            set_clause = ', '.join([f"{key} = ?" for key in updates.keys()])
            values = list(updates.values())
            
            sql = f"UPDATE Bearings SET {set_clause} WHERE rowid = ?"
            values.append(rowid)
            
            cursor.execute(sql, values)
            
            new_data = self.get_current_data(conn, 'Bearings', rowid)
            
            self.log_change('Bearings', rowid, guid, old_data, new_data, 
                          list(updates.keys()), sql)
        
        conn.commit()
    
    def update_measure_groups(self, conn: sqlite3.Connection):
        """Изменения в таблице MeasureGroups"""
        cursor = conn.cursor()
        
        changes = [
            (1, {'Name': 'Длина (обновлено)'}),
            (3, {'Name': 'Температура (обновлено)'}),
            (4, {'Name': 'Частота колебаний'}),
            (5, {'Name': 'Скорость вращения'}),
            (7, {'Name': 'Давление (обновлено)'}),
            (13, {'Name': 'Мощность электрическая'}),
            (15, {'Name': 'Масса (обновлено)'}),
            (16, {'Name': 'Объём жидкости'}),
            (14, {'Name': 'Напряжение электрическое'}),
            (20, {'Name': 'TEST_GROUP_v2'}),
        ]
        
        for rowid, updates in changes:
            guid = self.get_blob_id(conn, 'MeasureGroups', rowid)
            old_data = self.get_current_data(conn, 'MeasureGroups', rowid)
            
            if not guid:
                print(f"Warning: GUID not found for MeasureGroups rowid={rowid}")
                continue
            
            set_clause = ', '.join([f"{key} = ?" for key in updates.keys()])
            values = list(updates.values())
            
            sql = f"UPDATE MeasureGroups SET {set_clause} WHERE rowid = ?"
            values.append(rowid)
            
            cursor.execute(sql, values)
            
            new_data = self.get_current_data(conn, 'MeasureGroups', rowid)
            
            self.log_change('MeasureGroups', rowid, guid, old_data, new_data,
                          list(updates.keys()), sql)
        
        conn.commit()
    
    def update_measure_units(self, conn: sqlite3.Connection):
        """Изменения в таблице MeasureUnits"""
        cursor = conn.cursor()
        
        changes = [
            (1, {'Abbreviation': 'мм (new)'}),
            (2, {'Name': 'Сантиметр (обновлено)'}),
            (4, {'Abbreviation': '°C (new)'}),
            (8, {'Name': 'Герц (обновлено)'}),
            (9, {'Abbreviation': 'мм/с (new)'}),
            (15, {'Name': 'Паскаль (обновлено)'}),
            (18, {'Abbreviation': 'бар (new)'}),
            (27, {'Name': 'Ватт (обновлено)'}),
            (16, {'Abbreviation': 'кгс/см^2 (new)'}),
            (24, {'Name': 'Микрометр (обновлено)', 'Abbreviation': 'мкм (new)'}),
        ]
        
        for rowid, updates in changes:
            guid = self.get_blob_id(conn, 'MeasureUnits', rowid)
            old_data = self.get_current_data(conn, 'MeasureUnits', rowid)
            
            if not guid:
                print(f"Warning: GUID not found for MeasureUnits rowid={rowid}")
                continue
            
            set_clause = ', '.join([f"{key} = ?" for key in updates.keys()])
            values = list(updates.values())
            
            sql = f"UPDATE MeasureUnits SET {set_clause} WHERE rowid = ?"
            values.append(rowid)
            
            cursor.execute(sql, values)
            
            new_data = self.get_current_data(conn, 'MeasureUnits', rowid)
            
            self.log_change('MeasureUnits', rowid, guid, old_data, new_data,
                          list(updates.keys()), sql)
        
        conn.commit()
    
    def update_object_property_descriptor_nodes(self, conn: sqlite3.Connection):
        """Изменения в таблице ObjectPropertyDescriptorNodes"""
        cursor = conn.cursor()
        
        changes = [
            (6, {'Description': 'тестовая запись v2'}),
            (3, {'Name': 'Частота вращения (обновлено)'}),
            (27, {'Description': 'для использования в правилах v2'}),
            (12, {'Name': 'Виброскорость (обновлено)'}),
            (13, {'Name': 'Температура подшипника'}),
            (14, {'Name': 'Давление масла'}),
            (16, {'Name': 'Виброускорение (обновлено)'}),
            (17, {'Name': 'Подшипники качения'}),
            (28, {'Name': 'Наработка (часы)'}),
            (4, {'Description': 'контроль роста амплитуд'}),
        ]
        
        for rowid, updates in changes:
            guid = self.get_blob_id(conn, 'ObjectPropertyDescriptorNodes', rowid)
            old_data = self.get_current_data(conn, 'ObjectPropertyDescriptorNodes', rowid)
            
            if not guid:
                print(f"Warning: GUID not found for ObjectPropertyDescriptorNodes rowid={rowid}")
                continue
            
            set_clause = ', '.join([f"{key} = ?" for key in updates.keys()])
            values = list(updates.values())
            
            sql = f"UPDATE ObjectPropertyDescriptorNodes SET {set_clause} WHERE rowid = ?"
            values.append(rowid)
            
            cursor.execute(sql, values)
            
            new_data = self.get_current_data(conn, 'ObjectPropertyDescriptorNodes', rowid)
            
            self.log_change('ObjectPropertyDescriptorNodes', rowid, guid, old_data, new_data,
                          list(updates.keys()), sql)
        
        conn.commit()
    
    def update_object_property_descriptors(self, conn: sqlite3.Connection):
        """Изменения в таблице ObjectPropertyDescriptors"""
        cursor = conn.cursor()
        
        changes = [
            (1, {'Name': 'Амплитуда виброскорости-Роторная (обновлено)', 'Tag': 'AVR_NEW'}),
            (2, {'Description': 'Вероятность дефекта Износ подшипника - v2'}),
            (3, {'Tag': 'RMSASegment_NEW', 'MaxRecords': 600000}),
            (4, {'Name': 'Предыдущее значение раб-ет/не работает (обновлено)'}),
            (5, {'PropertyType': 5, 'SaveHistory': 1}),
            (6, {'Tag': 'LHarm2Tine_NEW', 'Name': 'Левая от второй зубцевой (обновлено)'}),
            (7, {'MaxRecords': 50000}),
            (8, {'Description': 'Разность фаз ППНв-ППНг - обновлено'}),
            (9, {'Name': 'Мощность в полосе 29 (обновлено)', 'PropertyType': 5}),
            (10, {'Tag': 'LHarmBlade_NEW', 'MaxRecords': 2000}),
        ]
        
        for rowid, updates in changes:
            guid = self.get_blob_id(conn, 'ObjectPropertyDescriptors', rowid)
            old_data = self.get_current_data(conn, 'ObjectPropertyDescriptors', rowid)
            
            if not guid:
                print(f"Warning: GUID not found for ObjectPropertyDescriptors rowid={rowid}")
                continue
            
            set_clause = ', '.join([f"{key} = ?" for key in updates.keys()])
            values = list(updates.values())
            
            sql = f"UPDATE ObjectPropertyDescriptors SET {set_clause} WHERE rowid = ?"
            values.append(rowid)
            
            cursor.execute(sql, values)
            
            new_data = self.get_current_data(conn, 'ObjectPropertyDescriptors', rowid)
            
            self.log_change('ObjectPropertyDescriptors', rowid, guid, old_data, new_data,
                          list(updates.keys()), sql)
        
        conn.commit()
    
    def update_objects(self, conn: sqlite3.Connection):
        """Изменения в таблице Objects"""
        cursor = conn.cursor()
        
        changes = [
            (9, {'TemplateName': 'Прогнозирование узла (обновлено)', 'Name': 'Прогнозирование узла (обновлено)'}),
            (10, {'TagName': 'ListHarm_NEW', 'FromTemplateName': 'Подшипник (обновлено)'}),
            # Добавлено еще 8 изменений:
            (11, {'Name': 'Фильтр (обновлено)', 'TagName': 'Фильтр_NEW'}),
            (12, {'Name': 'История дефектов (обновлено)', 'Description': 'История дефектов подшипника v2'}),
            (13, {'Name': 'Прогнозирование узла v2', 'TemplateName': 'Прогнозирование узла v2'}),
            (14, {'Name': 'Структурные гармоники (обновлено)', 'TagName': 'ListHarm_updated'}),
            (15, {'Name': 'Эксцесс (обновлено)', 'Description': 'Эксцесс (задир) v2'}),
            (16, {'Name': 'Задний подшипник (обновлено)', 'TagName': 'ЗПД_new'}),
            (17, {'Name': 'Данные по обслуживанию (обновлено)', 'TemplateName': 'Данные по обслуживанию подшипника v2'}),
            (18, {'Name': 'История дефектов v2', 'FromTemplateName': 'Подшипник v2'}),
        ]
        
        for rowid, updates in changes:
            if 'DateModified' not in updates:
                updates['DateModified'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            guid = self.get_blob_id(conn, 'Objects', rowid)
            old_data = self.get_current_data(conn, 'Objects', rowid)
            
            if not guid:
                print(f"Warning: GUID not found for Objects rowid={rowid}")
                continue
            
            set_clause = ', '.join([f"{key} = ?" for key in updates.keys()])
            values = list(updates.values())
            
            sql = f"UPDATE Objects SET {set_clause} WHERE rowid = ?"
            values.append(rowid)
            
            cursor.execute(sql, values)
            
            new_data = self.get_current_data(conn, 'Objects', rowid)
            
            self.log_change('Objects', rowid, guid, old_data, new_data,
                        list(updates.keys()), sql)
        
        conn.commit()
    
    def update_model_templates(self, conn: sqlite3.Connection):
        """Изменения в таблице ModelTemplates"""
        cursor = conn.cursor()
        
        changes = [
            (1, {'TagName': 'Гильза_обновлено'}),
            (2, {'TagName': 'ПФ_обновлено', 'Description': 'Прогноз FIFOV2 v2'}),
            (4, {'TagName': 'Двигатель_обновлено'}),
            (7, {'TagName': 'Ц_обновлено'}),
            (8, {'TagName': 'НПС_обновлено'}),
            (12, {'TagName': 'kl_обновлено'}),
            (13, {'TagName': '120_65_750_обновлено', 'Description': '8 точек контроля вибрации v2'}),
            (14, {'TagName': 'Прш_обновлено'}),
            (19, {'TagName': 'ОФС_обновлено'}),
            (23, {'TagName': 'Н_обновлено'}),
        ]
        
        for rowid, updates in changes:
            updates['DateModified'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            guid = self.get_blob_id(conn, 'ModelTemplates', rowid)
            old_data = self.get_current_data(conn, 'ModelTemplates', rowid)
            
            if not guid:
                print(f"Warning: GUID not found for ModelTemplates rowid={rowid}")
                continue
            
            set_clause = ', '.join([f"{key} = ?" for key in updates.keys()])
            values = list(updates.values())
            
            sql = f"UPDATE ModelTemplates SET {set_clause} WHERE rowid = ?"
            values.append(rowid)
            
            cursor.execute(sql, values)
            
            new_data = self.get_current_data(conn, 'ModelTemplates', rowid)
            
            self.log_change('ModelTemplates', rowid, guid, old_data, new_data,
                          list(updates.keys()), sql)
        
        conn.commit()
    
    def update_object_types(self, conn: sqlite3.Connection):
        """Изменения в таблице ObjectTypes"""
        cursor = conn.cursor()
        
        changes = [
            (1, {'FromTemplateName': 'Подшипник_обновлено'}),
            (2, {'FromTemplateName': 'Подшипник_v2'}),
            (3, {'FromTemplateName': 'Структурные гармоники цилиндр_обновлено'}),
            (5, {'FromTemplateName': 'Аппаратные уставки_обновлено'}),
            (6, {'FromTemplateName': 'Свойства для расчета наработки_v2'}),
            (7, {'FromTemplateName': 'Аппаратные уставки_new'}),
            (10, {'FromTemplateName': 'Вибропреобразователь_обновлено'}),
            (11, {'FromTemplateName': 'Вибропреобразователь_v2'}),
            (13, {'FromTemplateName': 'тест_обновлено'}),
            (14, {'FromTemplateName': 'Коленвал_обновлено'}),
        ]
        
        for rowid, updates in changes:
            updates['DateModified'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            guid = self.get_blob_id(conn, 'ObjectTypes', rowid)
            old_data = self.get_current_data(conn, 'ObjectTypes', rowid)
            
            if not guid:
                print(f"Warning: GUID not found for ObjectTypes rowid={rowid}")
                continue
            
            set_clause = ', '.join([f"{key} = ?" for key in updates.keys()])
            values = list(updates.values())
            
            sql = f"UPDATE ObjectTypes SET {set_clause} WHERE rowid = ?"
            values.append(rowid)
            
            cursor.execute(sql, values)
            
            new_data = self.get_current_data(conn, 'ObjectTypes', rowid)
            
            self.log_change('ObjectTypes', rowid, guid, old_data, new_data,
                          list(updates.keys()), sql)
        
        conn.commit()
    
    def save_to_csv(self, output_dir: str = '.'):
        """Сохранение лога изменений в CSV"""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Основной файл с полным логом
        filename = output_path / f"data_changes_audit_{self.timestamp}.csv"
        
        if not self.changes_log:
            print("No changes to save")
            return
        
        fieldnames = [
            'timestamp',
            'table_name',
            'rowid',
            'guid',
            'changed_fields',
            'old_data_json',
            'new_data_json',
            'sql_query'
        ]
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.changes_log)
        
        print(f"Changes log saved to: {filename}")
        
        # Создаем отдельный CSV с GUID и данными до/после
        self.save_guid_changes_csv(output_path)
        
        print(f"Total changes: {len(self.changes_log)}")
    
    def save_guid_changes_csv(self, output_path: Path):
        """Создание CSV файла с GUID измененных объектов и данными до/после"""
        guid_filename = output_path / f"changed_objects_guid_{self.timestamp}.csv"
        
        # Собираем все уникальные GUID с данными
        guid_data = []
        for change in self.changes_log:
            guid_record = {
                'table_name': change['table_name'],
                'rowid': change['rowid'],
                'guid': change['guid'],
                'changed_fields': change['changed_fields'],
                'timestamp': change['timestamp'],
                'old_data_json': change['old_data_json'],
                'new_data_json': change['new_data_json']
            }
            guid_data.append(guid_record)
        
        if guid_data:
            fieldnames = [
                'table_name',
                'rowid',
                'guid',
                'changed_fields',
                'timestamp',
                'old_data_json',
                'new_data_json'
            ]
            
            with open(guid_filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(guid_data)
            
            print(f"GUID changes saved to: {guid_filename}")
    
    def run_all_updates(self):
        """Запуск всех обновлений"""
        print(f"Starting data updates at {datetime.now().isoformat()}")
        print(f"Database: {self.db_path}")
        print("-" * 60)
        
        conn = self.connect()
        
        try:
            print("Updating Bearings...")
            self.update_bearings(conn)
            
            print("Updating MeasureGroups...")
            self.update_measure_groups(conn)
            
            print("Updating MeasureUnits...")
            self.update_measure_units(conn)
            
            print("Updating ObjectPropertyDescriptorNodes...")
            self.update_object_property_descriptor_nodes(conn)
            
            print("Updating ObjectPropertyDescriptors...")
            self.update_object_property_descriptors(conn)
            
            print("Updating Objects...")
            self.update_objects(conn)
            
            print("Updating ModelTemplates...")
            self.update_model_templates(conn)
            
            print("Updating ObjectTypes...")
            self.update_object_types(conn)
            
            print("-" * 60)
            print("All updates completed successfully!")
            
        except Exception as e:
            print(f"Error during updates: {e}")
            import traceback
            traceback.print_exc()
            conn.rollback()
            raise
        finally:
            conn.close()
        
        self.save_to_csv('audit_logs')
        
        return self.changes_log


def main():
    """Основная функция"""
    db_path = r'D:\ml-sandbox\test_data\Kriogen_2\Kriogen.db'
    
    if not Path(db_path).exists():
        print(f"Error: Database file not found: {db_path}")
        return
    
    manager = DataAuditManager(db_path)
    changes = manager.run_all_updates()
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    tables_stats = {}
    for change in changes:
        table = change['table_name']
        tables_stats[table] = tables_stats.get(table, 0) + 1
    
    for table, count in tables_stats.items():
        print(f"{table}: {count} changes")
    
    print(f"\nTotal: {len(changes)} changes")
    print("=" * 60)


if __name__ == '__main__':
    main()