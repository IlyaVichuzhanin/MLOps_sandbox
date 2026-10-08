import sqlite3
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional


class DataAuditManager:
    """Управление аудитом изменений связей между таблицами"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.changes_log = []
        self.skipped_count = 0
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        self.id_columns = {
            'MeasureGroups': 'ID',
            'MeasureUnits': 'ID',
            'Objects': 'ID',
            'ObjectProperties': 'PropertyID',
            'ObjectPropertyDescriptors': 'PropertyDescriptorID',
        }
    
    def connect(self) -> sqlite3.Connection:
        """Подключение к базе данных"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def get_blob_id(self, conn: sqlite3.Connection, table_name: str, rowid: int) -> Optional[str]:
        """Получение GUID (BLOB ID) по ROWID"""
        cursor = conn.cursor()
        id_column = self.id_columns.get(table_name, 'ID')
        
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
    
    def get_blob_id_bytes(self, conn: sqlite3.Connection, table_name: str, rowid: int) -> Optional[bytes]:
        """Получение GUID в формате bytes для записи в БД"""
        cursor = conn.cursor()
        id_column = self.id_columns.get(table_name, 'ID')
        
        try:
            cursor.execute(f"SELECT {id_column} FROM {table_name} WHERE rowid = ?", (rowid,))
            result = cursor.fetchone()
            if result:
                blob_id = result[id_column]
                return blob_id if isinstance(blob_id, bytes) else None
        except Exception as e:
            print(f"Error getting ID bytes from {table_name} rowid={rowid}: {e}")
        
        return None
    
    def get_all_measure_groups(self, conn: sqlite3.Connection) -> List[Dict[str, Any]]:
        """Получение всех записей из MeasureGroups с их GUID"""
        cursor = conn.cursor()
        cursor.execute("SELECT rowid, ID, Name FROM MeasureGroups")
        
        groups = []
        for row in cursor.fetchall():
            guid = row['ID'].hex() if isinstance(row['ID'], bytes) else str(row['ID'])
            groups.append({
                'rowid': row['rowid'],
                'guid': guid,
                'guid_bytes': row['ID'],
                'name': row['Name']
            })
        
        return groups
    
    def get_all_objects(self, conn: sqlite3.Connection) -> List[Dict[str, Any]]:
        """Получение всех записей из Objects с их GUID"""
        cursor = conn.cursor()
        cursor.execute("SELECT rowid, ID, Name, TagName FROM Objects")
        
        objects = []
        for row in cursor.fetchall():
            guid = row['ID'].hex() if isinstance(row['ID'], bytes) else str(row['ID'])
            objects.append({
                'rowid': row['rowid'],
                'guid': guid,
                'guid_bytes': row['ID'],
                'name': row['Name'],
                'tagname': row['TagName']
            })
        
        return objects
    
    def get_all_measure_units(self, conn: sqlite3.Connection) -> List[Dict[str, Any]]:
        """Получение всех записей из MeasureUnits с их GUID"""
        cursor = conn.cursor()
        cursor.execute("SELECT rowid, ID, Name, Abbreviation FROM MeasureUnits")
        
        units = []
        for row in cursor.fetchall():
            guid = row['ID'].hex() if isinstance(row['ID'], bytes) else str(row['ID'])
            units.append({
                'rowid': row['rowid'],
                'guid': guid,
                'guid_bytes': row['ID'],
                'name': row['Name'],
                'abbreviation': row['Abbreviation']
            })
        
        return units
    
    def get_current_data(self, conn: sqlite3.Connection, table_name: str, rowid: int) -> Dict[str, Any]:
        """Получение текущих данных записи"""
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name} WHERE rowid = ?", (rowid,))
        result = cursor.fetchone()
        if result:
            return dict(result)
        return {}
    
    def log_change(self, table_name: str, rowid: int, guid: str, 
                   changed_fields: Dict[str, Dict], sql_query: str):
        """
        Логирование изменения связи
        
        changed_fields формат:
        {
            'field_name': {
                'old_guid': '...',
                'new_guid': '...',
                'old_name': '...',
                'new_name': '...'
            }
        }
        """
        change_record = {
            'timestamp': datetime.now().isoformat(),
            'table_name': table_name,
            'rowid': rowid,
            'record_guid': guid,
            'changed_fields_json': json.dumps(changed_fields, default=str, ensure_ascii=False),
            'sql_query': sql_query
        }
        self.changes_log.append(change_record)
    
    def update_measure_units_links(self, conn: sqlite3.Connection):
        """Изменение связей MeasureGroupID в таблице MeasureUnits"""
        cursor = conn.cursor()
        measure_groups = self.get_all_measure_groups(conn)
        
        if not measure_groups:
            print("❌ Error: No MeasureGroups found in database!")
            return
        
        print("\n" + "="*80)
        print("ДОСТУПНЫЕ MeasureGroups (группы измерений):")
        print("="*80)
        for i, group in enumerate(measure_groups, 1):
            print(f"{i:2d}. RowID={group['rowid']:2d} | GUID={group['guid'][:16]}... | {group['name']}")
        
        changes = [
            (1, 3),   # Миллиметр: Длина -> Температура
            (2, 15),  # Сантиметр: Длина -> Масса
            (4, 1),   # Градус Цельсия: Температура -> Длина
            (8, 19),  # Герц: Частота колебаний -> Относительные
            (9, 17),  # мм/с: Скорость вращения -> Расход
            (15, 8),  # Паскаль: Давление -> Производительность
            (18, 19), # бар: Давление -> Относительные
            (27, 21), # Ватт: Мощность -> Энергия
            (16, 15), # кгс/см^2: Давление -> Масса
            (24, 10), # Микрометр: Длина -> Угол
        ]
        
        print("\n" + "="*80)
        print("ИЗМЕНЕНИЕ СВЯЗЕЙ MeasureUnits -> MeasureGroups")
        print("="*80)
        
        for unit_rowid, new_group_rowid in changes:
            if new_group_rowid > len(measure_groups):
                print(f"⚠ Warning: MeasureGroups rowid={new_group_rowid} not found, skipping...")
                self.skipped_count += 1
                continue
            
            unit_guid = self.get_blob_id(conn, 'MeasureUnits', unit_rowid)
            if not unit_guid:
                print(f"⚠ Warning: GUID not found for MeasureUnits rowid={unit_rowid}")
                self.skipped_count += 1
                continue
            
            old_data = self.get_current_data(conn, 'MeasureUnits', unit_rowid)
            old_measure_group_bytes = old_data.get('MeasureGroupID')
            old_measure_group_guid = old_measure_group_bytes.hex() if old_measure_group_bytes else None
            
            old_group_name = "N/A"
            if old_measure_group_guid:
                for group in measure_groups:
                    if group['guid'] == old_measure_group_guid:
                        old_group_name = group['name']
                        break
            
            new_group = measure_groups[new_group_rowid - 1]
            new_measure_group_guid = new_group['guid']
            new_measure_group_bytes = new_group['guid_bytes']
            new_group_name = new_group['name']
            
            unit_name = old_data.get('Name', 'N/A')
            unit_abbr = old_data.get('Abbreviation', 'N/A')
            
            if old_measure_group_guid == new_measure_group_guid:
                print(f"\n⏭ MeasureUnits RowID {unit_rowid}:")
                print(f"   Единица: {unit_name} ({unit_abbr})")
                print(f"    Пропущено: группа не изменилась ({old_group_name})")
                self.skipped_count += 1
                continue
            
            print(f"\n MeasureUnits RowID {unit_rowid}:")
            print(f"   Единица: {unit_name} ({unit_abbr})")
            print(f"   GUID единицы: {unit_guid[:16]}...")
            print(f"   Новая группа (RowID {new_group_rowid}): {new_group_name}")
            
            sql = f"UPDATE MeasureUnits SET MeasureGroupID = ? WHERE rowid = ?"
            cursor.execute(sql, (new_measure_group_bytes, unit_rowid))
            
            changed_fields = {
                'MeasureGroupID': {
                    'old_guid': old_measure_group_guid,
                    'new_guid': new_measure_group_guid,
                    'old_name': old_group_name,
                    'new_name': new_group_name
                }
            }
            
            self.log_change('MeasureUnits', unit_rowid, unit_guid, changed_fields, sql)
            print(f"   ✅ Связь изменена")
        
        conn.commit()
    
    def update_object_properties_links(self, conn: sqlite3.Connection):
        """
        Изменение связей в таблице ObjectProperties:
        - ObjectID -> ссылка на Objects
        - MeasureUnitID -> ссылка на MeasureUnits
        """
        cursor = conn.cursor()
        
        # Получаем все доступные Objects и MeasureUnits
        objects = self.get_all_objects(conn)
        measure_units = self.get_all_measure_units(conn)
        
        if not objects:
            print("❌ Error: No Objects found in database!")
            return
        
        if not measure_units:
            print("❌ Error: No MeasureUnits found in database!")
            return
        
        print("\n" + "="*80)
        print("ДОСТУПНЫЕ Objects (объекты):")
        print("="*80)
        for i, obj in enumerate(objects[:15], 1):
            tagname = obj.get('tagname', '')[:20] if obj.get('tagname') else ''
            print(f"{i:2d}. RowID={obj['rowid']:2d} | GUID={obj['guid'][:16]}... | {obj['name'][:40]} | {tagname}")
        if len(objects) > 15:
            print(f"... и еще {len(objects) - 15} объектов")
        
        print("\n" + "="*80)
        print("ДОСТУПНЫЕ MeasureUnits (единицы измерения):")
        print("="*80)
        for i, unit in enumerate(measure_units[:15], 1):
            abbr = unit.get('abbreviation', '')[:15] if unit.get('abbreviation') else ''
            print(f"{i:2d}. RowID={unit['rowid']:2d} | GUID={unit['guid'][:16]}... | {unit['name'][:35]} | {abbr}")
        if len(measure_units) > 15:
            print(f"... и еще {len(measure_units) - 15} единиц измерения")
        
        # Определяем изменения для ObjectProperties
        # Формат: (rowid ObjectProperties, new_object_rowid, new_measure_unit_rowid)
        # None означает "не менять"
        changes = [
            (1, 3, 9),    # Property 1: Object 1->3, MeasureUnit -> 9 (мм/с)
            (2, 5, 15),   # Property 2: Object 2->5, MeasureUnit -> 15 (Па)
            (3, 7, 4),    # Property 3: Object 3->7, MeasureUnit -> 4 (°C)
            (4, 2, 1),    # Property 4: Object 4->2, MeasureUnit -> 1 (мм)
            (5, 9, 27),   # Property 5: Object 5->9, MeasureUnit -> 27 (Вт)
            (6, 11, 8),   # Property 6: Object 11, MeasureUnit -> 8 (Гц)
            (7, 4, 10),   # Property 7: Object 4, MeasureUnit -> 10 (м/с^2)
            (8, None, 18),# Property 8: Object не менять, MeasureUnit -> 18 (бар)
            (9, 6, 10),   # Property 9: Object 6, MeasureUnit -> 10 (м/с^2)
            (10, 8, 24),  # Property 10: Object 8, MeasureUnit -> 24 (мкм)
        ]
        
        print("\n" + "="*80)
        print("ИЗМЕНЕНИЕ СВЯЗЕЙ ObjectProperties -> Objects и MeasureUnits")
        print("="*80)
        
        for prop_rowid, new_obj_rowid, new_unit_rowid in changes:
            # Проверяем, есть ли изменения
            if new_obj_rowid is None and new_unit_rowid is None:
                print(f"\n⏭ ObjectProperties RowID {prop_rowid}:")
                print(f"   ⚠ Пропущено: нет изменений")
                self.skipped_count += 1
                continue
            
            # Проверяем существование Object
            if new_obj_rowid is not None and new_obj_rowid > len(objects):
                print(f"⚠ Warning: Objects rowid={new_obj_rowid} not found, skipping...")
                self.skipped_count += 1
                continue
            
            # Проверяем существование MeasureUnit
            if new_unit_rowid is not None and new_unit_rowid > len(measure_units):
                print(f"⚠ Warning: MeasureUnits rowid={new_unit_rowid} not found, skipping...")
                self.skipped_count += 1
                continue
            
            # Получаем GUID свойства
            prop_guid = self.get_blob_id(conn, 'ObjectProperties', prop_rowid)
            if not prop_guid:
                print(f"⚠ Warning: GUID not found for ObjectProperties rowid={prop_rowid}")
                self.skipped_count += 1
                continue
            
            # Получаем текущие данные
            old_data = self.get_current_data(conn, 'ObjectProperties', prop_rowid)
            
            # Текущий ObjectID
            old_object_bytes = old_data.get('ObjectID')
            old_object_guid = old_object_bytes.hex() if old_object_bytes else None
            
            # Текущий MeasureUnitID
            old_unit_bytes = old_data.get('MeasureUnitID')
            old_unit_guid = old_unit_bytes.hex() if old_unit_bytes else None
            
            # Находим имена старых сущностей
            old_object_name = "N/A"
            old_object_rowid = None
            if old_object_guid:
                for obj in objects:
                    if obj['guid'] == old_object_guid:
                        old_object_name = obj['name']
                        old_object_rowid = obj['rowid']
                        break
            
            old_unit_name = "N/A"
            old_unit_rowid = None
            if old_unit_guid:
                for unit in measure_units:
                    if unit['guid'] == old_unit_guid:
                        old_unit_name = unit['name']
                        old_unit_rowid = unit['rowid']
                        break
            
            # Получаем новые значения
            new_object_guid = None
            new_object_bytes = None
            new_object_name = None
            if new_obj_rowid is not None:
                new_obj = objects[new_obj_rowid - 1]
                new_object_guid = new_obj['guid']
                new_object_bytes = new_obj['guid_bytes']
                new_object_name = new_obj['name']
            
            new_unit_guid = None
            new_unit_bytes = None
            new_unit_name = None
            if new_unit_rowid is not None:
                new_unit = measure_units[new_unit_rowid - 1]
                new_unit_guid = new_unit['guid']
                new_unit_bytes = new_unit['guid_bytes']
                new_unit_name = new_unit['name']
            
            # Проверяем, есть ли реальные изменения
            object_changed = (new_obj_rowid is not None and old_object_guid != new_object_guid)
            unit_changed = (new_unit_rowid is not None and old_unit_guid != new_unit_guid)
            
            if not object_changed and not unit_changed:
                print(f"\n⏭ ObjectProperties RowID {prop_rowid}:")
                print(f"   ⚠ Пропущено: связи не изменились")
                self.skipped_count += 1
                continue
            
            print(f"\n📝 ObjectProperties RowID {prop_rowid}:")
            print(f"   GUID свойства: {prop_guid[:16]}...")
            
            if object_changed:
                print(f"   Object (старый RowID {old_object_rowid}): {old_object_name}")
                print(f"   Object (новый RowID {new_obj_rowid}): {new_object_name}")
            
            if unit_changed:
                print(f"   MeasureUnit (старый RowID {old_unit_rowid}): {old_unit_name}")
                print(f"   MeasureUnit (новый RowID {new_unit_rowid}): {new_unit_name}")
            
            # Формируем SQL запрос
            updates = []
            values = []
            changed_fields = {}
            
            if object_changed:
                updates.append("ObjectID = ?")
                values.append(new_object_bytes)
                changed_fields['ObjectID'] = {
                    'old_guid': old_object_guid,
                    'new_guid': new_object_guid,
                    'old_name': old_object_name,
                    'new_name': new_object_name
                }
            
            if unit_changed:
                updates.append("MeasureUnitID = ?")
                values.append(new_unit_bytes)
                changed_fields['MeasureUnitID'] = {
                    'old_guid': old_unit_guid,
                    'new_guid': new_unit_guid,
                    'old_name': old_unit_name,
                    'new_name': new_unit_name
                }
            
            set_clause = ', '.join(updates)
            sql = f"UPDATE ObjectProperties SET {set_clause} WHERE rowid = ?"
            values.append(prop_rowid)
            
            # Выполняем обновление
            cursor.execute(sql, values)
            
            # Логируем
            self.log_change('ObjectProperties', prop_rowid, prop_guid, changed_fields, sql)
            print(f"   ✅ Связи изменены")
        
        conn.commit()
        print("\n" + "="*80)
        print("✅ ObjectProperties обновлены успешно!")
        print("="*80)
    
    def save_to_csv(self, output_dir: str = '.'):
        """Сохранение лога изменений в CSV"""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        if not self.changes_log:
            print(" No changes to save")
            return
        
        # Основной файл с изменениями
        filename = output_path / f"all_tables_links_changes_{self.timestamp}.csv"
        
        fieldnames = [
            'timestamp',
            'table_name',
            'rowid',
            'record_guid',
            'changed_fields_json',
            'sql_query'
        ]
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.changes_log)
        
        print(f"\n Changes log saved to: {filename}")
        
        # Файл с GUID
        guid_filename = output_path / f"changed_links_guid_{self.timestamp}.csv"
        
        guid_data = []
        for change in self.changes_log:
            changed_fields = json.loads(change['changed_fields_json'])
            
            for field_name, field_data in changed_fields.items():
                guid_record = {
                    'table_name': change['table_name'],
                    'rowid': change['rowid'],
                    'record_guid': change['record_guid'],
                    'field_name': field_name,
                    'old_guid': field_data.get('old_guid', ''),
                    'new_guid': field_data.get('new_guid', ''),
                    'old_name': field_data.get('old_name', ''),
                    'new_name': field_data.get('new_name', ''),
                    'timestamp': change['timestamp']
                }
                guid_data.append(guid_record)
        
        with open(guid_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'table_name', 'rowid', 'record_guid', 'field_name',
                'old_guid', 'new_guid', 'old_name', 'new_name', 'timestamp'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(guid_data)
        
        print(f"📁 GUID changes saved to: {guid_filename}")
        print(f"\n📊 Total links changed: {len(self.changes_log)}")
        print(f"📊 Total skipped: {self.skipped_count}")
    
    def run_all_updates(self):
        """Запуск всех обновлений"""
        print(f"\n🚀 Starting links update at {datetime.now().isoformat()}")
        print(f" Database: {self.db_path}")
        
        conn = self.connect()
        
        try:
            # Сначала MeasureUnits -> MeasureGroups
            self.update_measure_units_links(conn)
            
            # Затем ObjectProperties -> Objects и MeasureUnits (всё в одном методе)
            self.update_object_properties_links(conn)
            
        except Exception as e:
            print(f"\n❌ Error during updates: {e}")
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
        print(f"❌ Error: Database file not found: {db_path}")
        return
    
    print("="*80)
    print("DATA VAULT 2.0 - UPDATE MULTIPLE TABLE LINKS")
    print("Изменение связей в MeasureUnits и ObjectProperties")
    print("="*80)
    
    manager = DataAuditManager(db_path)
    changes = manager.run_all_updates()
    
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    
    # Статистика по таблицам
    tables_stats = {}
    for change in changes:
        table = change['table_name']
        tables_stats[table] = tables_stats.get(table, 0) + 1
    
    for table, count in sorted(tables_stats.items()):
        print(f"  {table}: {count} changes")
    
    print(f"\n  Total changes: {len(changes)}")
    print(f"  Total skipped: {manager.skipped_count}")
    print("="*80)


if __name__ == '__main__':
    main()