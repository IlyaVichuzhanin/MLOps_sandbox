"""
DAG для тестирования подключения к HDFS через WebHDFS.

Требования:
1. Установлен провайдер: apache-airflow-providers-apache-hdfs>=4.2.0
2. Создано соединение с conn_id='hdfs_default':
   - Connection Type: HDFS
   - Host: namenode (имя контейнера в вашей Docker-сети)
   - Port: 9870
   - Schema: http
   - Login: hadoop
   - Extra: {"timeout": 15}

Важно: Библиотека hdfs (pywebhdfs) поддерживает ТОЛЬКО следующие публичные методы:
  - status(path, strict=True/False)
  - list(path, status=False)
  - makedirs(path)
  - write(path, ...)
  - read(path, ...)
  - delete(path, recursive=False)
  - set_permission(path, permission)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.exceptions import AirflowException
import json
import socket


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='test_hdfs_connection',
    default_args=default_args,
    description='Тест подключения к HDFS: проверка соединения, запись/чтение файла',
    schedule_interval=None,  # Запускать вручную
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['hdfs', 'test', 'webhdfs'],
) as dag:

    @task(task_id='check_webhdfs_availability')
    def check_webhdfs_availability():
        """Проверка сетевой доступности WebHDFS перед подключением"""
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection('hdfs_default')
        
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            result = sock.connect_ex((conn.host, conn.port or 9870))
            sock.close()
            
            if result != 0:
                raise AirflowException(
                    f"WebHDFS недоступен: {conn.host}:{conn.port or 9870} "
                    f"(код ошибки {result}). Проверьте:\n"
                    f"  1. Работает ли контейнер namenode\n"
                    f"  2. Проброшен ли порт 9870\n"
                    f"  3. Находится ли Airflow в той же Docker-сети"
                )
            print(f"✓ WebHDFS доступен на {conn.host}:{conn.port or 9870}")
            return {'host': conn.host, 'port': conn.port or 9870}
        except Exception as e:
            raise AirflowException(f"Ошибка проверки доступности: {str(e)}")

    @task(task_id='check_connection')
    def check_hdfs_connection():
        """Проверка подключения к HDFS через базовые операции"""
        from hdfs.util import HdfsError
        
        try:
            hook = WebHDFSHook(webhdfs_conn_id='hdfs_default')
            client = hook.get_conn()
            
            # Проверяем подключение через получение статуса корня
            root_status = client.status('/', strict=True)
            
            # Получаем список объектов в корне
            root_items = list(client.list('/', status=False))
            
            print("✓ Успешное подключение к HDFS через WebHDFS")
            print(f"  Корневая директория: {root_status}")
            print(f"  Объектов в корне: {len(root_items)}")
            if len(root_items) > 0:
                print(f"  Пример объектов: {root_items[:5]}")
            
            return {
                'status': 'success',
                'root_items_count': len(root_items),
                'root_type': root_status.get('type'),
                'root_permission': root_status.get('permission')
            }
            
        except HdfsError as e:
            raise AirflowException(f"Ошибка HDFS: {str(e)}")
        except Exception as e:
            raise AirflowException(f"Не удалось подключиться к HDFS: {str(e)}")

    @task(task_id='ensure_user_home')
    def ensure_user_home():
        """Гарантируем существование домашней директории пользователя hadoop"""
        from hdfs.util import HdfsError
        
        try:
            hook = WebHDFSHook(webhdfs_conn_id='hdfs_default')
            client = hook.get_conn()
            user_home = '/user/hadoop'
            
            # Проверяем существование домашней директории
            if client.status(user_home, strict=False) is None:
                print(f"Домашняя директория {user_home} не существует — создаём...")
                client.makedirs(user_home)
                # Устанавливаем права 755
                client.set_permission(user_home, permission='755')
                print(f"✓ Создана домашняя директория: {user_home}")
            else:
                print(f"✓ Домашняя директория существует: {user_home}")
            
            # Проверяем права доступа
            status = client.status(user_home, strict=True)
            print(f"  Права: {status.get('permission')}, Владелец: {status.get('owner')}")
            
            return user_home
            
        except HdfsError as e:
            raise AirflowException(f"Ошибка при работе с домашней директорией: {str(e)}")

    @task(task_id='create_test_directory')
    def create_test_directory(user_home: str):
        """Создание тестовой директории в HDFS"""
        from hdfs.util import HdfsError
        
        try:
            hook = WebHDFSHook(webhdfs_conn_id='hdfs_default')
            client = hook.get_conn()
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            test_dir = f"{user_home}/airflow_hdfs_test_{timestamp}"
            
            # Создаём директорию
            client.makedirs(test_dir)
            
            # Проверяем создание
            status = client.status(test_dir, strict=False)
            if status is not None:
                print(f"✓ Создана тестовая директория: {test_dir}")
                print(f"  Статус: {status}")
                return test_dir
            else:
                raise AirflowException(f"Директория {test_dir} не создана (статус: None)")
                
        except HdfsError as e:
            raise AirflowException(f"Ошибка при создании директории: {str(e)}")

    @task(task_id='write_test_file')
    def write_test_file(test_dir: str):
        """Запись тестового файла в HDFS"""
        from hdfs.util import HdfsError
        
        try:
            hook = WebHDFSHook(webhdfs_conn_id='hdfs_default')
            client = hook.get_conn()
            
            test_file = f"{test_dir}/test_file.json"
            test_data = {
                'timestamp': datetime.now().isoformat(),
                'source': 'airflow_dag',
                'message': 'Тестовое сообщение из Airflow DAG',
                'data': [1, 2, 3, 4, 5]
            }
            
            # Записываем файл
            with client.write(test_file, encoding='utf-8', overwrite=True) as writer:
                json.dump(test_data, writer, ensure_ascii=False, indent=2)
            
            # Проверяем существование и получаем размер
            file_status = client.status(test_file, strict=True)
            print(f"✓ Файл записан: {test_file}")
            print(f"  Размер: {file_status['length']} байт")
            print(f"  Данные: {test_data}")
            
            return test_file
                
        except HdfsError as e:
            raise AirflowException(f"Ошибка записи файла: {str(e)}")

    @task(task_id='read_test_file')
    def read_test_file(test_file: str):
        """Чтение и валидация тестового файла из HDFS"""
        from hdfs.util import HdfsError
        
        try:
            hook = WebHDFSHook(webhdfs_conn_id='hdfs_default')
            client = hook.get_conn()
            
            # Читаем файл
            with client.read(test_file, encoding='utf-8') as reader:
                content = reader.read()
                data = json.loads(content)
            
            print(f"✓ Файл прочитан успешно: {test_file}")
            print(f"  Содержимое: {json.dumps(data, ensure_ascii=False, indent=2)}")
            
            # Валидация данных
            required_fields = ['timestamp', 'source', 'message', 'data']
            missing = [f for f in required_fields if f not in data]
            if missing:
                raise AirflowException(f"Отсутствуют обязательные поля: {missing}")
            
            return {
                'status': 'success',
                'file_size_bytes': len(content),
                'timestamp': data['timestamp'],
                'data_length': len(data['data'])
            }
            
        except HdfsError as e:
            raise AirflowException(f"Ошибка чтения файла: {str(e)}")
        except json.JSONDecodeError as e:
            raise AirflowException(f"Ошибка парсинга JSON: {str(e)}")

    @task(task_id='cleanup_test_data', trigger_rule='all_done')
    def cleanup_test_data(test_dir: str):
        """Очистка тестовых данных (директории)"""
        from hdfs.util import HdfsError
        
        try:
            hook = WebHDFSHook(webhdfs_conn_id='hdfs_default')
            client = hook.get_conn()
            
            # Проверяем существование перед удалением
            if client.status(test_dir, strict=False) is not None:
                # Удаляем рекурсивно
                client.delete(test_dir, recursive=True)
                print(f"✓ Тестовые данные удалены: {test_dir}")
                return {'status': 'cleaned', 'path': test_dir}
            else:
                print(f"⚠ Директория уже удалена или не существует: {test_dir}")
                return {'status': 'already_cleaned', 'path': test_dir}
                
        except HdfsError as e:
            # Не критично — логируем предупреждение
            print(f"⚠ Не удалось удалить тестовые данные {test_dir}: {str(e)}")
            return {'status': 'cleanup_failed', 'path': test_dir, 'error': str(e)}

    # Определяем последовательность задач
    network_check = check_webhdfs_availability()
    connection_status = check_hdfs_connection()
    user_home = ensure_user_home()
    test_dir = create_test_directory(user_home)
    test_file = write_test_file(test_dir)
    read_result = read_test_file(test_file)
    cleanup = cleanup_test_data(test_dir)

    # Зависимости
    network_check >> connection_status >> user_home >> test_dir >> test_file >> read_result >> cleanup