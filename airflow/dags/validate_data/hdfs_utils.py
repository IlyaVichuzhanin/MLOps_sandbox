# airflow/dags/validate_data/common/hdfs_utils.py
from contextlib import contextmanager
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.exceptions import AirflowException
import tempfile
import os

@contextmanager
def hdfs_tempfile(hdfs_path: str, suffix: str = '.sqlite'):
    """
    Контекстный менеджер для безопасного скачивания файла из HDFS.
    Автоматически удаляет временный файл после использования.
    """
    hook = WebHDFSHook(webhdfs_conn_id='hdfs_default')
    client = hook.get_conn()
    
    file_status = client.status(hdfs_path, strict=False)
    if file_status is None:
        raise FileNotFoundError(f"Файл не найден в HDFS: {hdfs_path}")
    if file_status.get('type') != 'FILE':
        raise ValueError(f"Путь {hdfs_path} не является файлом (тип: {file_status.get('type')})")
    
    with tempfile.NamedTemporaryFile(mode='wb', suffix=suffix, delete=False) as tmp_file:
        temp_path = tmp_file.name
        try:
            with client.read(hdfs_path, chunk_size=65536) as reader:
                for chunk in reader:
                    tmp_file.write(chunk)
            tmp_file.flush()
            os.fsync(tmp_file.fileno())
            yield temp_path
        finally:
            if os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except OSError as e:
                    print(f"⚠ Не удалось удалить временный файл {temp_path}: {e}")