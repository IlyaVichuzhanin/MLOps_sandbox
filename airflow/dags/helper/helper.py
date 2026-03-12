from uuid import UUID, uuid4  # ✅ КРИТИЧНО: Импорт из stdlib, а не из SQLAlchemy
from typing import Any, Optional
import logging


def parse_uuid(value: Any) -> Optional[UUID]:
    uid: Optional[UUID] = None
    # ✅ 1. Если уже UUID - используем как есть
    if isinstance(value, UUID):
        uid = value
    # ✅ 2. Если bytes - проверяем длину
    elif isinstance(value, bytes):
        if len(value) == 16:
            uid = UUID(bytes=value)
        else:
            uid = UUID(value.decode('utf-8').strip())
    # ✅ 3. Если строка - пробуем распарсить
    elif isinstance(value, str):
        uid = UUID(value.strip())
    # ✅ 4. fallback - пробуем привести к строке
    else:
        uid = UUID(str(value).strip())
    return uid