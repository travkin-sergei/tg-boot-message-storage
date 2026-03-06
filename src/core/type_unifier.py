# src/schema_comparator.py
"""
Модуль для сравнения схем между источниками данных без подключения к БД.
"""
import json
import re

from pathlib import Path
from typing import Dict, Any, List, Union, Tuple


class SchemaComparator:
    """
    Класс для сравнения схем между источниками данных.

    Возвращает: (True, {}) если всё хорошо,
                (False, {описание проблем}) если есть несовпадения.
    """

    # Маппинг типов для разных источников
    TYPE_MAPPING: Dict[str, Dict[str, str]] = {
        'parquet': {
            'int32': 'INTEGER',
            'int64': 'BIGINT',
            'float': 'REAL',
            'double': 'DOUBLE PRECISION',
            'string': 'VARCHAR',
            'boolean': 'BOOLEAN',
            'date32': 'DATE',
            'date64': 'DATE',
            'timestamp': 'TIMESTAMP',
            'timestamp[us, tz=utc]': 'TIMESTAMPTZ',
            'timestamp[ms]': 'TIMESTAMP',
            'timestamp[us]': 'TIMESTAMP',
            'timestamp[ns]': 'TIMESTAMP',
        },
        'postgresql': {
            'integer': 'INTEGER',
            'bigint': 'BIGINT',
            'smallint': 'SMALLINT',
            'real': 'REAL',
            'double precision': 'DOUBLE PRECISION',
            'numeric': 'DECIMAL',
            'decimal': 'DECIMAL',
            'varchar': 'VARCHAR',
            'character varying': 'VARCHAR',
            'text': 'TEXT',
            'boolean': 'BOOLEAN',
            'date': 'DATE',
            'time': 'TIME',
            'timestamp': 'TIMESTAMP',
            'timestamp with time zone': 'TIMESTAMPTZ',
            'timestamptz': 'TIMESTAMPTZ',
            'json': 'JSON',
            'jsonb': 'JSON',
            'uuid': 'UUID',
            'bytea': 'BYTEA',
        },
        'duckdb': {
            'integer': 'INTEGER',
            'bigint': 'BIGINT',
            'smallint': 'SMALLINT',
            'float': 'REAL',
            'double': 'DOUBLE PRECISION',
            'decimal': 'DECIMAL',
            'varchar': 'VARCHAR',
            'text': 'TEXT',
            'boolean': 'BOOLEAN',
            'date': 'DATE',
            'timestamp': 'TIMESTAMP',
            'timestamp with time zone': 'TIMESTAMPTZ',
            'json': 'JSON',
        },
        'mysql': {
            'int': 'INTEGER',
            'bigint': 'BIGINT',
            'smallint': 'SMALLINT',
            'float': 'REAL',
            'double': 'DOUBLE PRECISION',
            'decimal': 'DECIMAL',
            'varchar': 'VARCHAR',
            'text': 'TEXT',
            'boolean': 'BOOLEAN',
            'date': 'DATE',
            'datetime': 'TIMESTAMP',
            'timestamp': 'TIMESTAMP',
            'json': 'JSON',
        },
    }

    # Матрица совместимости типов
    COMPATIBLE_TYPES: Dict[str, List[str]] = {
        'INTEGER': ['INTEGER', 'BIGINT', 'SMALLINT', 'REAL', 'DOUBLE PRECISION', 'DECIMAL', 'TEXT'],
        'BIGINT': ['BIGINT', 'INTEGER', 'REAL', 'DOUBLE PRECISION', 'DECIMAL', 'TEXT'],
        'SMALLINT': ['SMALLINT', 'INTEGER', 'BIGINT', 'REAL', 'DOUBLE PRECISION', 'DECIMAL', 'TEXT'],
        'REAL': ['REAL', 'DOUBLE PRECISION', 'DECIMAL', 'TEXT'],
        'DOUBLE PRECISION': ['DOUBLE PRECISION', 'REAL', 'DECIMAL', 'TEXT'],
        'DECIMAL': ['DECIMAL', 'REAL', 'DOUBLE PRECISION', 'TEXT'],
        'VARCHAR': ['VARCHAR', 'TEXT', 'CHAR'],
        'TEXT': ['TEXT', 'VARCHAR'],
        'BOOLEAN': ['BOOLEAN', 'INTEGER', 'SMALLINT'],
        'DATE': ['DATE', 'TIMESTAMP', 'TEXT'],
        'TIME': ['TIME', 'TIMESTAMP', 'TEXT'],
        'TIMESTAMP': ['TIMESTAMP', 'TIMESTAMPTZ', 'DATETIME', 'TEXT'],
        'TIMESTAMPTZ': ['TIMESTAMPTZ', 'TIMESTAMP', 'DATETIME', 'TEXT'],
        'JSON': ['JSON', 'JSONB', 'TEXT'],
        'UUID': ['UUID', 'VARCHAR', 'TEXT'],
        'BYTEA': ['BYTEA', 'BLOB', 'BINARY'],
    }

    def __init__(self, default_varchar_length: int = 255):
        self.default_varchar_length = default_varchar_length

    def compare(self,
                source_schema: Union[Dict[str, str], str, Path],
                source_type: str,
                target_schema: Union[Dict[str, str], str, Path],
                target_type: str) -> Tuple[bool, Dict[str, Any]]:
        """
        Сравнивает схему источника со схемой цели.

        Args:
            source_schema: JSON со схемой источника (словарь, строка JSON или путь к файлу).
            source_type: Тип источника ('parquet', 'postgresql', 'duckdb', 'mysql').
            target_schema: JSON со схемой цели (словарь, строка JSON или путь к файлу).
            target_type: Тип цели ('postgresql', 'duckdb', 'mysql').

        Returns:
            (True, {}) если всё хорошо,
            (False, {описание проблем}) если есть несовпадения.

        Пример:
            >> comparator = SchemaComparator()
            >> ok, errors = comparator.compare(
            ...     source_schema={'id': 'INT64', 'name': 'STRING'},
            ...     source_type='parquet',
            ...     target_schema={'id': 'BIGINT', 'name': 'VARCHAR'},
            ...     target_type='postgresql'
            ... )
            >> if ok:
            ...     print("Всё хорошо")
            ... else:
            ...     print(errors)
        """
        # Загружаем схемы
        source = self._load_schema(source_schema)
        target = self._load_schema(target_schema)

        # Нормализуем типы источников
        source_type = source_type.lower().strip()
        target_type = target_type.lower().strip()

        # Получаем маппинги
        source_mapping = self.TYPE_MAPPING.get(source_type, {})
        target_mapping = self.TYPE_MAPPING.get(target_type, {})

        errors = {}

        source_fields = set(source.keys())
        target_fields = set(target.keys())

        # === Проверка отсутствующих полей в цели ===
        missing_in_target = source_fields - target_fields
        for field_name in sorted(missing_in_target):
            errors[field_name] = {
                'issue': 'missing_in_target',
                'source_type': source[field_name],
                'target_type': None,
                'message': f"Поле '{field_name}' отсутствует в целевой таблице"
            }

        # === Проверка лишних полей в цели ===
        extra_in_target = target_fields - source_fields
        for field_name in sorted(extra_in_target):
            errors[field_name] = {
                'issue': 'extra_in_target',
                'source_type': None,
                'target_type': target[field_name],
                'message': f"Поле '{field_name}' есть только в целевой таблице"
            }

        # === Сравнение общих полей ===
        common_fields = source_fields & target_fields
        for field_name in sorted(common_fields):
            source_field_type = source[field_name]
            target_field_type = target[field_name]

            # Нормализуем типы
            normalized_source = self._normalize_type(source_field_type, source_mapping)
            normalized_target = self._normalize_type(target_field_type, target_mapping)

            # Проверяем совместимость
            is_compatible, message = self._check_compatibility(
                normalized_source,
                normalized_target,
                source_field_type,
                target_field_type
            )

            if not is_compatible:
                errors[field_name] = {
                    'issue': 'type_mismatch',
                    'source_type': source_field_type,
                    'target_type': target_field_type,
                    'message': message
                }

        # Возвращаем результат
        if errors:
            return False, errors
        else:
            return True, {}

    def _load_schema(self, schema_input: Union[Dict, str, Path]) -> Dict[str, str]:
        """Загружает схему из словаря, JSON строки или файла."""
        if isinstance(schema_input, dict):
            return schema_input

        if isinstance(schema_input, (str, Path)):
            path = Path(schema_input)
            if path.exists():
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return self._extract_schema(data)
            try:
                data = json.loads(schema_input)
                return self._extract_schema(data)
            except json.JSONDecodeError:
                raise ValueError(f"Invalid JSON: {schema_input}")

        raise ValueError(f"Unsupported schema input type: {type(schema_input)}")

    def _extract_schema(self, data: Dict[str, Any]) -> Dict[str, str]:
        """Извлекает схему из JSON."""
        # Прямой формат: {'id': 'INTEGER', 'name': 'VARCHAR'}
        if all(isinstance(v, str) for v in data.values()):
            return data

        # Вложенный формат: {'schema': {'id': 'INTEGER', ...}}
        if 'schema' in data:
            return data['schema']

        # Формат с колонками: {'columns': [{'name': 'id', 'type': 'INTEGER'}, ...]}
        if 'columns' in data:
            return {col['name']: col['type'] for col in data['columns']}

        # Формат parquet: {'fields': [{'name': 'id', 'type': 'INT64'}, ...]}
        if 'fields' in data:
            return {f['name']: f['type'] for f in data['fields']}

        raise ValueError(f"Cannot extract schema from: {data}")

    def _normalize_type(self, type_str: str, type_mapping: Dict[str, str]) -> str:
        """Нормализует тип данных через маппинг источника."""
        if not type_str:
            return 'UNKNOWN'

        original = type_str.strip()
        normalized = original.lower()

        # Прямое совпадение в маппинге
        if normalized in type_mapping:
            return type_mapping[normalized]

        # Извлекаем базовый тип
        match = re.match(r'^([a-z_\s]+)(?:\(([^)]+)\))?', normalized)
        if match:
            base_type = match.group(1).strip()
            if base_type in type_mapping:
                return type_mapping[base_type]

        # Особая обработка для VARCHAR
        if 'varchar' in normalized or 'char' in normalized:
            return 'VARCHAR'

        # Особая обработка для DECIMAL
        if 'decimal' in normalized or 'numeric' in normalized:
            return 'DECIMAL'

        # Особая обработка для TIMESTAMP
        if 'timestamp' in normalized:
            if 'time zone' in normalized or 'timestamptz' in normalized:
                return 'TIMESTAMPTZ'
            return 'TIMESTAMP'

        return original.upper()

    def _check_compatibility(self,
                             source_normalized: str,
                             target_normalized: str,
                             source_original: str,
                             target_original: str) -> Tuple[bool, str]:
        """Проверяет совместимость типов."""
        source_base = self._extract_base_type(source_normalized)
        target_base = self._extract_base_type(target_normalized)

        compatible_targets = self.COMPATIBLE_TYPES.get(source_base, [])

        if target_base in compatible_targets:
            return True, "Типы совместимы"

        return False, f"Несовместимые типы: {source_original} → {target_original}"

    def _extract_base_type(self, type_str: str) -> str:
        """Извлекает базовый тип из строки."""
        match = re.match(r'^([a-z_\s]+)(?:\(|$)', type_str.lower())
        if match:
            return match.group(1).strip().upper()
        return type_str.strip().upper()
