import re
from typing import Any, Dict, List, Optional, Set

from src.core.enums.ddl import DDLType
from src.core.logging import get_logger

logger = get_logger(__name__)


class DDLAnalyzer:

    def __init__(self):
        self.ddl_patterns = {
            DDLType.CREATE_TABLE: re.compile(r"^\s*CREATE\s+TABLE\s+", re.IGNORECASE),
            DDLType.CREATE_VIEW: re.compile(r"^\s*CREATE\s+VIEW\s+", re.IGNORECASE),
            DDLType.CREATE_SCHEMA: re.compile(r"^\s*CREATE\s+SCHEMA\s+", re.IGNORECASE),
            DDLType.ALTER_TABLE: re.compile(r"^\s*ALTER\s+TABLE\s+", re.IGNORECASE),
            DDLType.DROP_TABLE: re.compile(r"^\s*DROP\s+TABLE\s+", re.IGNORECASE),
            DDLType.DROP_VIEW: re.compile(r"^\s*DROP\s+VIEW\s+", re.IGNORECASE),
        }

        self.object_name_pattern = re.compile(
            r"(?:CREATE|ALTER|DROP)\s+(?:TABLE|VIEW|SCHEMA)\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?([^\s\(]+)",
            re.IGNORECASE,
        )

        self.column_pattern = re.compile(
            r"\(\s*([^)]+)\s*\)", re.IGNORECASE | re.DOTALL
        )

    def identify_ddl_type(self, ddl: str) -> DDLType:
        """
        Определяет тип DDL выражения.

        :param ddl: DDL выражение
        :return: Тип DDL
        """
        ddl_clean = ddl.strip()

        for ddl_type, pattern in self.ddl_patterns.items():
            if pattern.match(ddl_clean):
                return ddl_type

        return DDLType.UNKNOWN

    def extract_object_name(self, ddl: str) -> Optional[str]:
        """
        Извлекает имя объекта из DDL выражения.

        :param ddl: DDL выражение
        :return: Имя объекта или None
        """
        match = self.object_name_pattern.search(ddl)
        if match:
            return match.group(1).strip('`"')
        return None

    def extract_dependencies(self, ddl: str) -> Set[str]:
        """
        Извлекает зависимости из DDL выражения (referenced tables/views).

        :param ddl: DDL выражение
        :return: Множество имен объектов-зависимостей
        """
        dependencies = set()

        if self.identify_ddl_type(ddl) == DDLType.CREATE_VIEW:
            from_pattern = re.compile(r"\bFROM\s+([^\s,\)]+)", re.IGNORECASE)
            join_pattern = re.compile(r"\bJOIN\s+([^\s,\)]+)", re.IGNORECASE)

            for pattern in [from_pattern, join_pattern]:
                matches = pattern.findall(ddl)
                for match in matches:
                    table_name = match.strip('`"')
                    if table_name and not table_name.upper() in [
                        "SELECT",
                        "WHERE",
                        "GROUP",
                        "ORDER",
                    ]:
                        dependencies.add(table_name)

        return dependencies

    def extract_columns_from_create_table(self, ddl: str) -> List[Dict[str, Any]]:
        """
        Извлекает информацию о колонках из CREATE TABLE.

        :param ddl: CREATE TABLE выражение
        :return: Список колонок с их свойствами
        """
        if self.identify_ddl_type(ddl) != DDLType.CREATE_TABLE:
            return []

        match = self.column_pattern.search(ddl)
        if not match:
            return []

        columns_text = match.group(1)
        columns = []

        current_column = ""
        paren_count = 0

        for char in columns_text:
            if char == "(":
                paren_count += 1
            elif char == ")":
                paren_count -= 1
            elif char == "," and paren_count == 0:
                if current_column.strip():
                    columns.append(
                        self._parse_column_definition(current_column.strip())
                    )
                current_column = ""
                continue

            current_column += char

        if current_column.strip():
            columns.append(self._parse_column_definition(current_column.strip()))

        return columns

    def _parse_column_definition(self, column_def: str) -> Dict[str, Any]:
        """
        Парсит определение отдельной колонки.

        :param column_def: Определение колонки
        :return: Словарь с информацией о колонке
        """
        parts = column_def.strip().split()
        if len(parts) < 2:
            return {
                "name": column_def,
                "type": "UNKNOWN",
                "nullable": True,
                "constraints": [],
            }

        name = parts[0].strip('`"')
        type_part = parts[1]

        size_match = re.match(r"([^(]+)\(([^)]+)\)", type_part)
        if size_match:
            data_type = size_match.group(1)
            size = size_match.group(2)
        else:
            data_type = type_part
            size = None

        constraints = []
        nullable = True

        remaining = " ".join(parts[2:]).upper()
        if "NOT NULL" in remaining:
            nullable = False
            constraints.append("NOT NULL")
        if "PRIMARY KEY" in remaining:
            constraints.append("PRIMARY KEY")
            nullable = False
        if "UNIQUE" in remaining:
            constraints.append("UNIQUE")
        if "DEFAULT" in remaining:
            constraints.append("DEFAULT")

        return {
            "name": name,
            "type": data_type.upper(),
            "size": size,
            "nullable": nullable,
            "constraints": constraints,
        }

    def analyze_ddl_list(self, ddl_list: List[str]) -> Dict[str, Any]:
        """
        Анализирует список DDL выражений.

        :param ddl_list: Список DDL выражений
        :return: Результаты анализа
        """
        results = {
            "total_statements": len(ddl_list),
            "by_type": {},
            "objects": [],
            "dependencies": {},
            "potential_issues": [],
        }

        for ddl_type in DDLType:
            results["by_type"][ddl_type.value] = 0

        for i, ddl in enumerate(ddl_list):
            if not ddl or not ddl.strip():
                continue

            ddl_type = self.identify_ddl_type(ddl)
            results["by_type"][ddl_type.value] += 1

            object_name = self.extract_object_name(ddl)
            dependencies = self.extract_dependencies(ddl)

            obj_info = {
                "index": i,
                "type": ddl_type.value,
                "name": object_name,
                "dependencies": list(dependencies),
                "ddl_preview": ddl[:100] + "..." if len(ddl) > 100 else ddl,
            }

            if ddl_type == DDLType.CREATE_TABLE:
                columns = self.extract_columns_from_create_table(ddl)
                obj_info["columns"] = columns
                obj_info["column_count"] = len(columns)

            results["objects"].append(obj_info)

            if dependencies:
                results["dependencies"][object_name or f"statement_{i}"] = list(
                    dependencies
                )

            self._check_ddl_issues(
                ddl, ddl_type, object_name, results["potential_issues"], i
            )

        return results

    def _check_ddl_issues(
        self,
        ddl: str,
        ddl_type: DDLType,
        object_name: Optional[str],
        issues: List[Dict],
        index: int,
    ):
        """
        Проверяет DDL на потенциальные проблемы.

        :param ddl: DDL выражение
        :param ddl_type: Тип DDL
        :param object_name: Имя объекта
        :param issues: Список для добавления найденных проблем
        :param index: Индекс DDL в списке
        """
        if ddl_type in [
            DDLType.CREATE_TABLE,
            DDLType.CREATE_VIEW,
            DDLType.CREATE_SCHEMA,
        ]:
            if "IF NOT EXISTS" not in ddl.upper():
                issues.append(
                    {
                        "type": "missing_if_not_exists",
                        "severity": "warning",
                        "message": "CREATE statement без IF NOT EXISTS может вызвать ошибку если объект уже существует",
                        "object": object_name,
                        "statement_index": index,
                    }
                )

        if ddl_type in [DDLType.DROP_TABLE, DDLType.DROP_VIEW]:
            if "IF EXISTS" not in ddl.upper():
                issues.append(
                    {
                        "type": "missing_if_exists",
                        "severity": "warning",
                        "message": "DROP statement без IF EXISTS может вызвать ошибку если объект не существует",
                        "object": object_name,
                        "statement_index": index,
                    }
                )

        if ddl_type == DDLType.DROP_TABLE:
            issues.append(
                {
                    "type": "destructive_operation",
                    "severity": "high",
                    "message": "DROP TABLE - деструктивная операция, которая удалит данные",
                    "object": object_name,
                    "statement_index": index,
                }
            )

        if ddl_type == DDLType.CREATE_TABLE:
            if "PRIMARY KEY" not in ddl.upper() and "UNIQUE" not in ddl.upper():
                issues.append(
                    {
                        "type": "no_primary_key",
                        "severity": "warning",
                        "message": "Таблица создается без первичного ключа или уникального индекса",
                        "object": object_name,
                        "statement_index": index,
                    }
                )


ddl_analyzer = DDLAnalyzer()
