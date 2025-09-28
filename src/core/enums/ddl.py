from enum import Enum


class DDLType(Enum):
    """Типы DDL операций."""

    CREATE_TABLE = "CREATE_TABLE"
    CREATE_VIEW = "CREATE_VIEW"
    CREATE_SCHEMA = "CREATE_SCHEMA"
    ALTER_TABLE = "ALTER_TABLE"
    DROP_TABLE = "DROP_TABLE"
    DROP_VIEW = "DROP_VIEW"
    UNKNOWN = "UNKNOWN"
