# Trino MCP Server

**Model Context Protocol (MCP) сервер для анализа схем базы данных Trino**

Этот проект представляет собой MCP сервер, который предоставляет мощные инструменты для анализа структуры баз данных Trino. Сервер принимает параметры подключения динамически и поддерживает работу с множественными подключениями одновременно.

## 🚀 Основные возможности

### Базовые операции

- **Проверка подключения** к Trino серверам
- **Навигация по структуре БД**: каталоги → схемы → таблицы
- **Описание структуры таблиц** с детальной информацией о колонках
- **Безопасное выполнение SQL запросов** с ограничениями
- **Анализ запросов** через EXPLAIN без выполнения

### Работа с DDL

- **Валидация DDL выражений** перед выполнением
- **Анализ зависимостей** между объектами базы данных
- **Безопасное выполнение DDL** с предварительной проверкой
- **Обнаружение конфликтов** имен объектов
- **Построение графа зависимостей** для правильного порядка создания

### Документация и анализ

- **Автогенерация документации** схемы базы данных
- **Анализ качества DDL** с рекомендациями
- **Обнаружение потенциальных проблем** в структуре
- **Статистика использования подключений**

## 🛠 Установка

### Требования

- Python 3.11+
- Poetry для управления зависимостями
- Доступ к Trino серверу

### Установка зависимостей

```bash
# Клонирование репозитория
git clone <repository-url>
cd VkHackMCP

# Установка зависимостей через Poetry
poetry install

# Или через pip (если Poetry не используется)
pip install -r requirements.txt
```

### Настройка окружения

Создайте файл `.env` на основе `.env.example`:

```bash
cp .env.example .env
```

Пример конфигурации `.env`:

```env
# Настройки приложения
APP_NAME=trino-mcp
LOG_LEVEL=INFO
```

## 🎯 Запуск

### Разработка

```bash
# Запуск MCP сервера для разработки
make run-server

# Или напрямую через Python
poetry run python -m src.api.server
```

### Форматирование и проверка кода

```bash
# Форматирование кода
make format

# Проверка стиля кода
make lint

# Все проверки сразу
make check-all
```

### Docker

```bash
# Сборка образа
docker build -t trino-mcp .

# Запуск контейнера
docker run -it trino-mcp
```

## 📖 Использование

MCP сервер предоставляет следующие инструменты:

### Базовые инструменты

#### `connection_status`

Проверяет статус подключения к Trino серверу.

```json
{
  "jdbc_url": "jdbc:trino://host:443?user=analyst"
}
```

#### `list_catalogs`

Возвращает список всех доступных каталогов.

```json
{
  "jdbc_url": "jdbc:trino://host:443?user=analyst"
}
```

#### `list_schemas`

Получает список схем в указанном каталоге.

```json
{
  "jdbc_url": "jdbc:trino://host:443?user=analyst",
  "catalog": "hive"
}
```

#### `describe_table`

Описывает структуру таблицы с детальной информацией о колонках.

```json
{
  "jdbc_url": "jdbc:trino://host:443?user=analyst",
  "table": "users",
  "schema": "default",
  "catalog": "hive"
}
```

### Инструменты для работы с DDL

#### `validate_ddl_statements`

Анализирует список DDL выражений без их выполнения.

```json
{
  "ddl_list": [
    "CREATE TABLE users (id bigint, name varchar(255))",
    "CREATE VIEW active_users AS SELECT * FROM users WHERE active = true"
  ]
}
```

#### `execute_ddl_statements`

Выполняет DDL с предварительной валидацией.

```json
{
  "jdbc_url": "jdbc:trino://host:443?user=analyst",
  "ddl_list": ["CREATE TABLE test (id bigint)"],
  "catalog": "hive",
  "schema": "default",
  "validate_first": true
}
```

#### `analyze_schema_dependencies`

Анализирует зависимости между объектами и рекомендует порядок создания.

```json
{
  "jdbc_url": "jdbc:trino://host:443?user=analyst",
  "ddl_list": [
    "CREATE VIEW v1 AS SELECT * FROM table1",
    "CREATE TABLE table1 (id bigint)"
  ],
  "catalog": "hive",
  "schema": "default"
}
```

### Инструменты анализа и документации

#### `generate_schema_documentation`

Создает документацию для схемы базы данных.

```json
{
  "jdbc_url": "jdbc:trino://host:443?user=analyst",
  "catalog": "hive",
  "schema": "default",
  "include_ddl": ["CREATE TABLE additional (id bigint)"]
}
```

### Пример workflow

```python
# 1. Проверка подключения
connection_status(jdbc_url="jdbc:trino://host:443?user=analyst")

# 2. Анализ существующей структуры
list_catalogs(jdbc_url="...")
list_schemas(jdbc_url="...", catalog="hive")
list_tables(jdbc_url="...", schema="default", catalog="hive")

# 3. Валидация новых DDL
validate_ddl_statements(ddl_list=["CREATE TABLE ...", "CREATE VIEW ..."])

# 4. Анализ зависимостей
analyze_schema_dependencies(jdbc_url="...", ddl_list=["..."])

# 5. Выполнение DDL
execute_ddl_statements(jdbc_url="...", ddl_list=["..."], validate_first=True)

# 6. Генерация документации
generate_schema_documentation(jdbc_url="...", catalog="hive", schema="default")
```

## 📊 Мониторинг

### Статистика подключений

```json
{
  "tool": "get_connection_stats"
}
```
