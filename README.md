# Excel to PostgreSQL converter

Скрипт `excel_to_postgres.py` читает данные из Excel-файла (`.xlsx`) и генерирует SQL-скрипт для PostgreSQL:

- `INSERT` для каждой строки таблицы
- или `UPDATE` по ключевым колонкам
- по желанию добавляет `CREATE TABLE` с автоопределением типов колонок

По умолчанию результат сохраняется в папку `output`:

- `book.xlsx` -> `output/book.sql`

Также можно вывести SQL в консоль через `--stdout` или указать свой путь через `--output`.

## Требования

- Python 3.10+
- Установленные зависимости:
  - `pandas`
  - `openpyxl`

Установка зависимостей:

```bash
pip install -r requirements.txt
```

## Быстрый старт

Базовая генерация `INSERT`:

```bash
python excel_to_postgres.py --excel data.xlsx --table users
```

После выполнения появится файл:

- `output/data.sql`

## Основные параметры

- `--excel` - путь к Excel-файлу (`.xlsx`), обязательно
- `--table` - имя целевой таблицы PostgreSQL, обязательно
- `--schema` - схема БД (например `public`)
- `--sheet` - имя листа или индекс листа (по умолчанию первый лист)
- `--mode insert|update` - режим генерации (по умолчанию `insert`)
- `--key-columns` - ключевые колонки для `update` (через запятую)
- `--array-columns` - колонки-массивы (через запятую)
- `--no-infer-array-columns` - отключает автоопределение колонок-массивов
- `--null-token` - строка, которая должна считаться `NULL` (параметр можно повторять)
- `--create-table` - добавить перед DML командой `CREATE TABLE`
- `--output` (`-o`) - путь к итоговому `.sql` файлу
- `--stdout` - вывести SQL в консоль вместо записи в файл

## Примеры использования

### 1) INSERT в схему public

```bash
python excel_to_postgres.py --excel employees.xlsx --table employees --schema public
```

### 2) UPDATE по ключам

```bash
python excel_to_postgres.py --excel employees.xlsx --table employees --mode update --key-columns id
```

Для `--mode update` параметр `--key-columns` обязателен.

### 3) Добавить CREATE TABLE + INSERT

```bash
python excel_to_postgres.py --excel products.xlsx --table products --create-table
```

### 4) Явно указать путь выходного файла

```bash
python excel_to_postgres.py --excel data.xlsx --table users --output output/custom_users.sql
```

### 5) Вывести SQL в консоль

```bash
python excel_to_postgres.py --excel data.xlsx --table users --stdout
```

## Работа с массивами

Скрипт умеет обрабатывать массивы PostgreSQL. Поддерживаются форматы значений в ячейках:

- JSON-массив: `[1, 2, 3]`
- PostgreSQL-формат: `{a,b,c}`
- Через вертикальную черту: `a|b|c`
- Через запятую: `a,b,c` (или числа)

Колонки-массивы можно:

- указать явно через `--array-columns`
- или доверить автоопределение (включено по умолчанию)

## Примечания

- Пустые значения и заданные через `--null-token` токены становятся `NULL`.
- Имена колонок берутся из заголовков Excel-листа.
- Выходной SQL-файл по умолчанию создается в папке `output`.
