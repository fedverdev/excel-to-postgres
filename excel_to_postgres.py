#!/usr/bin/env python3
"""
Read an Excel sheet and write PostgreSQL INSERT or UPDATE statements to a .sql file
(by default to ./output using workbook name: book.xlsx -> output/book.sql).
Use --stdout to print instead.
Use --create-table to prepend a CREATE TABLE whose column types are inferred from the data.

Array columns: mark with --array-columns. Cell values may use any of these forms; the
script normalizes every one to the same shape in the .sql file: a JSON array literal
'[...]'::jsonb, then ARRAY(SELECT jsonb_array_elements_text(...)) (with COALESCE for []):
  - JSON array, e.g. [1, 2, 3] or ["a", "b"]
  - PostgreSQL brace form, e.g. {a,b,c} or {"x","y"}
  - Pipe-delimited text: a|b|c
  - Comma-separated values (numbers and/or text, depending on content)

Unless --no-infer-array-columns is set, columns are also auto-detected as arrays when
most non-empty cells look like arrays (e.g. a|b|c or JSON [...] or {a,b}), so pipe
cells are compiled the same way as JSON array cells.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


def _cell_looks_like_array_value(raw: Any) -> bool:
    """Heuristic: cell content is probably an array (pipe list, JSON array, PG {…}, etc.)."""
    if isinstance(raw, (list, tuple)) and len(raw) > 1:
        return True
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return False
    s = str(raw).strip()
    if not s:
        return False
    if "|" in s:
        parts = [p.strip() for p in s.split("|") if p.strip() != ""]
        return len(parts) >= 2
    if s.startswith("{") and s.endswith("}"):
        return _parse_braced_pg_array(s) is not None
    if s.startswith("[") and s.endswith("]"):
        try:
            return isinstance(json.loads(s), list)
        except json.JSONDecodeError:
            return False
    return False


def infer_array_columns_from_df(df: pd.DataFrame) -> set[str]:
    """
    Columns where at least half of non-empty cells look like array data.
    Merged with explicit --array-columns so pipe-heavy rows match JSON-heavy rows.
    """
    out: set[str] = set()
    for c in df.columns:
        c = str(c).strip()
        total = 0
        hits = 0
        for v in df[c]:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                continue
            if str(v).strip() == "":
                continue
            total += 1
            if _cell_looks_like_array_value(v):
                hits += 1
        if total and hits * 2 >= total:
            out.add(c)
    return out


def escape_sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def _is_number(s: str) -> bool:
    try:
        float(s)
        return True
    except ValueError:
        return False


def _parse_braced_pg_array(s: str) -> list[Any] | None:
    """Parse PostgreSQL array literal form {elem,elem,...}. Returns None if not a brace array."""
    if not (s.startswith("{") and s.endswith("}")):
        return None
    body = s[1:-1].strip()
    if not body:
        return []
    parts: list[str] = []
    buf: list[str] = []
    in_quote = False
    for i, c in enumerate(body):
        if c == '"':
            in_quote = not in_quote
            buf.append(c)
        elif c == "," and not in_quote:
            parts.append("".join(buf).strip())
            buf = []
        else:
            buf.append(c)
    if buf:
        parts.append("".join(buf).strip())
    if in_quote:
        return None
    out: list[Any] = []
    for p in parts:
        if not p:
            continue
        if len(p) >= 2 and p[0] == '"' and p[-1] == '"':
            try:
                out.append(json.loads(p))
            except json.JSONDecodeError:
                out.append(p[1:-1].replace('""', '"'))
        elif p.lower() in ("null", "none"):
            out.append(None)
        elif _is_number(p):
            f = float(p)
            is_int = float(int(f)) == f and "." not in p and "e" not in p.lower()
            out.append(int(f) if is_int else f)
        else:
            out.append(p)
    return out


def _parse_array_cell(raw: Any) -> list[Any]:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return []
    if isinstance(raw, (list, tuple)):
        return list(raw)
    s = str(raw).strip()
    if not s:
        return []
    if s.startswith("[") and s.endswith("]"):
        try:
            data = json.loads(s)
            if isinstance(data, list):
                return data
        except json.JSONDecodeError:
            pass
    br = _parse_braced_pg_array(s)
    if br is not None:
        return br
    if "|" in s:
        return [p.strip() for p in s.split("|") if p.strip() != ""]
    if "," in s:
        parts = [p.strip() for p in s.split(",")]
        if all(_is_number(p) for p in parts if p != ""):
            return parts
        return parts
    return [s]


def _normalize_array_item(it: Any) -> tuple[str, str]:
    """
    Classify one array element.
    Returns (sql_fragment, tag) where tag is 'int' | 'float' | 'text' | 'bool'.
    """
    if isinstance(it, bool):
        return ("TRUE" if it else "FALSE", "bool")
    if isinstance(it, int) and not isinstance(it, bool):
        return (str(int(it)), "int")
    if isinstance(it, float):
        return (repr(float(it)), "float")
    st = str(it).strip()
    if _is_number(st):
        f = float(st)
        is_int = float(int(f)) == f and "." not in st and "e" not in st.lower()
        return ((str(int(f)) if is_int else repr(f)), "int" if is_int else "float")
    return (escape_sql_string(st), "text")


def _to_float_element(it: Any) -> str:
    if isinstance(it, bool):
        raise TypeError
    if isinstance(it, (int, float)) and not isinstance(it, bool):
        return repr(float(it))
    return repr(float(str(it).strip()))


def _array_sql_elements(items: list[Any]) -> tuple[list[str], str]:
    """Return (element_sql_fragments, postgres array type cast)."""
    tagged = [_normalize_array_item(x) for x in items]
    tags = {t for _, t in tagged}

    if "text" in tags:
        return [escape_sql_string(str(x)) for x in items], "text[]"

    if "bool" in tags:
        if tags == {"bool"}:
            return [s for s, _ in tagged], "boolean[]"
        return [escape_sql_string(str(x)) for x in items], "text[]"

    if tags == {"int"}:
        return [s for s, _ in tagged], "integer[]"

    if tags <= {"int", "float"}:
        elems = [_to_float_element(x) for x in items]
        return elems, "double precision[]"

    return [escape_sql_string(str(x)) for x in items], "text[]"


def _array_pg_suffix(items: list[Any]) -> str:
    _, suffix = _array_sql_elements(items)
    return suffix


def _coerce_json_array_element(it: Any, suffix: str) -> Any:
    """Single value for json.dumps matching PostgreSQL array element type."""
    if it is None or (isinstance(it, float) and pd.isna(it)):
        if suffix == "text[]":
            return ""
        if suffix == "boolean[]":
            return False
        if suffix in ("integer[]", "double precision[]"):
            return 0
    if suffix == "boolean[]":
        if isinstance(it, bool):
            return it
        s = str(it).strip().lower()
        if s in ("true", "1", "yes", "y"):
            return True
        if s in ("false", "0", "no", "n", ""):
            return False
        raise ValueError(f"Not a boolean array element: {it!r}")
    if suffix == "integer[]":
        if isinstance(it, bool):
            raise TypeError
        if isinstance(it, int) and not isinstance(it, bool):
            return int(it)
        if isinstance(it, float):
            return int(it)
        st = str(it).strip()
        return int(float(st))
    if suffix == "double precision[]":
        if isinstance(it, bool):
            raise TypeError
        if isinstance(it, (int, float)) and not isinstance(it, bool):
            return float(it)
        return float(str(it).strip())
    # text[]
    if isinstance(it, bool):
        return "true" if it else "false"
    return str(it)


def _canonical_json_list_for_pg_array(items: list[Any], suffix: str) -> list[Any]:
    return [_coerce_json_array_element(x, suffix) for x in items]


def _pg_array_expr_from_jsonb_literal(lit: str, suffix: str) -> str:
    """
    Build a typed PostgreSQL array from a SQL string literal `lit` that is already
    quoted (escape_sql_string) JSON array text, cast to jsonb. Always uses jsonb + COALESCE
    so an empty JSON [] still yields a typed empty array, not NULL.
    """
    if suffix == "text[]":
        sel = f"ARRAY(SELECT jsonb_array_elements_text({lit}::jsonb))"
        fb = "ARRAY[]::text[]"
    elif suffix == "integer[]":
        sel = f"ARRAY(SELECT (jsonb_array_elements_text({lit}::jsonb))::integer)"
        fb = "ARRAY[]::integer[]"
    elif suffix == "double precision[]":
        sel = f"ARRAY(SELECT (jsonb_array_elements_text({lit}::jsonb))::double precision)"
        fb = "ARRAY[]::double precision[]"
    elif suffix == "boolean[]":
        sel = f"ARRAY(SELECT (jsonb_array_elements_text({lit}::jsonb))::boolean)"
        fb = "ARRAY[]::boolean[]"
    else:
        sel = f"ARRAY(SELECT jsonb_array_elements_text({lit}::jsonb))"
        fb = "ARRAY[]::text[]"
        suffix = "text[]"
    return f"(COALESCE({sel}, {fb}))::{suffix}"


def _format_array_sql_as_jsonb_array(items: list[Any], suffix: str) -> str:
    """Normalize to canonical JSON and emit only via '...'::jsonb + jsonb_array_elements_text."""
    canon = _canonical_json_list_for_pg_array(items, suffix) if items else []
    try:
        json_text = json.dumps(canon, separators=(", ", ": "), ensure_ascii=False)
    except (TypeError, ValueError) as e:
        raise SystemExit(f"Cannot serialize array to JSON: {canon!r} ({e})") from e
    lit = escape_sql_string(json_text)
    return _pg_array_expr_from_jsonb_literal(lit, suffix)


def format_sql_literal(value: Any, *, as_array: bool) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "NULL"

    if as_array:
        items = _parse_array_cell(value)
        suffix = _array_pg_suffix(items) if items else "text[]"
        return _format_array_sql_as_jsonb_array(items, suffix)

    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int) and not isinstance(value, bool):
        return str(int(value))
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return repr(float(value))
    s = str(value).strip()
    if s == "":
        return "NULL"
    if _is_number(s):
        f = float(s)
        if float(int(f)) == f and "e" not in s.lower():
            return str(int(f))
        return repr(f)
    return escape_sql_string(s)


def qualified_table(schema: str | None, table: str) -> str:
    if schema:
        return f'"{schema}"."{table}"'
    return f'"{table}"'


def _cell_scalar_kind(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int) and not isinstance(value, bool):
        return "int"
    if isinstance(value, float):
        return "float" if not value.is_integer() else "int"
    s = str(value).strip()
    if not s:
        return None
    if s.lower() in ("true", "false"):
        return "bool"
    if _is_number(s):
        f = float(s)
        if float(int(f)) == f and "." not in s and "e" not in s.lower():
            return "int"
        return "float"
    return "text"


def infer_scalar_pg_type(series: pd.Series) -> str:
    kinds: set[str] = set()
    for v in series:
        k = _cell_scalar_kind(v)
        if k:
            kinds.add(k)
    if "text" in kinds or len(kinds) > 1 and "bool" in kinds:
        return "TEXT"
    if kinds == {"bool"}:
        return "BOOLEAN"
    if "float" in kinds:
        return "DOUBLE PRECISION"
    if kinds == {"int"}:
        return "INTEGER"
    return "TEXT"


def _merge_pg_array_types(kinds: set[str]) -> str:
    if not kinds:
        return "text[]"
    if "text[]" in kinds:
        return "text[]"
    if "boolean[]" in kinds and kinds != {"boolean[]"}:
        return "text[]"
    if "double precision[]" in kinds:
        return "double precision[]"
    if "integer[]" in kinds:
        return "integer[]"
    if "boolean[]" in kinds:
        return "boolean[]"
    return "text[]"


_DDL_ARRAY_TYPE = {
    "text[]": "TEXT[]",
    "integer[]": "INTEGER[]",
    "double precision[]": "DOUBLE PRECISION[]",
    "boolean[]": "BOOLEAN[]",
}


def infer_array_column_pg_type(series: pd.Series) -> str:
    kinds: set[str] = set()
    for v in series:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        if isinstance(v, str) and not str(v).strip():
            continue
        items = _parse_array_cell(v)
        if not items:
            continue
        _, suffix = _array_sql_elements(items)
        kinds.add(suffix)
    raw = _merge_pg_array_types(kinds)
    return _DDL_ARRAY_TYPE.get(raw, raw.upper())


def infer_column_pg_type(series: pd.Series, *, as_array: bool) -> str:
    if as_array:
        return infer_array_column_pg_type(series)
    return infer_scalar_pg_type(series)


def build_create_table_sql(
    df: pd.DataFrame,
    *,
    table: str,
    schema: str | None,
    array_cols: set[str],
) -> str:
    cols = [str(c).strip() for c in df.columns]
    qt = qualified_table(schema, table)
    parts: list[str] = []
    for c in cols:
        pg_t = infer_column_pg_type(df[c], as_array=c in array_cols)
        parts.append(f'  "{c}" {pg_t}')
    return f"CREATE TABLE {qt} (\n" + ",\n".join(parts) + "\n);"


def build_inserts(
    df: pd.DataFrame,
    *,
    table: str,
    schema: str | None,
    array_cols: set[str],
    null_tokens: set[str],
) -> Iterable[str]:
    cols = [str(c).strip() for c in df.columns]
    qt = qualified_table(schema, table)
    col_list = ", ".join(f'"{c}"' for c in cols)

    for _, row in df.iterrows():
        values: list[str] = []
        for c in cols:
            v = row[c]
            if isinstance(v, str) and v.strip() in null_tokens:
                values.append("NULL")
            else:
                values.append(format_sql_literal(v, as_array=c in array_cols))
        yield f"INSERT INTO {qt} ({col_list}) VALUES ({', '.join(values)});"


def build_updates(
    df: pd.DataFrame,
    *,
    table: str,
    schema: str | None,
    key_columns: list[str],
    array_cols: set[str],
    null_tokens: set[str],
) -> Iterable[str]:
    cols = [str(c).strip() for c in df.columns]
    missing = [k for k in key_columns if k not in cols]
    if missing:
        raise SystemExit(f"Key columns not found in sheet: {missing}")

    qt = qualified_table(schema, table)
    non_keys = [c for c in cols if c not in key_columns]

    for _, row in df.iterrows():
        sets: list[str] = []
        for c in non_keys:
            v = row[c]
            if isinstance(v, str) and v.strip() in null_tokens:
                lit = "NULL"
            else:
                lit = format_sql_literal(v, as_array=c in array_cols)
            sets.append(f'"{c}" = {lit}')

        wheres: list[str] = []
        for k in key_columns:
            v = row[k]
            if isinstance(v, str) and v.strip() in null_tokens:
                wheres.append(f'"{k}" IS NULL')
            else:
                wheres.append(f'"{k}" = {format_sql_literal(v, as_array=k in array_cols)}')

        yield f"UPDATE {qt} SET {', '.join(sets)} WHERE {' AND '.join(wheres)};"


def main() -> None:
    p = argparse.ArgumentParser(description="Compile Excel to PostgreSQL SQL (INSERT or UPDATE).")
    p.add_argument("--excel", required=True, type=Path, help="Path to .xlsx file")
    p.add_argument("--table", required=True, help="Target table name (unquoted identifier)")
    p.add_argument("--schema", default=None, help="Optional schema name")
    p.add_argument("--sheet", default=0, help="Sheet name or index (default: first sheet)")
    p.add_argument(
        "--mode",
        choices=("insert", "update"),
        default="insert",
        help="insert: one INSERT per row; update: UPDATE ... WHERE key columns match",
    )
    p.add_argument(
        "--key-columns",
        default="",
        help="Comma-separated key column names (required for --mode update)",
    )
    p.add_argument(
        "--array-columns",
        default="",
        help="Comma-separated columns that are PostgreSQL arrays (merged with inferred columns by default)",
    )
    p.add_argument(
        "--no-infer-array-columns",
        action="store_true",
        help="Do not auto-detect array columns; only --array-columns are treated as arrays",
    )
    p.add_argument(
        "--null-token",
        action="append",
        default=[],
        help="Treat this cell string as SQL NULL (repeatable). Default: empty string",
    )
    p.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="SQL file path (default: ./output/<excel_name>.sql)",
    )
    p.add_argument("--stdout", action="store_true", help="Print SQL to stdout instead of writing a file")
    p.add_argument(
        "--create-table",
        action="store_true",
        help="Prepend CREATE TABLE inferred from sheet columns (types are best-effort)",
    )

    args = p.parse_args()
    null_tokens = {""} | {t for t in args.null_token if t is not None}

    explicit_array_cols = {c.strip() for c in args.array_columns.split(",") if c.strip()}
    key_columns = [c.strip() for c in args.key_columns.split(",") if c.strip()]

    if args.mode == "update" and not key_columns:
        p.error("--key-columns is required when --mode update")

    if not args.excel.is_file():
        raise SystemExit(f"Excel file not found: {args.excel}")

    df = pd.read_excel(args.excel, sheet_name=args.sheet, dtype=object)
    df.columns = [str(c).strip() for c in df.columns]

    unknown_arrays = explicit_array_cols - set(df.columns)
    if unknown_arrays:
        raise SystemExit(f"--array-columns not in sheet headers: {sorted(unknown_arrays)}")

    inferred_array_cols = set() if args.no_infer_array_columns else infer_array_columns_from_df(df)
    array_cols = explicit_array_cols | inferred_array_cols

    if args.mode == "insert":
        lines = list(
            build_inserts(
                df,
                table=args.table,
                schema=args.schema,
                array_cols=array_cols,
                null_tokens=null_tokens,
            )
        )
    else:
        lines = list(
            build_updates(
                df,
                table=args.table,
                schema=args.schema,
                key_columns=key_columns,
                array_cols=array_cols,
                null_tokens=null_tokens,
            )
        )

    chunks: list[str] = []
    if args.create_table:
        chunks.append(
            build_create_table_sql(
                df,
                table=args.table,
                schema=args.schema,
                array_cols=array_cols,
            )
        )
    chunks.extend(lines)
    text = "\n".join(chunks)
    if text:
        text = text + "\n"
    if args.stdout:
        sys.stdout.write(text)
    else:
        out_path = (
            args.output
            if args.output is not None
            else (Path.cwd() / "output" / f"{args.excel.stem}.sql")
        )
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text, encoding="utf-8")


if __name__ == "__main__":
    main()
