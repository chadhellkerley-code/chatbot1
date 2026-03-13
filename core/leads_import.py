from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass
from pathlib import Path

from core.leads_store import normalize_lead_username, normalize_lead_usernames

_ENCODING_CANDIDATES = ("utf-8-sig", "utf-8", "utf-16", "cp1252", "latin-1")
_CSV_DELIMITERS = (",", ";", "\t", "|")
_USERNAME_HEADER_ALIASES = {
    "username",
    "user",
    "usuario",
    "nombre_de_usuario",
    "instagram",
    "instagram_username",
    "usuario_instagram",
    "ig",
    "ig_username",
    "handle",
}


class LeadImportError(ValueError):
    pass


@dataclass(frozen=True)
class LeadImportPreview:
    kind: str
    encoding: str
    delimiter: str
    header_detected: bool
    username_column: str
    selected_column_index: int
    row_count: int
    max_columns: int
    used_first_column_fallback: bool
    usernames: list[str]
    duplicate_count: int
    blank_or_invalid_count: int


def _read_text_with_known_encodings(path: str | Path, *, label: str) -> tuple[str, str]:
    file_path = Path(path)
    if not file_path.is_file():
        raise LeadImportError(f"No existe el archivo {label}: {file_path}")

    last_error: Exception | None = None
    for encoding in _ENCODING_CANDIDATES:
        try:
            return file_path.read_text(encoding=encoding), encoding
        except UnicodeDecodeError as exc:
            last_error = exc
            continue
        except Exception as exc:
            raise LeadImportError(f"No se pudo leer el archivo {label}: {file_path}") from exc
    raise LeadImportError(
        f"No se pudo leer el archivo {label} con encodings soportados: {file_path}"
    ) from last_error


def _detect_csv_delimiter(sample: str) -> tuple[str, csv.Dialect | None]:
    snippet = sample[:4096]
    try:
        dialect = csv.Sniffer().sniff(snippet, delimiters="".join(_CSV_DELIMITERS))
        return dialect.delimiter, dialect
    except Exception:
        pass
    for delimiter in _CSV_DELIMITERS:
        if delimiter in snippet:
            return delimiter, None
    return ",", None


def _normalize_header_label(value: object) -> str:
    label = str(value or "").strip().lower()
    label = re.sub(r"[^a-z0-9]+", "_", label)
    return label.strip("_")


def _username_column_index(header_row: list[str]) -> int | None:
    for index, cell in enumerate(header_row):
        if _normalize_header_label(cell) in _USERNAME_HEADER_ALIASES:
            return index
    return None


def _read_csv_rows(text: str) -> list[list[str]]:
    delimiter, dialect = _detect_csv_delimiter(text)
    stream = io.StringIO(text)
    if dialect is not None:
        reader = csv.reader(stream, dialect=dialect)
    else:
        reader = csv.reader(stream, delimiter=delimiter)

    rows: list[list[str]] = []
    for row in reader:
        trimmed = [cell.strip() for cell in row]
        if not any(trimmed):
            continue
        rows.append(trimmed)
    return rows


def _extract_csv_usernames(rows: list[list[str]]) -> list[str]:
    (
        usernames,
        _header_detected,
        _username_column,
        _selected_column_index,
        _max_columns,
        _used_first_column_fallback,
    ) = _extract_csv_preview(rows)
    return usernames


def _collect_normalized_usernames(values: list[str]) -> tuple[list[str], int, int]:
    normalized_raw = [normalize_lead_username(value) for value in values]
    nonempty = [value for value in normalized_raw if value]
    usernames = normalize_lead_usernames(nonempty)
    duplicate_count = max(0, len(nonempty) - len(usernames))
    blank_or_invalid_count = max(0, len(values) - len(nonempty))
    return usernames, duplicate_count, blank_or_invalid_count


def _extract_csv_preview(rows: list[list[str]]) -> tuple[list[str], bool, str, int, int, bool]:
    if not rows:
        return [], False, "", 0, 0, False

    header_index = _username_column_index(rows[0])
    data_rows = rows[1:] if header_index is not None else rows
    header_detected = header_index is not None
    username_column = rows[0][header_index].strip() if header_index is not None else ""
    selected_column_index = header_index if header_index is not None else 0
    used_first_column_fallback = header_index is None and len(rows[0]) > 1
    if header_index is None and len(rows[0]) == 1:
        first_value = _normalize_header_label(rows[0][0])
        if first_value in _USERNAME_HEADER_ALIASES:
            data_rows = rows[1:]
            header_detected = True
            username_column = rows[0][0].strip()
            selected_column_index = 0
            used_first_column_fallback = False

    usernames: list[str] = []
    max_columns = max((len(row) for row in rows), default=0)
    for row in data_rows:
        if not row:
            continue
        if selected_column_index >= len(row):
            continue
        usernames.append(row[selected_column_index])
    return usernames, header_detected, username_column, selected_column_index, max_columns, used_first_column_fallback


def preview_usernames_from_csv(path: str | Path) -> LeadImportPreview:
    text, encoding = _read_text_with_known_encodings(path, label="CSV")
    rows = _read_csv_rows(text)
    (
        raw_usernames,
        header_detected,
        username_column,
        selected_column_index,
        max_columns,
        used_first_column_fallback,
    ) = _extract_csv_preview(rows)
    usernames, duplicate_count, blank_or_invalid_count = _collect_normalized_usernames(raw_usernames)
    delimiter, _dialect = _detect_csv_delimiter(text)
    return LeadImportPreview(
        kind="csv",
        encoding=encoding,
        delimiter=delimiter,
        header_detected=header_detected,
        username_column=username_column,
        selected_column_index=selected_column_index,
        row_count=len(rows),
        max_columns=max_columns,
        used_first_column_fallback=used_first_column_fallback,
        usernames=usernames,
        duplicate_count=duplicate_count,
        blank_or_invalid_count=blank_or_invalid_count,
    )


def preview_usernames_from_txt(path: str | Path) -> LeadImportPreview:
    text, encoding = _read_text_with_known_encodings(path, label="TXT")
    raw_usernames = text.splitlines()
    usernames, duplicate_count, blank_or_invalid_count = _collect_normalized_usernames(raw_usernames)
    return LeadImportPreview(
        kind="txt",
        encoding=encoding,
        delimiter="",
        header_detected=False,
        username_column="",
        selected_column_index=0,
        row_count=len(raw_usernames),
        max_columns=1,
        used_first_column_fallback=False,
        usernames=usernames,
        duplicate_count=duplicate_count,
        blank_or_invalid_count=blank_or_invalid_count,
    )


def read_usernames_from_csv(path: str | Path) -> list[str]:
    return preview_usernames_from_csv(path).usernames


def read_usernames_from_txt(path: str | Path) -> list[str]:
    return preview_usernames_from_txt(path).usernames
