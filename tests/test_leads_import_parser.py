from __future__ import annotations

from pathlib import Path

from core.leads_import import (
    preview_usernames_from_csv,
    read_usernames_from_csv,
    read_usernames_from_txt,
)


def test_csv_skips_single_column_header(tmp_path: Path) -> None:
    csv_path = tmp_path / "header.csv"
    csv_path.write_text("username\nuno\ndos\n", encoding="utf-8")

    assert read_usernames_from_csv(csv_path) == ["uno", "dos"]


def test_csv_uses_username_column_from_multi_column_header(tmp_path: Path) -> None:
    csv_path = tmp_path / "multi.csv"
    csv_path.write_text("id,username\n1,uno\n2,dos\n", encoding="utf-8")

    assert read_usernames_from_csv(csv_path) == ["uno", "dos"]


def test_csv_detects_semicolon_delimiter_and_alias_header(tmp_path: Path) -> None:
    csv_path = tmp_path / "semicolon.csv"
    csv_path.write_text("usuario;nombre\nuno;Uno\ndos;Dos\n", encoding="utf-8")

    assert read_usernames_from_csv(csv_path) == ["uno", "dos"]


def test_csv_supports_cp1252_with_non_utf8_headers(tmp_path: Path) -> None:
    csv_path = tmp_path / "cp1252.csv"
    csv_path.write_bytes("usuario;descripci\xf3n\nuno;cl\xednica\ndos;asesor\xeda\n".encode("latin-1"))

    assert read_usernames_from_csv(csv_path) == ["uno", "dos"]


def test_txt_supports_utf16_files(tmp_path: Path) -> None:
    txt_path = tmp_path / "utf16.txt"
    txt_path.write_text("uno\ndos\n", encoding="utf-16")

    assert read_usernames_from_txt(txt_path) == ["uno", "dos"]


def test_csv_preview_reports_metadata_duplicates_and_invalid_rows(tmp_path: Path) -> None:
    csv_path = tmp_path / "preview.csv"
    csv_path.write_text("id;usuario\n1;uno\n2;uno\n3;\n4;dos\n", encoding="utf-8")

    preview = preview_usernames_from_csv(csv_path)

    assert preview.kind == "csv"
    assert preview.encoding == "utf-8-sig"
    assert preview.delimiter == ";"
    assert preview.header_detected is True
    assert preview.username_column == "usuario"
    assert preview.selected_column_index == 1
    assert preview.row_count == 5
    assert preview.max_columns == 2
    assert preview.used_first_column_fallback is False
    assert preview.usernames == ["uno", "dos"]
    assert preview.duplicate_count == 1
    assert preview.blank_or_invalid_count == 1


def test_csv_preview_marks_multicolumn_fallback_when_no_header_is_found(tmp_path: Path) -> None:
    csv_path = tmp_path / "fallback.csv"
    csv_path.write_text("1,uno\n2,dos\n", encoding="utf-8")

    preview = preview_usernames_from_csv(csv_path)

    assert preview.header_detected is False
    assert preview.selected_column_index == 0
    assert preview.max_columns == 2
    assert preview.used_first_column_fallback is True
    assert preview.usernames == ["1", "2"]
