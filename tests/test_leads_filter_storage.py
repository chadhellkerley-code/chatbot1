from __future__ import annotations

import json
from pathlib import Path

import core.leads as leads


def _configure_filter_storage(monkeypatch, tmp_path: Path) -> Path:
    root = tmp_path / "lead_filters"
    lists_dir = root / "lists"
    config_path = root / "filters_config.json"
    lists_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(leads, "FILTER_STORAGE", root)
    monkeypatch.setattr(leads, "FILTER_LISTS", lists_dir)
    monkeypatch.setattr(leads, "FILTER_CONFIG_PATH", config_path)
    return root


def test_filter_lists_use_unique_ids_even_with_back_to_back_creation(monkeypatch, tmp_path: Path) -> None:
    root = _configure_filter_storage(monkeypatch, tmp_path)

    first = leads._create_filter_list(["uno"])
    second = leads._create_filter_list(["dos"])

    assert first["id"] != second["id"]
    assert len(list((root / "lists").glob("*.json"))) == 2


def test_loading_filter_lists_skips_corrupted_payload_and_keeps_backup(monkeypatch, tmp_path: Path) -> None:
    root = _configure_filter_storage(monkeypatch, tmp_path)
    leads._save_filter_list({"id": "valid_1", "items": [], "export_alias": "demo"})
    broken_path = root / "lists" / "broken.json"
    broken_path.write_text("{bad json", encoding="utf-8")

    rows = leads._load_filter_lists()

    assert [str(row.get("id") or "") for row in rows] == ["valid_1"]
    assert list((root / "lists").glob("broken.json.json_parse_error*.bak"))


def test_loading_filter_list_by_id_reads_only_requested_payload(monkeypatch, tmp_path: Path) -> None:
    _configure_filter_storage(monkeypatch, tmp_path)
    leads._save_filter_list({"id": "valid_1", "items": [{"username": "uno", "status": "PENDING"}], "export_alias": "demo"})
    leads._save_filter_list({"id": "valid_2", "items": [], "export_alias": "demo"})

    row = leads._load_filter_list_by_id("valid_1")

    assert row is not None
    assert row["id"] == "valid_1"
    assert row["items"][0]["username"] == "uno"
    assert str(row["_path"]).endswith("valid_1.json")


def test_filter_list_summaries_build_sqlite_index_from_existing_json(monkeypatch, tmp_path: Path) -> None:
    root = _configure_filter_storage(monkeypatch, tmp_path)
    payload_path = root / "lists" / "legacy_run.json"
    payload_path.write_text(
        """
        {
          "id": "legacy_run",
          "created_at": "2026-03-12T10:00:00Z",
          "source_list": "seed",
          "export_alias": "demo",
          "run": {"alias": "main"},
          "items": [
            {"username": "uno", "status": "QUALIFIED"},
            {"username": "dos", "status": "PENDING"}
          ]
        }
        """.strip(),
        encoding="utf-8",
    )

    rows = leads._load_filter_list_summaries()

    assert [str(row.get("id") or "") for row in rows] == ["legacy_run"]
    assert rows[0]["processed"] == 1
    assert rows[0]["pending"] == 1
    assert rows[0]["run"]["alias"] == "main"
    assert "items" not in rows[0]
    assert (root / "filters_index.sqlite3").exists()


def test_filter_list_summaries_reuse_sqlite_index_for_unchanged_payloads(monkeypatch, tmp_path: Path) -> None:
    _configure_filter_storage(monkeypatch, tmp_path)
    leads._save_filter_list(
        {
            "id": "cached_run",
            "created_at": "2026-03-12T10:00:00Z",
            "source_list": "seed",
            "export_alias": "demo",
            "run": {"alias": "main"},
            "items": [{"username": "uno", "status": "PENDING"}],
        }
    )

    first = leads._load_filter_list_summaries()
    assert [str(row.get("id") or "") for row in first] == ["cached_run"]

    def _unexpected_load(*args, **kwargs):  # noqa: ANN001, ANN002
        raise AssertionError("No deberia releer el JSON si el payload no cambio.")

    monkeypatch.setattr("core.leads_filter_store.load_json_file", _unexpected_load)

    second = leads._load_filter_list_summaries()

    assert [str(row.get("id") or "") for row in second] == ["cached_run"]
    assert second[0]["pending"] == 1


def test_runtime_state_updates_items_from_sqlite_without_rewriting_shadow_json(monkeypatch, tmp_path: Path) -> None:
    root = _configure_filter_storage(monkeypatch, tmp_path)
    leads._save_filter_list(
        {
            "id": "runtime_run",
            "created_at": "2026-03-12T10:00:00Z",
            "export_alias": "demo",
            "items": [
                {
                    "username": "uno",
                    "status": "PENDING",
                    "result": "",
                    "reason": "",
                    "account": "",
                    "updated_at": "",
                }
            ],
        }
    )
    shadow_path = root / "lists" / "runtime_run.json"
    shadow_before = json.loads(shadow_path.read_text(encoding="utf-8"))

    runtime_row = leads._load_filter_list_by_id("runtime_run")
    assert runtime_row is not None
    runtime_row["items"][0]["status"] = "QUALIFIED"
    runtime_row["items"][0]["result"] = "CALIFICA"
    runtime_row["items"][0]["reason"] = ""
    runtime_row["items"][0]["account"] = "CuentaA"
    runtime_row["items"][0]["updated_at"] = "2026-03-12T10:01:00Z"
    leads._refresh_list_stats(runtime_row)

    leads._save_filter_list_runtime_state(runtime_row, item_indexes=[0])

    shadow_after = json.loads(shadow_path.read_text(encoding="utf-8"))
    loaded = leads._load_filter_list_by_id("runtime_run")

    assert shadow_after == shadow_before
    assert loaded is not None
    assert loaded["items"][0]["status"] == "QUALIFIED"
    assert loaded["items"][0]["account"] == "CuentaA"
    assert loaded["processed"] == 1
    assert loaded["qualified"] == 1


def test_filter_config_roundtrip_recovers_from_corruption(monkeypatch, tmp_path: Path) -> None:
    root = _configure_filter_storage(monkeypatch, tmp_path)
    cfg = leads.LeadFilterConfig(
        classic=leads.ClassicFilterConfig(
            min_followers=100,
            min_posts=10,
            privacy="any",
            link_in_bio="any",
            include_keywords=[],
            exclude_keywords=[],
            language="any",
        ),
        text=leads.TextFilterConfig(enabled=False, criteria="", model_path="", state="disabled"),
        image=leads.ImageFilterConfig(enabled=False, prompt="", state="disabled"),
    )

    leads._save_filter_config(cfg)
    loaded = leads._load_filter_config()
    assert loaded is not None
    assert loaded.classic.min_followers == 100

    leads.FILTER_CONFIG_PATH.write_text("{bad json", encoding="utf-8")

    assert leads._load_filter_config() is None
    assert list(root.glob("filters_config.json.json_parse_error*.bak"))


def test_delete_filter_list_moves_payload_to_backup(monkeypatch, tmp_path: Path) -> None:
    root = _configure_filter_storage(monkeypatch, tmp_path)
    payload = {"id": "delete_me", "items": [], "export_alias": "demo"}
    leads._save_filter_list(payload)
    stored = leads._load_filter_lists()[0]
    assert [str(row.get("id") or "") for row in leads._load_filter_list_summaries()] == ["delete_me"]

    leads._delete_filter_list(stored)

    assert leads._load_filter_lists() == []
    assert leads._load_filter_list_summaries() == []
    assert list((root / "_deleted_lists").glob("delete_me.json.deleted.*.bak"))
