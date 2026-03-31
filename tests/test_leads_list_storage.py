from __future__ import annotations

import time
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

import core.leads_store as leads_store_module
import core.storage_atomic as storage_atomic_module
from application.services.base import ServiceContext, ServiceError
from application.services.leads_service import LeadsService
from src.dm_campaign.proxy_workers_runner import load_leads


def _service(root: Path) -> LeadsService:
    return LeadsService(ServiceContext(root_dir=root))


def test_import_txt_appends_instead_of_replacing(tmp_path: Path) -> None:
    service = _service(tmp_path)
    service.save_list("demo", ["uno", "dos"])
    txt_path = tmp_path / "seed.txt"
    txt_path.write_text("tres\ncuatro\n", encoding="utf-8")

    service.import_txt(txt_path, "demo")

    assert service.load_list("demo") == ["uno", "dos", "tres", "cuatro"]


def test_import_empty_csv_is_blocked_and_records_failed_audit(tmp_path: Path) -> None:
    service = _service(tmp_path)
    csv_path = tmp_path / "empty.csv"
    csv_path.write_text("", encoding="utf-8")

    preview = service.preview_csv(csv_path, "demo")

    assert preview["valid_count"] == 0
    assert preview["sanity_state"] == "blocked"
    assert "No se detectaron usernames validos" in preview["blocking_reasons"][0]

    with pytest.raises(ServiceError, match="No se detectaron usernames validos"):
        service.import_csv(csv_path, "demo")

    assert service.list_lists() == []
    audit_entries = service.context.read_jsonl(service.context.storage_path("lead_imports", "audit.jsonl"))
    assert audit_entries[-1]["event"] == "import_failed"
    assert audit_entries[-1]["file_name"] == "empty.csv"
    assert service.import_status_snapshot()["metrics"]["failed_total"] == 1


def test_preview_csv_reports_new_duplicates_invalids_and_repeat_state(tmp_path: Path) -> None:
    service = _service(tmp_path)
    service.save_list("demo", ["uno"])
    csv_path = tmp_path / "seed.csv"
    csv_path.write_text("username\nuno\ndos\ndos\n@\n", encoding="utf-8")

    preview = service.preview_csv(csv_path, "demo")

    assert preview["valid_count"] == 2
    assert preview["new_count"] == 1
    assert preview["already_present_count"] == 1
    assert preview["duplicate_in_file_count"] == 1
    assert preview["blank_or_invalid_count"] == 1
    assert preview["same_file_import_count"] == 0
    assert preview["sanity_state"] == "warning"
    assert preview["sanity_messages"]

    service.import_csv(csv_path, "demo")

    second_preview = service.preview_csv(csv_path, "demo")

    assert second_preview["same_file_import_count"] == 1
    assert second_preview["new_count"] == 0


def test_preview_csv_blocks_multicolumn_fallback_that_looks_like_ids(tmp_path: Path) -> None:
    service = _service(tmp_path)
    csv_path = tmp_path / "danger.csv"
    csv_path.write_text("1,uno\n2,dos\n", encoding="utf-8")

    preview = service.preview_csv(csv_path, "demo")

    assert preview["sanity_state"] == "blocked"
    assert preview["used_first_column_fallback"] is True
    assert "IDs en la primera columna" in preview["blocking_reasons"][0]

    with pytest.raises(ServiceError, match="IDs en la primera columna"):
        service.import_csv(csv_path, "demo")


def test_add_manual_rewrites_without_persisting_duplicates(tmp_path: Path) -> None:
    service = _service(tmp_path)
    service.save_list("demo", ["uno"])

    service.add_manual("demo", ["uno", "@dos", "dos"])

    assert service.load_list("demo") == ["uno", "dos"]
    stored_lines = service.context.leads_path("demo.txt").read_text(encoding="utf-8").splitlines()
    assert stored_lines == ["uno", "dos"]


def test_import_large_txt_preserves_unique_order_and_campaign_compatibility(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _service(tmp_path)
    txt_path = tmp_path / "large.txt"
    total_unique = 2500
    rows: list[str] = []
    for index in range(total_unique):
        rows.append(f"@lead{index:04d}")
        rows.append(f"lead{index:04d}")
    rows.append("@")
    txt_path.write_text("\n".join(rows), encoding="utf-8")

    preview = service.preview_txt(txt_path, "demo")
    result = service.import_txt(txt_path, "demo")
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.load_list", lambda alias: service.load_list(alias))

    assert preview["row_count"] == len(rows)
    assert preview["valid_count"] == total_unique
    assert preview["duplicate_in_file_count"] == total_unique
    assert preview["blank_or_invalid_count"] == 1
    assert result["new_count"] == total_unique
    assert result["resulting_count"] == total_unique
    loaded = load_leads("demo")
    assert len(loaded) == total_unique
    assert loaded[0] == "lead0000"
    assert loaded[-1] == f"lead{total_unique - 1:04d}"


def test_add_manual_preserves_all_usernames_across_concurrent_updates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _service(tmp_path)
    service.save_list("demo", ["base"])
    original_atomic_write_text = leads_store_module.atomic_write_text

    def delayed_atomic_write_text(path: str | Path, text: str, *, encoding: str = "utf-8") -> Path:
        if Path(path).name == "demo.txt":
            time.sleep(0.2)
        return original_atomic_write_text(path, text, encoding=encoding)

    monkeypatch.setattr(leads_store_module, "atomic_write_text", delayed_atomic_write_text)

    payloads = (["alpha"], ["beta"], ["gamma"], ["delta"])

    def worker(values: list[str]) -> None:
        service.add_manual("demo", values)

    with ThreadPoolExecutor(max_workers=len(payloads)) as executor:
        futures = [executor.submit(worker, values) for values in payloads]
        for future in futures:
            future.result(timeout=10)

    result = service.load_list("demo")
    assert len(result) == 5
    assert result[0] == "base"
    assert set(result) == {"base", "alpha", "beta", "gamma", "delta"}


def test_add_manual_retries_transient_permission_error_during_atomic_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _service(tmp_path)
    service.save_list("demo", ["uno"])
    original_replace = storage_atomic_module.os.replace
    raised_once = {"value": False}

    def flaky_replace(src: str | Path, dst: str | Path) -> None:
        if not raised_once["value"] and Path(dst).name == "demo.txt":
            raised_once["value"] = True
            raise PermissionError("transient lock")
        original_replace(src, dst)

    monkeypatch.setattr(storage_atomic_module.os, "replace", flaky_replace)
    monkeypatch.setattr(storage_atomic_module.time, "sleep", lambda _seconds: None)

    service.add_manual("demo", ["dos"])

    assert raised_once["value"] is True
    assert service.load_list("demo") == ["uno", "dos"]


def test_invalid_list_name_is_rejected(tmp_path: Path) -> None:
    service = _service(tmp_path)

    with pytest.raises(ServiceError):
        service.save_list("..\\fuera", ["uno"])


@pytest.mark.parametrize("name", ["a*b", "a?b", 'a"b', "a|b", "a<b", "a>b"])
def test_invalid_windows_filename_characters_are_rejected(tmp_path: Path, name: str) -> None:
    service = _service(tmp_path)

    with pytest.raises(ServiceError, match="caracteres no permitidos"):
        service.save_list(name, ["uno"])


def test_load_list_normalizes_existing_dirty_file(tmp_path: Path) -> None:
    service = _service(tmp_path)
    service.context.leads_path("demo.txt").write_text("\ufeff@LeadUno\nleaduno\n  @leadDos\u200b \n", encoding="utf-8")

    assert service.load_list("demo") == ["LeadUno", "leadDos"]


def test_campaign_loader_reads_clean_unique_usernames(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = _service(tmp_path)
    service.save_list("demo", ["@uno", "uno", "dos"])
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.load_list", lambda alias: service.load_list(alias))

    assert load_leads("demo") == ["uno", "dos"]


<<<<<<< HEAD
def test_delete_list_removes_file_permanently(tmp_path: Path) -> None:
=======
def test_delete_list_moves_file_to_backup(tmp_path: Path) -> None:
>>>>>>> origin/main
    service = _service(tmp_path)
    service.save_list("demo", ["uno"])

    service.delete_list("demo")

    assert service.list_lists() == []
<<<<<<< HEAD
    assert not service.context.leads_path("demo.txt").exists()
    assert not list(service.context.leads_path("_deleted").glob("demo.txt.deleted.*.bak"))


def test_delete_list_cleans_import_audit_and_snapshots_for_same_name(tmp_path: Path) -> None:
    service = _service(tmp_path)
    csv_path = tmp_path / "seed.csv"
    csv_path.write_text("username\nuno\ndos\n", encoding="utf-8")

    service.import_csv(csv_path, "demo")

    audit_path = service.context.storage_path("lead_imports", "audit.jsonl")
    snapshots_dir = service.context.storage_path("lead_imports", "snapshots")
    assert service.context.read_jsonl(audit_path)
    assert list(snapshots_dir.glob("*.json"))

    service.delete_list("demo")

    assert service.context.read_jsonl(audit_path) == []
    assert not list(snapshots_dir.glob("*.json"))

    preview = service.preview_csv(csv_path, "demo")
    assert preview["same_file_import_count"] == 0
=======
    assert list(service.context.leads_path("_deleted").glob("demo.txt.deleted.*.bak"))
>>>>>>> origin/main


def test_list_summaries_reuse_persisted_counts_until_file_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _service(tmp_path)
    service.save_list("demo", ["uno", "uno", "dos"])

    assert service.list_list_summaries() == [{"name": "demo", "count": 2}]

    recounts = 0
    real_counter = leads_store_module.LeadListStore._count_usernames_from_path

    def _counting_counter(path: Path) -> int:
        nonlocal recounts
        recounts += 1
        return real_counter(path)

    reloaded = _service(tmp_path)
    monkeypatch.setattr(
        leads_store_module.LeadListStore,
        "_count_usernames_from_path",
        staticmethod(_counting_counter),
    )

    assert reloaded.list_list_summaries() == [{"name": "demo", "count": 2}]
    assert recounts == 0

    time.sleep(0.02)
    reloaded.context.leads_path("demo.txt").write_text("uno\ndos\ntres\n", encoding="utf-8")

    assert reloaded.list_list_summaries() == [{"name": "demo", "count": 3}]
    assert recounts == 1


def test_import_csv_writes_audit_log_and_returns_operational_counts(tmp_path: Path) -> None:
    service = _service(tmp_path)
    service.save_list("demo", ["uno"])
    csv_path = tmp_path / "seed.csv"
    csv_path.write_text("username\nuno\ndos\ndos\n", encoding="utf-8")

    result = service.import_csv(csv_path, "demo")

    assert result["kind"] == "csv"
    assert result["new_count"] == 1
    assert result["already_present_count"] == 1
    assert result["duplicate_in_file_count"] == 1
    assert result["resulting_count"] == 2
    audit_entries = service.context.read_jsonl(service.context.storage_path("lead_imports", "audit.jsonl"))
    assert audit_entries[-1]["event"] == "import_success"
    assert audit_entries[-1]["list_name"] == "demo"
    assert audit_entries[-1]["new_count"] == 1
    assert Path(str(audit_entries[-1]["snapshot_path"])).is_file()
    assert audit_entries[-1]["sanity_state"] == "warning"


def test_import_csv_rejects_exact_repeat_without_new_leads(tmp_path: Path) -> None:
    service = _service(tmp_path)
    csv_path = tmp_path / "seed.csv"
    csv_path.write_text("username\nuno\ndos\n", encoding="utf-8")

    service.import_csv(csv_path, "demo")

    with pytest.raises(ServiceError, match="ya fue importado"):
        service.import_csv(csv_path, "demo")

    snapshot = service.import_status_snapshot()

    assert snapshot["metrics"]["success_total"] == 1
    assert snapshot["metrics"]["failed_total"] == 1
    assert "7d: ok 1" in snapshot["summary"]


def test_rollback_last_import_restores_previous_list_state(tmp_path: Path) -> None:
    service = _service(tmp_path)
    service.save_list("demo", ["base"])
    csv_path = tmp_path / "seed.csv"
    csv_path.write_text("username\nuno\ndos\n", encoding="utf-8")

    service.import_csv(csv_path, "demo")

    result = service.rollback_last_import("demo")

    assert result["list_name"] == "demo"
    assert result["restored_count"] == 1
    assert service.load_list("demo") == ["base"]
    audit_entries = service.context.read_jsonl(service.context.storage_path("lead_imports", "audit.jsonl"))
    assert audit_entries[-1]["event"] == "import_rollback"
    assert audit_entries[-1]["rolled_back_import_id"]


def test_rollback_last_import_removes_newly_created_list(tmp_path: Path) -> None:
    service = _service(tmp_path)
    csv_path = tmp_path / "seed.csv"
    csv_path.write_text("username\nuno\ndos\n", encoding="utf-8")

    service.import_csv(csv_path, "demo")
    assert service.list_lists() == ["demo"]

    result = service.rollback_last_import("demo")

    assert result["restored_count"] == 0
    assert service.list_lists() == []
    assert service.load_list("demo") == []


def test_import_csv_rejects_same_list_while_other_import_is_in_progress(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _service(tmp_path)
    first_csv = tmp_path / "first.csv"
    second_csv = tmp_path / "second.csv"
    first_csv.write_text("username\nuno\n", encoding="utf-8")
    second_csv.write_text("username\ndos\n", encoding="utf-8")
    started = threading.Event()
    release = threading.Event()
    original_append = service._list_store.append

    def blocking_append(name: object, usernames) -> Path:
        if str(name) == "demo":
            started.set()
            assert release.wait(timeout=10)
        return original_append(name, usernames)

    monkeypatch.setattr(service._list_store, "append", blocking_append)

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(service.import_csv, first_csv, "demo")
        assert started.wait(timeout=10)
        with pytest.raises(ServiceError, match="importacion en curso"):
            service.import_csv(second_csv, "demo")
        release.set()
        future.result(timeout=10)

    assert service.load_list("demo") == ["uno"]


def test_save_list_wraps_storage_os_error_as_service_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _service(tmp_path)

    def broken_save(_name: object, _usernames) -> Path:
        raise OSError("disk failure")

    monkeypatch.setattr(service._list_store, "save", broken_save)

    with pytest.raises(ServiceError, match="No se pudo guardar la lista de leads"):
        service.save_list("demo", ["uno"])


def test_import_csv_wraps_storage_os_error_as_service_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _service(tmp_path)
    csv_path = tmp_path / "seed.csv"
    csv_path.write_text("username\nuno\n", encoding="utf-8")

    def broken_append(_name: object, _usernames) -> Path:
        raise OSError("disk failure")

    monkeypatch.setattr(service._list_store, "append", broken_append)

    with pytest.raises(ServiceError, match="No se pudo importar el CSV en la lista de leads"):
        service.import_csv(csv_path, "demo")
