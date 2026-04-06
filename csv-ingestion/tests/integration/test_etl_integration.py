import shutil
import uuid
from pathlib import Path

import psycopg2
import pytest

from app.api.upload import EXPECTED_COLUMNS
from app.pipeline import etl


def _write_csv(path: Path, rows: list[list[str]]) -> None:
    lines = [";".join(EXPECTED_COLUMNS)] + [";".join(row) for row in rows]
    path.write_text("\n".join(lines), encoding="utf-8")


def _db_connection():
    return psycopg2.connect(
        host="localhost",
        port=5433,
        database="credit-card",
        user="postgres",
        password="postgres",
        connect_timeout=3,
    )


@pytest.fixture
def postgres_ready(monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    monkeypatch.setenv("POSTGRES_DB", "credit-card")
    monkeypatch.setenv("POSTGRES_USER", "postgres")
    monkeypatch.setenv("POSTGRES_PASSWORD", "postgres")

    try:
        conn = _db_connection()
        conn.close()
    except Exception:
        pytest.skip("PostgreSQL não disponível em localhost:5433")


@pytest.mark.integration
def test_run_etl_from_unread_loads_rows_and_moves_file(tmp_path: Path, monkeypatch, postgres_ready):
    unread_dir = tmp_path / "unread"
    read_dir = tmp_path / "read"
    unread_dir.mkdir(parents=True, exist_ok=True)
    read_dir.mkdir(parents=True, exist_ok=True)

    file_name = f"it_{uuid.uuid4().hex}.csv"
    csv_path = unread_dir / file_name
    _write_csv(
        csv_path,
        [
            ["01/02/2026", "Pedro", "1234", "Mercado", "Compra A", "única", "0", "0", "100.00"],
            ["02/02/2026", "Pedro", "1234", "Transporte", "Compra B", "2/3", "10", "5", "50.00"],
        ],
    )

    monkeypatch.setattr(etl, "get_unread_csv_files", lambda: [csv_path])
    monkeypatch.setattr(etl, "move_file_to_read", lambda file_path: shutil.move(str(file_path), str(read_dir / file_path.name)))

    summary = etl.run_etl_from_unread()

    assert summary["files_found"] == 1
    assert summary["files_loaded"] == 1
    assert summary["files_failed"] == 0
    assert summary["files_invalid"] == 0
    assert summary["files_skipped"] == 0
    assert summary["rows_loaded"] == 2
    assert (read_dir / file_name).exists()

    conn = _db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM stg_credit_card_transactions WHERE source_file_name = %s",
                (file_name,),
            )
            assert cur.fetchone()[0] == 2
    finally:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM stg_credit_card_transactions WHERE source_file_name = %s",
                (file_name,),
            )
            cur.execute("DELETE FROM etl_file_batches WHERE file_name = %s", (file_name,))
        conn.commit()
        conn.close()


@pytest.mark.integration
def test_run_etl_from_unread_skips_already_loaded_file(tmp_path: Path, monkeypatch, postgres_ready):
    unread_dir = tmp_path / "unread"
    read_dir = tmp_path / "read"
    unread_dir.mkdir(parents=True, exist_ok=True)
    read_dir.mkdir(parents=True, exist_ok=True)

    file_name = f"it_skip_{uuid.uuid4().hex}.csv"
    rows = [["03/02/2026", "Pedro", "1234", "Mercado", "Compra C", "única", "0", "0", "80.00"]]

    first_file = unread_dir / file_name
    _write_csv(first_file, rows)

    monkeypatch.setattr(etl, "move_file_to_read", lambda file_path: shutil.move(str(file_path), str(read_dir / file_path.name)))

    monkeypatch.setattr(etl, "get_unread_csv_files", lambda: [first_file])
    first_summary = etl.run_etl_from_unread()
    assert first_summary["files_loaded"] == 1

    second_file = unread_dir / file_name
    _write_csv(second_file, rows)

    monkeypatch.setattr(etl, "get_unread_csv_files", lambda: [second_file])
    second_summary = etl.run_etl_from_unread()

    assert second_summary["files_found"] == 1
    assert second_summary["files_loaded"] == 0
    assert second_summary["files_skipped"] == 1
    assert second_summary["files_failed"] == 0

    conn = _db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM stg_credit_card_transactions WHERE source_file_name = %s",
                (file_name,),
            )
            assert cur.fetchone()[0] == 1
    finally:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM stg_credit_card_transactions WHERE source_file_name = %s",
                (file_name,),
            )
            cur.execute("DELETE FROM etl_file_batches WHERE file_name = %s", (file_name,))
        conn.commit()
        conn.close()
