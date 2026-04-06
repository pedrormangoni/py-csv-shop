import csv
import hashlib
import os
import re
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import psycopg2
from psycopg2 import OperationalError

from app.api.config import (
    EXPECTED_COLUMNS,
    FILE_DELIMITER,
    FIXED_SALES_FILES_GLOB,
    STAGING_TRANSACTIONS_TABLE,
)
from app.api.upload import get_unread_csv_files, move_file_to_read, validate_columns_file
from app.pipeline.schema import create_analytical_model


def _safe_sql_identifier(value: str) -> str:
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", value or ""):
        raise ValueError(f"Identificador SQL inválido: {value}")
    return value


STAGING_TABLE = _safe_sql_identifier(STAGING_TRANSACTIONS_TABLE)


def _load_env_file() -> None:
    env_path = Path(__file__).resolve().parents[3] / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ[key.strip()] = value.strip()


def _get_db_connection():
    _load_env_file()
    host = os.getenv("POSTGRES_HOST", "localhost")
    password = os.getenv("POSTGRES_PASSWORD", "")
    db_config = {
        "host": host,
        "database": os.getenv("POSTGRES_DB", "postgres"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
    }
    if password:
        db_config["password"] = password

    max_attempts = 20
    retry_seconds = 2
    last_error = None

    for _ in range(max_attempts):
        try:
            return psycopg2.connect(**db_config)
        except (OperationalError, UnicodeDecodeError) as exc:
            last_error = exc
            time.sleep(retry_seconds)

    if host != "localhost":
        db_config["host"] = "localhost"
        for _ in range(3):
            try:
                return psycopg2.connect(**db_config)
            except (OperationalError, UnicodeDecodeError) as exc:
                last_error = exc
                time.sleep(retry_seconds)

    if last_error is not None:
        if isinstance(last_error, UnicodeDecodeError):
            raise RuntimeError(
                "Falha ao conectar no PostgreSQL local. Verifique POSTGRES_USER/POSTGRES_PASSWORD/POSTGRES_DB no .env."
            ) from last_error
        raise last_error

    raise RuntimeError("Falha inesperada ao conectar no banco PostgreSQL")


def _ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS etl_file_batches (
                id BIGSERIAL PRIMARY KEY,
                file_name TEXT NOT NULL,
                file_hash TEXT NOT NULL UNIQUE,
                file_size BIGINT NOT NULL,
                status TEXT NOT NULL,
                rows_read INTEGER NOT NULL DEFAULT 0,
                rows_loaded INTEGER NOT NULL DEFAULT 0,
                error_message TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMPTZ
            );
            """
        )

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {STAGING_TABLE} (
                id BIGSERIAL PRIMARY KEY,
                batch_id BIGINT NOT NULL REFERENCES etl_file_batches(id),
                row_number INTEGER NOT NULL,
                row_hash TEXT NOT NULL,
                id_venda BIGINT NOT NULL,
                data_venda DATE NOT NULL,
                id_cliente BIGINT NOT NULL,
                cidade TEXT NOT NULL,
                estado TEXT NOT NULL,
                tipo_cliente TEXT NOT NULL,
                tipo_produto TEXT NOT NULL,
                categoria TEXT NOT NULL,
                material TEXT NOT NULL,
                origem TEXT NOT NULL,
                valor_total NUMERIC(14, 2) NOT NULL,
                custo_total NUMERIC(14, 2) NOT NULL,
                margem_lucro NUMERIC(14, 2) NOT NULL,
                quantidade INTEGER NOT NULL,
                status TEXT NOT NULL,
                source_file_name TEXT NOT NULL,
                loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(batch_id, row_hash),
                UNIQUE(source_file_name, id_venda)
            );
            """
        )


def _compute_file_hash(file_path: Path) -> str:
    sha = hashlib.sha256()
    with open(file_path, "rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(8192), b""):
            sha.update(chunk)
    return sha.hexdigest()


def _normalize_text(value: str) -> str:
    return " ".join((value or "").split())


def _parse_decimal(value: str) -> Decimal:
    normalized = (value or "").strip().replace(",", ".")
    if normalized == "":
        return Decimal("0")
    try:
        return Decimal(normalized)
    except InvalidOperation as exc:
        raise ValueError(f"Valor numérico inválido: {value}") from exc


def _parse_sale_date(value: str):
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"Data inválida: {value}. Formato esperado: YYYY-MM-DD") from exc


def _parse_positive_int(value: str, field_name: str) -> int:
    cleaned = (value or "").strip()
    if not cleaned:
        raise ValueError(f"Campo obrigatório vazio: {field_name}")

    try:
        parsed = int(cleaned)
    except ValueError as exc:
        raise ValueError(f"Valor inteiro inválido para {field_name}: {value}") from exc

    if parsed < 0:
        raise ValueError(f"Valor negativo inválido para {field_name}: {value}")

    return parsed


def _normalize_status(value: str) -> str:
    status = _normalize_text(value).lower()
    valid = {"orçamento", "orcamento", "aprovado", "produzido", "entregue"}
    if status not in valid:
        raise ValueError(f"Status inválido: {value}")
    if status == "orcamento":
        return "orçamento"
    return status


def _hash_row(row: dict) -> str:
    raw = "|".join(
        [
            row["id_venda"].strip(),
            row["data"].strip(),
            row["id_cliente"].strip(),
            _normalize_text(row["cidade"]),
            _normalize_text(row["estado"]),
            _normalize_text(row["tipo_cliente"]),
            _normalize_text(row["tipo_produto"]),
            _normalize_text(row["categoria"]),
            _normalize_text(row["material"]),
            _normalize_text(row["origem"]),
            row["valor_total"].strip(),
            row["custo_total"].strip(),
            row["margem_lucro"].strip(),
            row["quantidade"].strip(),
            _normalize_text(row["status"]),
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _upsert_batch_start(conn, file_path: Path, file_hash: str):
    file_name = file_path.name
    file_size = file_path.stat().st_size

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl_file_batches (file_name, file_hash, file_size, status, updated_at)
            VALUES (%s, %s, %s, 'processing', NOW())
            ON CONFLICT (file_hash)
            DO UPDATE SET
                updated_at = NOW(),
                error_message = NULL,
                status = CASE
                    WHEN etl_file_batches.status = 'loaded' THEN etl_file_batches.status
                    ELSE 'processing'
                END,
                file_name = EXCLUDED.file_name,
                file_size = EXCLUDED.file_size
            RETURNING id, status;
            """,
            (file_name, file_hash, file_size),
        )
        batch_id, status = cur.fetchone()

    return batch_id, status == "loaded"


def _parse_rows(file_path: Path) -> list[dict]:
    rows = []
    with open(file_path, "r", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file, delimiter=FILE_DELIMITER)
        if reader.fieldnames != EXPECTED_COLUMNS:
            raise ValueError(
                f"Cabeçalho inválido em {file_path.name}. Esperado: {EXPECTED_COLUMNS}; recebido: {reader.fieldnames}"
            )

        for row_number, row in enumerate(reader, start=2):
            id_venda = _parse_positive_int(row["id_venda"], "id_venda")
            id_cliente = _parse_positive_int(row["id_cliente"], "id_cliente")
            quantidade = _parse_positive_int(row["quantidade"], "quantidade")

            valor_total = _parse_decimal(row["valor_total"])
            custo_total = _parse_decimal(row["custo_total"])
            margem_lucro = _parse_decimal(row["margem_lucro"])

            margem_calculada = valor_total - custo_total
            if margem_lucro != margem_calculada:
                margem_lucro = margem_calculada

            rows.append(
                {
                    "row_number": row_number,
                    "row_hash": _hash_row(row),
                    "id_venda": id_venda,
                    "data_venda": _parse_sale_date(row["data"]),
                    "id_cliente": id_cliente,
                    "cidade": _normalize_text(row["cidade"]),
                    "estado": _normalize_text(row["estado"]).upper(),
                    "tipo_cliente": _normalize_text(row["tipo_cliente"]).upper(),
                    "tipo_produto": _normalize_text(row["tipo_produto"]).lower(),
                    "categoria": _normalize_text(row["categoria"]).lower(),
                    "material": _normalize_text(row["material"]).upper(),
                    "origem": _normalize_text(row["origem"]),
                    "valor_total": valor_total,
                    "custo_total": custo_total,
                    "margem_lucro": margem_lucro,
                    "quantidade": quantidade,
                    "status": _normalize_status(row["status"]),
                }
            )

    return rows


def _delete_previous_rows(conn, batch_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {STAGING_TABLE} WHERE batch_id = %s", (batch_id,))


def _insert_rows(conn, batch_id: int, source_file_name: str, rows: list[dict]) -> int:
    loaded = 0
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                f"""
                INSERT INTO {STAGING_TABLE} (
                    batch_id,
                    row_number,
                    row_hash,
                    id_venda,
                    data_venda,
                    id_cliente,
                    cidade,
                    estado,
                    tipo_cliente,
                    tipo_produto,
                    categoria,
                    material,
                    origem,
                    valor_total,
                    custo_total,
                    margem_lucro,
                    quantidade,
                    status,
                    source_file_name
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (source_file_name, id_venda) DO NOTHING;
                """,
                (
                    batch_id,
                    row["row_number"],
                    row["row_hash"],
                    row["id_venda"],
                    row["data_venda"],
                    row["id_cliente"],
                    row["cidade"],
                    row["estado"],
                    row["tipo_cliente"],
                    row["tipo_produto"],
                    row["categoria"],
                    row["material"],
                    row["origem"],
                    row["valor_total"],
                    row["custo_total"],
                    row["margem_lucro"],
                    row["quantidade"],
                    row["status"],
                    source_file_name,
                ),
            )
            loaded += cur.rowcount

    return loaded


def _mark_batch_loaded(conn, batch_id: int, rows_read: int, rows_loaded: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE etl_file_batches
            SET
                status = 'loaded',
                rows_read = %s,
                rows_loaded = %s,
                processed_at = NOW(),
                updated_at = NOW(),
                error_message = NULL
            WHERE id = %s;
            """,
            (rows_read, rows_loaded, batch_id),
        )


def _mark_batch_failed(conn, batch_id: int, message: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE etl_file_batches
            SET
                status = 'failed',
                error_message = %s,
                updated_at = NOW()
            WHERE id = %s;
            """,
            (message[:1000], batch_id),
        )


def _run_etl_for_files(files: list[Path], move_after_load: bool = True):
    summary = {
        "files_found": len(files),
        "files_loaded": 0,
        "files_invalid": 0,
        "files_skipped": 0,
        "files_failed": 0,
        "rows_loaded": 0,
    }

    if not files:
        return summary

    try:
        conn = _get_db_connection()
    except OperationalError as exc:
        raise RuntimeError(
            "Não foi possível conectar ao PostgreSQL. Verifique se o banco está ativo e as variáveis POSTGRES_* no arquivo .env."
        ) from exc

    try:
        _ensure_tables(conn)
        conn.commit()

        for file_path in files:
            batch_id = None
            try:
                if file_path.stat().st_size <= 0 or not validate_columns_file(file_path):
                    summary["files_invalid"] += 1
                    continue

                file_hash = _compute_file_hash(file_path)
                batch_id, already_loaded = _upsert_batch_start(conn, file_path, file_hash)
                conn.commit()

                if already_loaded:
                    if move_after_load:
                        move_file_to_read(file_path)
                    summary["files_skipped"] += 1
                    continue

                _delete_previous_rows(conn, batch_id)
                rows = _parse_rows(file_path)
                rows_loaded = _insert_rows(conn, batch_id, file_path.name, rows)
                _mark_batch_loaded(conn, batch_id, len(rows), rows_loaded)
                conn.commit()

                if move_after_load:
                    move_file_to_read(file_path)
                summary["files_loaded"] += 1
                summary["rows_loaded"] += rows_loaded

            except Exception as exc:
                conn.rollback()
                if batch_id is not None:
                    try:
                        _mark_batch_failed(conn, batch_id, str(exc))
                        conn.commit()
                    except Exception:
                        conn.rollback()
                summary["files_failed"] += 1

        create_analytical_model(conn, STAGING_TABLE)
    finally:
        conn.close()

    return summary


def _get_fixed_csv_files() -> list[Path]:
    project_root = Path(__file__).resolve().parents[3]
    return sorted(project_root.glob(FIXED_SALES_FILES_GLOB))


def run_etl_from_fixed_sales_csvs():
    return _run_etl_for_files(_get_fixed_csv_files(), move_after_load=False)


def run_etl_from_unread():
    return _run_etl_for_files(get_unread_csv_files())


def executar_etl(diretorio_csv: str = None):
    if diretorio_csv:
        csv_dir = Path(diretorio_csv)
        files = sorted(csv_dir.glob("*.csv"))
        return _run_etl_for_files(files, move_after_load=False)

    fixed_files = _get_fixed_csv_files()
    if fixed_files:
        return _run_etl_for_files(fixed_files, move_after_load=False)

    return run_etl_from_unread()


if __name__ == "__main__":
    executar_etl()
