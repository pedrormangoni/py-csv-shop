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

from app.api.upload import (
    get_unread_csv_files,
    move_file_to_read,
    validate_columns_file,
)

from app.api.config import (
    EXPECTED_COLUMNS,
    FILE_DELIMITER,
    STAGING_TRANSACTIONS_TABLE,
)


def _safe_sql_identifier(value: str) -> str:
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", value or ""):
        raise ValueError(f"Identificador SQL inválido: {value}")
    return value


STAGING_TABLE = _safe_sql_identifier(STAGING_TRANSACTIONS_TABLE)


def _load_env_file():
    env_path = Path(__file__).resolve().parents[3] / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ[key.strip()] = value.strip()

# Função para conectar ao banco de dados
def _get_db_connection():
    # Coleta as configurações a partir das variáveis de ambiente, com fallback para valores padrão
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
                "Falha ao conectar no PostgreSQL local. Em Windows, esse erro costuma acontecer quando o servidor exige senha "
                "(ou usuário/senha estão incorretos). Verifique POSTGRES_USER/POSTGRES_PASSWORD/POSTGRES_DB no .env e a configuração "
                "de autenticação (pg_hba.conf)."
            ) from last_error
        raise last_error
    raise RuntimeError("Falha inesperada ao conectar no banco PostgreSQL")

# Função para criar as tabelas do Data Warehouse
def _ensure_tables(conn):
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
                purchase_date DATE NOT NULL,
                cardholder_name TEXT NOT NULL,
                card_last4 TEXT NOT NULL,
                category TEXT NOT NULL,
                description TEXT NOT NULL,
                installment_raw TEXT NOT NULL,
                installment_number INTEGER,
                installment_total INTEGER,
                amount_usd NUMERIC(14, 2) NOT NULL,
                fx_rate_brl NUMERIC(14, 6) NOT NULL,
                amount_brl NUMERIC(14, 2) NOT NULL,
                source_file_name TEXT NOT NULL,
                loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(batch_id, row_hash)
            );
            """
        )

# Função para calcular o hash do arquivo
def _compute_file_hash(file_path: Path) -> str:
    sha = hashlib.sha256()
    with open(file_path, "rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(8192), b""):
            sha.update(chunk)
    return sha.hexdigest()

# Função para normalizar texto (remover espaços extras)
def _normalize_text(value: str) -> str:
    return " ".join((value or "").split())

# Função para converter string numérica para Decimal, tratando vírgulas e pontos  
def _parse_decimal(value: str) -> Decimal:
    normalized = (value or "").strip().replace(",", ".")
    if normalized == "":
        return Decimal("0")
    try:
        return Decimal(normalized)
    except InvalidOperation as exc:
        raise ValueError(f"Valor numérico inválido: {value}") from exc

# Função para processar a data de compra no formato "dd/mm/yyyy"
def _parse_purchase_date(value: str):
    try:
        return datetime.strptime(value.strip(), "%d/%m/%Y").date()
    except ValueError as exc:
        raise ValueError(f"Data inválida: {value}") from exc

# Função para processar a parcela, identificando se é única ou múltipla (ex: "2/3")
def _parse_installment(value: str):
    cleaned = (value or "").strip()
    if cleaned.lower() in {"única", "unica"}:
        return cleaned, None, None

    if "/" in cleaned:
        left, right = cleaned.split("/", 1)
        left = left.strip()
        right = right.strip()
        if left.isdigit() and right.isdigit():
            return cleaned, int(left), int(right)

    return cleaned, None, None


def _is_payment_row(description: str, amount_usd: Decimal, amount_brl: Decimal) -> bool:
    if amount_brl < 0 or amount_usd < 0:
        return True

    normalized_description = _normalize_text(description).lower()
    return "pagamento" in normalized_description

# 
def _hash_row(row: dict) -> str:
    raw = "|".join(
        [
            row["Data de Compra"].strip(),
            _normalize_text(row["Nome no Cartão"]),
            row["Final do Cartão"].strip(),
            _normalize_text(row["Categoria"]),
            _normalize_text(row["Descrição"]),
            row["Parcela"].strip(),
            row["Valor (em US$)"].strip(),
            row["Cotação (em R$)"].strip(),
            row["Valor (em R$)"].strip(),
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


def _parse_rows(file_path: Path):
    rows = []
    with open(file_path, "r", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file, delimiter=FILE_DELIMITER)
        if reader.fieldnames != EXPECTED_COLUMNS:
            raise ValueError(
                f"Cabeçalho inválido em {file_path.name}. Esperado: {EXPECTED_COLUMNS}; recebido: {reader.fieldnames}"
            )

        for row_number, row in enumerate(reader, start=2):
            purchase_date = _parse_purchase_date(row["Data de Compra"])
            installment_raw, installment_number, installment_total = _parse_installment(
                row["Parcela"]
            )
            amount_usd = _parse_decimal(row["Valor (em US$)"])
            amount_brl = _parse_decimal(row["Valor (em R$)"])

            if _is_payment_row(row["Descrição"], amount_usd, amount_brl):
                continue

            rows.append(
                {
                    "row_number": row_number,
                    "row_hash": _hash_row(row),
                    "purchase_date": purchase_date,
                    "cardholder_name": _normalize_text(row["Nome no Cartão"]),
                    "card_last4": row["Final do Cartão"].strip(),
                    "category": _normalize_text(row["Categoria"]),
                    "description": _normalize_text(row["Descrição"]),
                    "installment_raw": installment_raw,
                    "installment_number": installment_number,
                    "installment_total": installment_total,
                    "amount_usd": amount_usd,
                    "fx_rate_brl": _parse_decimal(row["Cotação (em R$)"]),
                    "amount_brl": amount_brl,
                }
            )

    return rows


def _delete_previous_rows(conn, batch_id: int):
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {STAGING_TABLE} WHERE batch_id = %s", (batch_id,))


def _insert_rows(conn, batch_id: int, source_file_name: str, rows: list[dict]):
    loaded = 0
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                f"""
                INSERT INTO {STAGING_TABLE} (
                    batch_id,
                    row_number,
                    row_hash,
                    purchase_date,
                    cardholder_name,
                    card_last4,
                    category,
                    description,
                    installment_raw,
                    installment_number,
                    installment_total,
                    amount_usd,
                    fx_rate_brl,
                    amount_brl,
                    source_file_name
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (batch_id, row_hash) DO NOTHING;
                """,
                (
                    batch_id,
                    row["row_number"],
                    row["row_hash"],
                    row["purchase_date"],
                    row["cardholder_name"],
                    row["card_last4"],
                    row["category"],
                    row["description"],
                    row["installment_raw"],
                    row["installment_number"],
                    row["installment_total"],
                    row["amount_usd"],
                    row["fx_rate_brl"],
                    row["amount_brl"],
                    source_file_name,
                ),
            )
            loaded += cur.rowcount

    return loaded


def _mark_batch_loaded(conn, batch_id: int, rows_read: int, rows_loaded: int):
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


def _mark_batch_failed(conn, batch_id: int, message: str):
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
    finally:
        conn.close()

    return summary


def run_etl_from_unread():
    return _run_etl_for_files(get_unread_csv_files())


def executar_etl(diretorio_csv: str = None):
    if diretorio_csv:
        csv_dir = Path(diretorio_csv)
        files = sorted(csv_dir.glob("*.csv"))
        return _run_etl_for_files(files, move_after_load=False)
    return run_etl_from_unread()


if __name__ == "__main__":
    executar_etl()
