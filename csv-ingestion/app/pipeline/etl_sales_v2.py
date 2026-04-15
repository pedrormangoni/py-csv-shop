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
from app.api.upload import (
    get_read_directory,
    get_unread_csv_files,
    move_file_to_read,
    validate_columns_file,
)
from app.pipeline.schema_v2 import create_analytical_model


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
                id_cliente BIGINT NOT NULL,
                id_produto BIGINT NOT NULL,
                data_venda DATE NOT NULL,
                id_origem INTEGER NOT NULL,
                origem TEXT NOT NULL,
                tipo_produto TEXT NOT NULL,
                categoria TEXT NOT NULL,
                material TEXT NOT NULL,
                acabamento TEXT NOT NULL,
                medidas TEXT NOT NULL,
                quantidade_modulos INTEGER NOT NULL,
                adicionais TEXT NOT NULL,
                valor_total NUMERIC(14, 2) NOT NULL,
                custo_total NUMERIC(14, 2) NOT NULL,
                margem_lucro NUMERIC(14, 2) NOT NULL,
                status TEXT NOT NULL,
                quantidade INTEGER NOT NULL,
                pagina TEXT NOT NULL,
                tempo_permanencia INTEGER NOT NULL,
                acao TEXT NOT NULL,
                cidade TEXT NOT NULL,
                estado TEXT NOT NULL,
                tipo_cliente TEXT NOT NULL,
                source_file_name TEXT NOT NULL,
                loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )

        # Evolução de esquema para bases antigas
        alter_statements = [
            f"ALTER TABLE {STAGING_TABLE} ADD COLUMN IF NOT EXISTS id_produto BIGINT",
            f"ALTER TABLE {STAGING_TABLE} ADD COLUMN IF NOT EXISTS id_origem INTEGER",
            f"ALTER TABLE {STAGING_TABLE} ADD COLUMN IF NOT EXISTS acabamento TEXT",
            f"ALTER TABLE {STAGING_TABLE} ADD COLUMN IF NOT EXISTS medidas TEXT",
            f"ALTER TABLE {STAGING_TABLE} ADD COLUMN IF NOT EXISTS quantidade_modulos INTEGER",
            f"ALTER TABLE {STAGING_TABLE} ADD COLUMN IF NOT EXISTS adicionais TEXT",
            f"ALTER TABLE {STAGING_TABLE} ADD COLUMN IF NOT EXISTS pagina TEXT",
            f"ALTER TABLE {STAGING_TABLE} ADD COLUMN IF NOT EXISTS tempo_permanencia INTEGER",
            f"ALTER TABLE {STAGING_TABLE} ADD COLUMN IF NOT EXISTS acao TEXT",
            f"ALTER TABLE {STAGING_TABLE} ADD COLUMN IF NOT EXISTS tipo_produto TEXT",
        ]
        for statement in alter_statements:
            cur.execute(statement)


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


def _parse_non_negative_int(value: str, field_name: str) -> int:
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
    normalized = _normalize_text(value).lower()
    mapper = {
        "orcamento": "orçamento",
        "orçamento": "orçamento",
        "aprovado": "aprovado",
        "produzido": "produzido",
        "entregue": "entregue",
    }
    if normalized not in mapper:
        raise ValueError(f"Status inválido: {value}")
    return mapper[normalized]


def _normalize_action(value: str) -> str:
    normalized = _normalize_text(value).lower()
    mapper = {
        "visualizar": "visualizacao",
        "visualizacao": "visualizacao",
        "orcamento": "orcamento",
        "orçamento": "orcamento",
        "comprar": "compra",
        "compra": "compra",
        "abandonar": "abandono",
        "abandono": "abandono",
    }
    return mapper.get(normalized, normalized)


def _normalize_page(value: str) -> str:
    return _normalize_text(value).lower().replace(" ", "_")


def _hash_row(row: dict) -> str:
    raw = "|".join((row.get(column, "") or "").strip() for column in EXPECTED_COLUMNS)
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
            id_venda = _parse_non_negative_int(row["id_venda"], "id_venda")
            id_cliente = _parse_non_negative_int(row["id_cliente"], "id_cliente")
            id_produto = _parse_non_negative_int(row["id_produto"], "id_produto")
            id_origem = _parse_non_negative_int(row["id_origem"], "id_origem")
            quantidade = _parse_non_negative_int(row["quantidade"], "quantidade")
            qtd_modulos = _parse_non_negative_int(row["quantidade_modulos"], "quantidade_modulos")
            tempo = _parse_non_negative_int(row["tempo_permanencia"], "tempo_permanencia")

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
                    "id_cliente": id_cliente,
                    "id_produto": id_produto,
                    "data_venda": _parse_sale_date(row["data"]),
                    "id_origem": id_origem,
                    "origem": _normalize_text(row["origem"]),
                    "tipo_produto": _normalize_text(row["tipo_movel"]).lower(),
                    "categoria": _normalize_text(row["categoria"]).lower(),
                    "material": _normalize_text(row["material"]),
                    "acabamento": _normalize_text(row["acabamento"]),
                    "medidas": _normalize_text(row["medidas"]),
                    "quantidade_modulos": qtd_modulos,
                    "adicionais": _normalize_text(row["adicionais"]),
                    "valor_total": valor_total,
                    "custo_total": custo_total,
                    "margem_lucro": margem_lucro,
                    "status": _normalize_status(row["status"]),
                    "quantidade": quantidade,
                    "pagina": _normalize_page(row["pagina"]),
                    "tempo_permanencia": tempo,
                    "acao": _normalize_action(row["acao"]),
                    "cidade": _normalize_text(row["cidade"]),
                    "estado": _normalize_text(row["estado"]).upper(),
                    "tipo_cliente": _normalize_text(row["tipo_cliente"]).lower(),
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
                    id_cliente,
                    id_produto,
                    data_venda,
                    id_origem,
                    origem,
                    tipo_produto,
                    categoria,
                    material,
                    acabamento,
                    medidas,
                    quantidade_modulos,
                    adicionais,
                    valor_total,
                    custo_total,
                    margem_lucro,
                    status,
                    quantidade,
                    pagina,
                    tempo_permanencia,
                    acao,
                    cidade,
                    estado,
                    tipo_cliente,
                    source_file_name
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT DO NOTHING;
                """,
                (
                    batch_id,
                    row["row_number"],
                    row["row_hash"],
                    row["id_venda"],
                    row["id_cliente"],
                    row["id_produto"],
                    row["data_venda"],
                    row["id_origem"],
                    row["origem"],
                    row["tipo_produto"],
                    row["categoria"],
                    row["material"],
                    row["acabamento"],
                    row["medidas"],
                    row["quantidade_modulos"],
                    row["adicionais"],
                    row["valor_total"],
                    row["custo_total"],
                    row["margem_lucro"],
                    row["status"],
                    row["quantidade"],
                    row["pagina"],
                    row["tempo_permanencia"],
                    row["acao"],
                    row["cidade"],
                    row["estado"],
                    row["tipo_cliente"],
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


def run_etl_from_read():
    read_dir = get_read_directory()
    return _run_etl_for_files(sorted(read_dir.glob("*.csv")), move_after_load=False)


def executar_etl(diretorio_csv: str = None):
    if diretorio_csv:
        csv_dir = Path(diretorio_csv)
        files = sorted(csv_dir.glob("*.csv"))
        return _run_etl_for_files(files, move_after_load=False)

    fixed_files = _get_fixed_csv_files()
    if fixed_files:
        return _run_etl_for_files(fixed_files, move_after_load=False)

    unread_files = get_unread_csv_files()
    if unread_files:
        return _run_etl_for_files(unread_files)

    read_dir = get_read_directory()
    read_files = sorted(read_dir.glob("*.csv"))
    if read_files:
        return _run_etl_for_files(read_files, move_after_load=False)

    return run_etl_from_unread()


if __name__ == "__main__":
    executar_etl()
