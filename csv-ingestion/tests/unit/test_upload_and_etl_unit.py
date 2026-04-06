from pathlib import Path
from decimal import Decimal

import pytest

from app.api.upload import EXPECTED_COLUMNS, validate_columns_file
from app.pipeline import etl


def _write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    lines = [";".join(header)] + [";".join(row) for row in rows]
    path.write_text("\n".join(lines), encoding="utf-8")


def test_validate_columns_file_returns_true_for_valid_header(tmp_path: Path):
    file_path = tmp_path / "ok.csv"
    _write_csv(
        file_path,
        EXPECTED_COLUMNS,
        [["01/01/2025", "Pedro", "1234", "Mercado", "Compra", "única", "0", "0", "100.50"]],
    )

    assert validate_columns_file(file_path) is True


def test_validate_columns_file_returns_false_for_invalid_header(tmp_path: Path):
    file_path = tmp_path / "bad.csv"
    bad_header = EXPECTED_COLUMNS.copy()
    bad_header[0] = "Data"
    _write_csv(file_path, bad_header, [])

    assert validate_columns_file(file_path) is False


def test_parse_decimal_handles_comma_and_empty():
    assert etl._parse_decimal("12,34") == Decimal("12.34")
    assert etl._parse_decimal("") == Decimal("0")


def test_parse_decimal_raises_for_invalid_value():
    with pytest.raises(ValueError):
        etl._parse_decimal("abc")


def test_parse_purchase_date_parses_expected_format():
    parsed = etl._parse_purchase_date("15/03/2026")
    assert parsed.year == 2026
    assert parsed.month == 3
    assert parsed.day == 15


def test_parse_purchase_date_raises_for_invalid_format():
    with pytest.raises(ValueError):
        etl._parse_purchase_date("2026-03-15")


def test_parse_installment_variants():
    assert etl._parse_installment("única") == ("única", None, None)
    assert etl._parse_installment("unica") == ("unica", None, None)
    assert etl._parse_installment("2/10") == ("2/10", 2, 10)
    assert etl._parse_installment("x/y") == ("x/y", None, None)


def test_hash_row_is_stable_for_same_content():
    row = {
        "Data de Compra": "15/03/2026",
        "Nome no Cartão": "Pedro",
        "Final do Cartão": "1234",
        "Categoria": "Mercado",
        "Descrição": "Compra do mês",
        "Parcela": "única",
        "Valor (em US$)": "0",
        "Cotação (em R$)": "0",
        "Valor (em R$)": "210.99",
    }

    assert etl._hash_row(row) == etl._hash_row(row)


def test_is_payment_row_identifies_negative_or_payment_description():
    assert etl._is_payment_row("Inclusao de Pagamento", Decimal("0"), Decimal("-100.00")) is True
    assert etl._is_payment_row("Compra normal", Decimal("-1.00"), Decimal("10.00")) is True
    assert etl._is_payment_row("Pagamento fatura", Decimal("0"), Decimal("0")) is True
    assert etl._is_payment_row("Supermercado", Decimal("0"), Decimal("150.00")) is False


def test_parse_rows_skips_payment_lines(tmp_path: Path):
    file_path = tmp_path / "fatura.csv"
    _write_csv(
        file_path,
        EXPECTED_COLUMNS,
        [
            ["01/01/2026", "Pedro", "1234", "Mercado", "Compra", "única", "0", "0", "100.00"],
            ["02/01/2026", "Pedro", "1234", "-", "Inclusao de Pagamento", "única", "0", "0", "-100.00"],
            ["03/01/2026", "Pedro", "1234", "Outros", "Pagamento manual", "única", "0", "0", "0"],
        ],
    )

    parsed = etl._parse_rows(file_path)

    assert len(parsed) == 1
    assert parsed[0]["description"] == "Compra"
