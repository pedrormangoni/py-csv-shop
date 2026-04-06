import os

THEME_NAME = os.getenv("DW_THEME_NAME", "marcenaria_sales")
STAGING_TRANSACTIONS_TABLE = os.getenv("DW_STAGING_TRANSACTIONS_TABLE", "stg_vendas")
SQL_BASE_VIEW_NAME = os.getenv("DW_SQL_BASE_VIEW_NAME", "vw_base_vendas")

EXPECTED_COLUMNS = [
    "id_venda",
    "data",
    "id_cliente",
    "cidade",
    "estado",
    "tipo_cliente",
    "tipo_produto",
    "categoria",
    "material",
    "origem",
    "valor_total",
    "custo_total",
    "margem_lucro",
    "quantidade",
    "status",
]

FILE_DELIMITER = ","

FIXED_SALES_FILES_GLOB = os.getenv("DW_FIXED_SALES_FILES_GLOB", "vendas_*.csv")