"""Modelo dimensional e views analíticas para BI de vendas de marcenaria."""

from __future__ import annotations


def create_analytical_model(conn, staging_table: str) -> None:
    """Cria dimensões, fato e views para consumo em BI."""

    sql_statements = [
        """
        CREATE TABLE IF NOT EXISTS dim_cliente (
            id_cliente_sk BIGSERIAL PRIMARY KEY,
            id_cliente BIGINT NOT NULL UNIQUE,
            cidade TEXT NOT NULL,
            estado TEXT NOT NULL,
            tipo_cliente TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_produto (
            id_produto_sk BIGSERIAL PRIMARY KEY,
            tipo_produto TEXT NOT NULL,
            categoria TEXT NOT NULL,
            material TEXT NOT NULL,
            UNIQUE(tipo_produto, categoria, material)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_tempo (
            id_tempo_sk INTEGER PRIMARY KEY,
            data DATE NOT NULL UNIQUE,
            mes INTEGER NOT NULL,
            ano INTEGER NOT NULL,
            trimestre INTEGER NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_origem (
            id_origem_sk BIGSERIAL PRIMARY KEY,
            origem TEXT NOT NULL UNIQUE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_personalizacao (
            id_personalizacao_sk BIGSERIAL PRIMARY KEY,
            medidas TEXT NOT NULL,
            quantidade_modulos INTEGER NOT NULL,
            adicionais TEXT NOT NULL,
            UNIQUE(medidas, quantidade_modulos, adicionais)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS fato_vendas (
            id BIGSERIAL PRIMARY KEY,
            id_venda BIGINT NOT NULL UNIQUE,
            id_cliente_sk BIGINT NOT NULL REFERENCES dim_cliente(id_cliente_sk),
            id_produto_sk BIGINT NOT NULL REFERENCES dim_produto(id_produto_sk),
            id_tempo_sk INTEGER NOT NULL REFERENCES dim_tempo(id_tempo_sk),
            id_origem_sk BIGINT NOT NULL REFERENCES dim_origem(id_origem_sk),
            id_personalizacao_sk BIGINT NOT NULL REFERENCES dim_personalizacao(id_personalizacao_sk),
            valor_total NUMERIC(14, 2) NOT NULL,
            custo_total NUMERIC(14, 2) NOT NULL,
            margem_lucro NUMERIC(14, 2) NOT NULL,
            status TEXT NOT NULL,
            quantidade INTEGER NOT NULL,
            source_file_name TEXT NOT NULL,
            loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        f"""
        INSERT INTO dim_cliente (id_cliente, cidade, estado, tipo_cliente)
        SELECT DISTINCT id_cliente, cidade, estado, tipo_cliente
        FROM {staging_table}
        ON CONFLICT (id_cliente) DO UPDATE SET
            cidade = EXCLUDED.cidade,
            estado = EXCLUDED.estado,
            tipo_cliente = EXCLUDED.tipo_cliente
        """,
        f"""
        INSERT INTO dim_produto (tipo_produto, categoria, material)
        SELECT DISTINCT tipo_produto, categoria, material
        FROM {staging_table}
        ON CONFLICT (tipo_produto, categoria, material) DO NOTHING
        """,
        f"""
        INSERT INTO dim_tempo (id_tempo_sk, data, mes, ano, trimestre)
        SELECT DISTINCT
            (EXTRACT(YEAR FROM data_venda)::INTEGER * 10000)
                + (EXTRACT(MONTH FROM data_venda)::INTEGER * 100)
                + EXTRACT(DAY FROM data_venda)::INTEGER AS id_tempo_sk,
            data_venda,
            EXTRACT(MONTH FROM data_venda)::INTEGER AS mes,
            EXTRACT(YEAR FROM data_venda)::INTEGER AS ano,
            EXTRACT(QUARTER FROM data_venda)::INTEGER AS trimestre
        FROM {staging_table}
        ON CONFLICT (id_tempo_sk) DO NOTHING
        """,
        f"""
        INSERT INTO dim_origem (origem)
        SELECT DISTINCT origem
        FROM {staging_table}
        ON CONFLICT (origem) DO NOTHING
        """,
        """
        INSERT INTO dim_personalizacao (medidas, quantidade_modulos, adicionais)
        VALUES ('N/A', 0, 'N/A')
        ON CONFLICT (medidas, quantidade_modulos, adicionais) DO NOTHING
        """,
        f"""
        INSERT INTO fato_vendas (
            id_venda,
            id_cliente_sk,
            id_produto_sk,
            id_tempo_sk,
            id_origem_sk,
            id_personalizacao_sk,
            valor_total,
            custo_total,
            margem_lucro,
            status,
            quantidade,
            source_file_name,
            loaded_at
        )
        SELECT
            stg.id_venda,
            dc.id_cliente_sk,
            dp.id_produto_sk,
            ((EXTRACT(YEAR FROM stg.data_venda)::INTEGER * 10000)
                + (EXTRACT(MONTH FROM stg.data_venda)::INTEGER * 100)
                + EXTRACT(DAY FROM stg.data_venda)::INTEGER) AS id_tempo_sk,
            dor.id_origem_sk,
            dper.id_personalizacao_sk,
            stg.valor_total,
            stg.custo_total,
            stg.margem_lucro,
            stg.status,
            stg.quantidade,
            stg.source_file_name,
            stg.loaded_at
        FROM {staging_table} stg
        JOIN dim_cliente dc ON dc.id_cliente = stg.id_cliente
        JOIN dim_produto dp ON (
            dp.tipo_produto = stg.tipo_produto
            AND dp.categoria = stg.categoria
            AND dp.material = stg.material
        )
        JOIN dim_origem dor ON dor.origem = stg.origem
        JOIN dim_personalizacao dper ON (
            dper.medidas = 'N/A'
            AND dper.quantidade_modulos = 0
            AND dper.adicionais = 'N/A'
        )
        ON CONFLICT (id_venda) DO UPDATE SET
            id_cliente_sk = EXCLUDED.id_cliente_sk,
            id_produto_sk = EXCLUDED.id_produto_sk,
            id_tempo_sk = EXCLUDED.id_tempo_sk,
            id_origem_sk = EXCLUDED.id_origem_sk,
            id_personalizacao_sk = EXCLUDED.id_personalizacao_sk,
            valor_total = EXCLUDED.valor_total,
            custo_total = EXCLUDED.custo_total,
            margem_lucro = EXCLUDED.margem_lucro,
            status = EXCLUDED.status,
            quantidade = EXCLUDED.quantidade,
            source_file_name = EXCLUDED.source_file_name,
            loaded_at = EXCLUDED.loaded_at
        """,
        """
        CREATE OR REPLACE VIEW vw_base_vendas AS
        SELECT
            fv.id_venda,
            dt.data,
            dt.mes,
            dt.ano,
            dt.trimestre,
            dc.id_cliente,
            dc.cidade,
            dc.estado,
            dc.tipo_cliente,
            dp.tipo_produto,
            dp.categoria,
            dp.material,
            dor.origem,
            fv.valor_total,
            fv.custo_total,
            fv.margem_lucro,
            fv.quantidade,
            fv.status,
            fv.source_file_name,
            fv.loaded_at
        FROM fato_vendas fv
        JOIN dim_cliente dc ON dc.id_cliente_sk = fv.id_cliente_sk
        JOIN dim_produto dp ON dp.id_produto_sk = fv.id_produto_sk
        JOIN dim_tempo dt ON dt.id_tempo_sk = fv.id_tempo_sk
        JOIN dim_origem dor ON dor.id_origem_sk = fv.id_origem_sk
        """,
        """
        CREATE OR REPLACE VIEW vw_kpi_vendas_por_tipo AS
        SELECT
            tipo_produto,
            SUM(quantidade) AS unidades,
            SUM(valor_total) AS receita_total,
            SUM(margem_lucro) AS lucro_total
        FROM vw_base_vendas
        GROUP BY tipo_produto
        ORDER BY receita_total DESC
        """,
        """
        CREATE OR REPLACE VIEW vw_kpi_receita_por_categoria AS
        SELECT
            categoria,
            SUM(valor_total) AS receita_total,
            SUM(margem_lucro) AS lucro_total
        FROM vw_base_vendas
        GROUP BY categoria
        ORDER BY receita_total DESC
        """,
        """
        CREATE OR REPLACE VIEW vw_kpi_ticket_medio_cliente AS
        SELECT
            id_cliente,
            ROUND(AVG(valor_total), 2) AS ticket_medio,
            SUM(valor_total) AS receita_total_cliente,
            COUNT(*) AS total_pedidos
        FROM vw_base_vendas
        GROUP BY id_cliente
        ORDER BY ticket_medio DESC
        """,
    ]

    cur = conn.cursor()
    try:
        for statement in sql_statements:
            cur.execute(statement)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
