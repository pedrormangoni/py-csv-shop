"""Modelo dimensional e views analíticas para o layout atualizado de vendas."""

from __future__ import annotations


def create_analytical_model(conn, staging_table: str) -> None:
    sql_statements = [
        "DROP VIEW IF EXISTS vw_kpi_ticket_medio_cliente CASCADE",
        "DROP VIEW IF EXISTS vw_kpi_receita_por_categoria CASCADE",
        "DROP VIEW IF EXISTS vw_kpi_vendas_por_tipo CASCADE",
        "DROP VIEW IF EXISTS vw_funil_navegacao CASCADE",
        "DROP VIEW IF EXISTS vw_base_vendas CASCADE",
        "DROP TABLE IF EXISTS fato_navegacao CASCADE",
        "DROP TABLE IF EXISTS fato_vendas CASCADE",
        "DROP TABLE IF EXISTS dim_personalizacao CASCADE",
        "DROP TABLE IF EXISTS dim_origem CASCADE",
        "DROP TABLE IF EXISTS dim_tempo CASCADE",
        "DROP TABLE IF EXISTS dim_produto CASCADE",
        "DROP TABLE IF EXISTS dim_cliente CASCADE",
        """
        CREATE TABLE dim_cliente (
            id_cliente_sk BIGSERIAL PRIMARY KEY,
            id_cliente BIGINT NOT NULL UNIQUE,
            cidade TEXT NOT NULL,
            estado TEXT NOT NULL,
            tipo_cliente TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE dim_produto (
            id_produto_sk BIGSERIAL PRIMARY KEY,
            id_produto BIGINT NOT NULL UNIQUE,
            tipo_produto TEXT NOT NULL,
            categoria TEXT NOT NULL,
            material TEXT NOT NULL,
            acabamento TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE dim_tempo (
            id_tempo_sk INTEGER PRIMARY KEY,
            data DATE NOT NULL UNIQUE,
            mes INTEGER NOT NULL,
            ano INTEGER NOT NULL,
            trimestre INTEGER NOT NULL
        )
        """,
        """
        CREATE TABLE dim_origem (
            id_origem_sk BIGSERIAL PRIMARY KEY,
            id_origem INTEGER NOT NULL UNIQUE,
            origem TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE dim_personalizacao (
            id_personalizacao_sk BIGSERIAL PRIMARY KEY,
            medidas TEXT NOT NULL,
            quantidade_modulos INTEGER NOT NULL,
            adicionais TEXT NOT NULL,
            UNIQUE(medidas, quantidade_modulos, adicionais)
        )
        """,
        """
        CREATE TABLE fato_vendas (
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
        """
        CREATE TABLE fato_navegacao (
            id_evento BIGSERIAL PRIMARY KEY,
            id_venda BIGINT NOT NULL,
            id_cliente BIGINT NOT NULL,
            id_produto BIGINT NOT NULL,
            id_tempo_sk INTEGER NOT NULL REFERENCES dim_tempo(id_tempo_sk),
            id_origem INTEGER NOT NULL,
            origem TEXT NOT NULL,
            pagina TEXT NOT NULL,
            tempo_permanencia INTEGER NOT NULL,
            acao TEXT NOT NULL,
            source_file_name TEXT NOT NULL,
            UNIQUE(id_venda)
        )
        """,
        f"""
        INSERT INTO dim_cliente (id_cliente, cidade, estado, tipo_cliente)
        SELECT DISTINCT ON (id_cliente) id_cliente, cidade, estado, tipo_cliente
        FROM {staging_table}
        WHERE id_cliente IS NOT NULL
            AND cidade IS NOT NULL
            AND estado IS NOT NULL
            AND tipo_cliente IS NOT NULL
        ORDER BY id_cliente, loaded_at DESC
        ON CONFLICT (id_cliente) DO UPDATE SET
            cidade = EXCLUDED.cidade,
            estado = EXCLUDED.estado,
            tipo_cliente = EXCLUDED.tipo_cliente
        """,
        f"""
        INSERT INTO dim_produto (id_produto, tipo_produto, categoria, material, acabamento)
        SELECT DISTINCT ON (id_produto) id_produto, tipo_produto, categoria, material, acabamento
        FROM {staging_table}
        WHERE id_produto IS NOT NULL
            AND tipo_produto IS NOT NULL
            AND categoria IS NOT NULL
            AND material IS NOT NULL
            AND acabamento IS NOT NULL
        ORDER BY id_produto, loaded_at DESC
        ON CONFLICT (id_produto) DO UPDATE SET
            tipo_produto = EXCLUDED.tipo_produto,
            categoria = EXCLUDED.categoria,
            material = EXCLUDED.material,
            acabamento = EXCLUDED.acabamento
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
        WHERE data_venda IS NOT NULL
        ON CONFLICT (id_tempo_sk) DO NOTHING
        """,
        f"""
        INSERT INTO dim_origem (id_origem, origem)
        SELECT DISTINCT ON (id_origem) id_origem, origem
        FROM {staging_table}
        WHERE id_origem IS NOT NULL AND origem IS NOT NULL
        ORDER BY id_origem, loaded_at DESC
        ON CONFLICT (id_origem) DO UPDATE SET
            origem = EXCLUDED.origem
        """,
        f"""
        INSERT INTO dim_personalizacao (medidas, quantidade_modulos, adicionais)
        SELECT DISTINCT medidas, quantidade_modulos, adicionais
        FROM {staging_table}
        WHERE medidas IS NOT NULL
            AND quantidade_modulos IS NOT NULL
            AND adicionais IS NOT NULL
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
        SELECT DISTINCT ON (stg.id_venda)
            stg.id_venda,
            dc.id_cliente_sk,
            dp.id_produto_sk,
            ((EXTRACT(YEAR FROM stg.data_venda)::INTEGER * 10000)
                + (EXTRACT(MONTH FROM stg.data_venda)::INTEGER * 100)
                + EXTRACT(DAY FROM stg.data_venda)::INTEGER),
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
        JOIN dim_produto dp ON dp.id_produto = stg.id_produto
        JOIN dim_origem dor ON dor.id_origem = stg.id_origem
        JOIN dim_personalizacao dper ON (
            dper.medidas = stg.medidas
            AND dper.quantidade_modulos = stg.quantidade_modulos
            AND dper.adicionais = stg.adicionais
        )
        WHERE stg.id_venda IS NOT NULL
            AND stg.id_cliente IS NOT NULL
            AND stg.id_produto IS NOT NULL
            AND stg.id_origem IS NOT NULL
            AND stg.data_venda IS NOT NULL
        ORDER BY stg.id_venda, stg.loaded_at DESC
        ON CONFLICT (id_venda) DO NOTHING
        """,
        f"""
        INSERT INTO fato_navegacao (
            id_venda,
            id_cliente,
            id_produto,
            id_tempo_sk,
            id_origem,
            origem,
            pagina,
            tempo_permanencia,
            acao,
            source_file_name
        )
        SELECT DISTINCT ON (id_venda)
            id_venda,
            id_cliente,
            id_produto,
            ((EXTRACT(YEAR FROM data_venda)::INTEGER * 10000)
                + (EXTRACT(MONTH FROM data_venda)::INTEGER * 100)
                + EXTRACT(DAY FROM data_venda)::INTEGER),
            id_origem,
            origem,
            pagina,
            tempo_permanencia,
            acao,
            source_file_name
        FROM {staging_table}
        WHERE id_venda IS NOT NULL
            AND id_cliente IS NOT NULL
            AND id_produto IS NOT NULL
            AND id_origem IS NOT NULL
            AND data_venda IS NOT NULL
            AND pagina IS NOT NULL
            AND tempo_permanencia IS NOT NULL
            AND acao IS NOT NULL
        ORDER BY id_venda, loaded_at DESC
        ON CONFLICT (id_venda) DO NOTHING
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
            dp.id_produto,
            dp.tipo_produto,
            dp.categoria,
            dp.material,
            dp.acabamento,
            dor.id_origem,
            dor.origem,
            dper.medidas,
            dper.quantidade_modulos,
            dper.adicionais,
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
        JOIN dim_personalizacao dper ON dper.id_personalizacao_sk = fv.id_personalizacao_sk
        """,
        """
        CREATE OR REPLACE VIEW vw_funil_navegacao AS
        SELECT
            id_cliente,
            SUM(CASE WHEN acao = 'visualizacao' THEN 1 ELSE 0 END) AS visualizacoes,
            SUM(CASE WHEN acao = 'orcamento' THEN 1 ELSE 0 END) AS orcamentos,
            SUM(CASE WHEN acao = 'compra' THEN 1 ELSE 0 END) AS compras,
            SUM(CASE WHEN acao = 'abandono' THEN 1 ELSE 0 END) AS abandonos
        FROM fato_navegacao
        GROUP BY id_cliente
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
