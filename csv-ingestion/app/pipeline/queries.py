"""
Consultas SQL alinhadas ao escopo de BI do projeto.

As consultas usam a camada semântica baseada em `vw_base_transacoes` e nas
views derivadas da pasta `sql/bi_views`.
"""

TOTAL_GASTO_PERIODO = """
SELECT
    MIN(purchase_date) AS data_inicial,
    MAX(purchase_date) AS data_final,
    COUNT(*) AS quantidade_compras,
    SUM(amount_brl) AS total_gasto_brl,
    SUM(amount_usd) AS total_gasto_usd
FROM vw_base_transacoes;
"""


EVOLUCAO_GASTOS_MENSAL = """
SELECT
    mes,
    quantidade_compras,
    total_gasto_brl,
    total_gasto_usd,
    ticket_medio_brl,
    total_gasto_brl - LAG(total_gasto_brl) OVER (ORDER BY mes) AS variacao_absoluta_brl,
    ROUND(
        ((total_gasto_brl - LAG(total_gasto_brl) OVER (ORDER BY mes))
        / NULLIF(LAG(total_gasto_brl) OVER (ORDER BY mes), 0)) * 100,
        2
    ) AS variacao_percentual_brl
FROM vw_gastos_mensais
ORDER BY mes;
"""


DISTRIBUICAO_GASTOS_CATEGORIA = """
SELECT
    category AS categoria,
    quantidade_compras,
    total_gasto_brl,
    total_gasto_usd,
    percentual_total_brl,
    RANK() OVER (ORDER BY total_gasto_brl DESC) AS ranking_categoria
FROM vw_gastos_categoria
ORDER BY total_gasto_brl DESC;
"""


TICKET_MEDIO_COMPRAS = """
SELECT
    COUNT(*) AS quantidade_compras,
    ROUND(AVG(amount_brl), 2) AS ticket_medio_brl,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount_brl), 2) AS mediana_brl,
    MIN(amount_brl) AS menor_compra_brl,
    MAX(amount_brl) AS maior_compra_brl
FROM vw_base_transacoes;
"""


IMPACTO_COMPRAS_PARCELADAS = """
SELECT
    tipo_compra,
    quantidade_compras,
    total_gasto_brl,
    ticket_medio_brl,
    ROUND(
        (total_gasto_brl / NULLIF(SUM(total_gasto_brl) OVER (), 0)) * 100,
        2
    ) AS percentual_do_total_brl
FROM vw_parcelamento
ORDER BY total_gasto_brl DESC;
"""


IMPACTO_COTACAO_DOLAR = """
SELECT
    mes,
    total_usd,
    cotacao_media,
    total_brl_real,
    total_brl_calculado,
    diferenca_brl
FROM vw_fx_impacto_mensal
ORDER BY mes;
"""


FREQUENCIA_CATEGORIA_TEMPO = """
SELECT
    mes,
    category AS categoria,
    frequencia_compras,
    total_gasto_brl,
    ticket_medio_brl,
    RANK() OVER (PARTITION BY mes ORDER BY frequencia_compras DESC, total_gasto_brl DESC) AS ranking_no_mes
FROM vw_frequencia_categoria_mensal
ORDER BY mes, ranking_no_mes;
"""


SEMANAS_MAIOR_VOLUME_MES = """
SELECT
    purchase_year,
    purchase_month_number,
    purchase_month,
    purchase_week,
    purchase_year_week,
    quantidade_compras,
    total_gasto_brl,
    ticket_medio_brl,
    ranking_semana_no_mes
FROM vw_gastos_semanais_mes
ORDER BY purchase_year, purchase_month_number, ranking_semana_no_mes, purchase_week;
"""


COMPRAS_RECORRENTES = """
SELECT
    descricao_base,
    categoria,
    nome_cartao,
    final_cartao,
    valor_medio_brl,
    quantidade_ocorrencias,
    primeira_compra,
    ultima_compra,
    media_dias_entre_compras,
    total_gasto_brl
FROM vw_compras_recorrentes
ORDER BY quantidade_ocorrencias DESC, total_gasto_brl DESC;
"""


RESUMO_KPIS = """
SELECT 'total_gasto_brl' AS indicador, SUM(amount_brl)::TEXT AS valor FROM vw_base_transacoes
UNION ALL
SELECT 'total_gasto_usd', SUM(amount_usd)::TEXT FROM vw_base_transacoes
UNION ALL
SELECT 'ticket_medio_brl', ROUND(AVG(amount_brl), 2)::TEXT FROM vw_base_transacoes
UNION ALL
SELECT 'qtd_compras', COUNT(*)::TEXT FROM vw_base_transacoes
UNION ALL
SELECT 'qtd_categorias', COUNT(DISTINCT category)::TEXT FROM vw_base_transacoes
UNION ALL
SELECT 'mes_maior_gasto', purchase_year_month
FROM vw_base_transacoes
GROUP BY purchase_year_month
ORDER BY SUM(amount_brl) DESC
LIMIT 1;
"""


CONSULTAS = {
    "total_gasto_periodo": {
        "descricao": "Valor total gasto no período analisado",
        "query": TOTAL_GASTO_PERIODO,
    },
    "evolucao_gastos_mensal": {
        "descricao": "Como os gastos evoluem ao longo do tempo por mês",
        "query": EVOLUCAO_GASTOS_MENSAL,
    },
    "distribuicao_gastos_categoria": {
        "descricao": "Distribuição e participação dos gastos por categoria",
        "query": DISTRIBUICAO_GASTOS_CATEGORIA,
    },
    "ticket_medio_compras": {
        "descricao": "Ticket médio, mediana e extremos das compras",
        "query": TICKET_MEDIO_COMPRAS,
    },
    "impacto_compras_parceladas": {
        "descricao": "Impacto das compras parceladas versus à vista",
        "query": IMPACTO_COMPRAS_PARCELADAS,
    },
    "impacto_cotacao_dolar": {
        "descricao": "Impacto da cotação do dólar no valor final em reais",
        "query": IMPACTO_COTACAO_DOLAR,
    },
    "frequencia_categoria_tempo": {
        "descricao": "Frequência de compra por categoria ao longo do tempo",
        "query": FREQUENCIA_CATEGORIA_TEMPO,
    },
    "semanas_maior_volume_mes": {
        "descricao": "Semanas com maior volume de gastos em cada mês",
        "query": SEMANAS_MAIOR_VOLUME_MES,
    },
    "compras_recorrentes": {
        "descricao": "Padrões de compras recorrentes",
        "query": COMPRAS_RECORRENTES,
    },
    "resumo_kpis": {
        "descricao": "Resumo executivo com os principais KPIs",
        "query": RESUMO_KPIS,
    },
}


def listar_consultas() -> None:
    """Lista as consultas disponíveis para exploração analítica."""
    print("\n" + "=" * 70)
    print("CONSULTAS SQL DISPONÍVEIS")
    print("=" * 70 + "\n")

    for chave, info in CONSULTAS.items():
        print(f"  • {chave}")
        print(f"    {info['descricao']}\n")

    print("=" * 70)


if __name__ == "__main__":
    listar_consultas()
