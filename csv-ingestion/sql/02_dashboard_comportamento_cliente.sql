-- Dashboard de comportamento do cliente
-- Estas queries dependem de `fato_navegacao` populada.
-- Estrutura mínima esperada:
-- fato_navegacao(id_evento, id_cliente, id_produto, id_tempo, pagina, tempo_permanencia, acao, origem)

-- 1) Em que etapa os clientes desistem (visualização -> orçamento -> compra)?
WITH funil AS (
    SELECT
        id_cliente,
        MAX(CASE WHEN acao = 'visualizacao' THEN 1 ELSE 0 END) AS viu_produto,
        MAX(CASE WHEN acao = 'orcamento' THEN 1 ELSE 0 END) AS gerou_orcamento,
        MAX(CASE WHEN acao = 'compra' THEN 1 ELSE 0 END) AS comprou
    FROM fato_navegacao
    GROUP BY id_cliente
)
SELECT
    SUM(viu_produto) AS clientes_visualizacao,
    SUM(CASE WHEN viu_produto = 1 AND gerou_orcamento = 1 THEN 1 ELSE 0 END) AS clientes_orcamento,
    SUM(CASE WHEN viu_produto = 1 AND comprou = 1 THEN 1 ELSE 0 END) AS clientes_compra,
    SUM(CASE WHEN viu_produto = 1 AND gerou_orcamento = 0 THEN 1 ELSE 0 END) AS abandono_antes_orcamento,
    SUM(CASE WHEN gerou_orcamento = 1 AND comprou = 0 THEN 1 ELSE 0 END) AS abandono_apos_orcamento
FROM funil;

-- 2) Quais produtos são mais visualizados mas pouco comprados?
WITH visualizacoes AS (
    SELECT
        id_produto,
        COUNT(*) AS total_visualizacoes
    FROM fato_navegacao
    WHERE acao = 'visualizacao'
    GROUP BY id_produto
),
compras AS (
    SELECT
        fv.id_produto_sk,
        COUNT(*) AS total_compras
    FROM fato_vendas fv
    GROUP BY fv.id_produto_sk
)
SELECT
    dp.tipo_produto,
    dp.categoria,
    dp.material,
    v.total_visualizacoes,
    COALESCE(c.total_compras, 0) AS total_compras,
    ROUND(v.total_visualizacoes::NUMERIC / NULLIF(COALESCE(c.total_compras, 0), 0), 2) AS relacao_view_compra
FROM visualizacoes v
JOIN dim_produto dp ON dp.id_produto_sk = v.id_produto
LEFT JOIN compras c ON c.id_produto_sk = dp.id_produto_sk
ORDER BY v.total_visualizacoes DESC, total_compras ASC;

-- 3) Qual origem traz mais clientes que realmente compram?
WITH clientes_que_compram AS (
    SELECT DISTINCT id_cliente
    FROM vw_base_vendas
)
SELECT
    fn.origem,
    COUNT(DISTINCT fn.id_cliente) AS clientes_navegaram,
    COUNT(DISTINCT CASE WHEN cqc.id_cliente IS NOT NULL THEN fn.id_cliente END) AS clientes_compradores,
    ROUND(
        COUNT(DISTINCT CASE WHEN cqc.id_cliente IS NOT NULL THEN fn.id_cliente END)::NUMERIC
        / NULLIF(COUNT(DISTINCT fn.id_cliente), 0) * 100,
        2
    ) AS taxa_conversao_percentual
FROM fato_navegacao fn
LEFT JOIN clientes_que_compram cqc ON cqc.id_cliente = fn.id_cliente
GROUP BY fn.origem
ORDER BY taxa_conversao_percentual DESC, clientes_compradores DESC;
