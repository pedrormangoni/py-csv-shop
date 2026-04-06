-- Dashboard financeiro
-- Perguntas do escopo:
-- 1. Quais produtos têm maior margem de lucro?
-- 2. Existe diferença de lucro entre planejado e modulado?
-- 3. Qual período do mês tem mais vendas?

-- 1) Produtos com maior margem de lucro
SELECT
    tipo_produto,
    categoria,
    material,
    COUNT(*) AS total_vendas,
    SUM(valor_total) AS receita_total,
    SUM(custo_total) AS custo_total,
    SUM(margem_lucro) AS lucro_total,
    ROUND(AVG(margem_lucro), 2) AS margem_media_por_venda,
    ROUND(AVG((margem_lucro / NULLIF(valor_total, 0)) * 100), 2) AS margem_percentual_media
FROM vw_base_vendas
GROUP BY tipo_produto, categoria, material
ORDER BY lucro_total DESC, margem_percentual_media DESC;

-- 2) Diferença de lucro entre planejado e modulado
SELECT
    tipo_produto,
    COUNT(*) AS total_vendas,
    SUM(valor_total) AS receita_total,
    SUM(custo_total) AS custo_total,
    SUM(margem_lucro) AS lucro_total,
    ROUND(AVG(margem_lucro), 2) AS lucro_medio,
    ROUND(AVG((margem_lucro / NULLIF(valor_total, 0)) * 100), 2) AS margem_percentual_media
FROM vw_base_vendas
WHERE tipo_produto IN ('planejado', 'modulado')
GROUP BY tipo_produto
ORDER BY lucro_total DESC;

-- 3) Período do mês com mais vendas
SELECT
    CASE
        WHEN EXTRACT(DAY FROM data) BETWEEN 1 AND 10 THEN 'inicio_do_mes'
        WHEN EXTRACT(DAY FROM data) BETWEEN 11 AND 20 THEN 'meio_do_mes'
        ELSE 'fim_do_mes'
    END AS periodo_mes,
    COUNT(*) AS total_vendas,
    SUM(valor_total) AS receita_total,
    SUM(margem_lucro) AS lucro_total,
    ROUND(AVG(valor_total), 2) AS ticket_medio
FROM vw_base_vendas
GROUP BY 1
ORDER BY total_vendas DESC, receita_total DESC;
