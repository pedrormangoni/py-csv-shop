-- KPIs executivos do projeto
-- Base atual: vw_base_vendas

-- 1) Resumo geral do período
SELECT
    MIN(data) AS data_inicial,
    MAX(data) AS data_final,
    COUNT(*) AS total_vendas,
    COUNT(DISTINCT id_cliente) AS total_clientes,
    SUM(quantidade) AS total_itens,
    SUM(valor_total) AS receita_total,
    SUM(custo_total) AS custo_total,
    SUM(margem_lucro) AS lucro_total,
    ROUND(AVG(valor_total), 2) AS ticket_medio,
    ROUND(AVG(margem_lucro), 2) AS lucro_medio_por_venda
FROM vw_base_vendas;

-- 2) Receita mensal
SELECT
    ano,
    mes,
    SUM(valor_total) AS receita_total,
    SUM(margem_lucro) AS lucro_total,
    COUNT(*) AS total_vendas,
    ROUND(AVG(valor_total), 2) AS ticket_medio
FROM vw_base_vendas
GROUP BY ano, mes
ORDER BY ano, mes;

-- 3) Receita por origem
SELECT
    origem,
    COUNT(*) AS total_vendas,
    SUM(valor_total) AS receita_total,
    SUM(margem_lucro) AS lucro_total,
    ROUND(AVG(valor_total), 2) AS ticket_medio
FROM vw_base_vendas
GROUP BY origem
ORDER BY receita_total DESC;
