-- Dashboard de vendas
-- Perguntas do escopo:
-- 1. Quais tipos de móveis vendem mais?
-- 2. Qual categoria gera mais receita?
-- 3. Qual é o ticket médio por cliente?

-- 1) Tipos de móveis que vendem mais
SELECT
    tipo_produto,
    SUM(quantidade) AS total_unidades,
    COUNT(*) AS total_pedidos,
    SUM(valor_total) AS receita_total,
    SUM(margem_lucro) AS lucro_total
FROM vw_base_vendas
GROUP BY tipo_produto
ORDER BY total_unidades DESC, receita_total DESC;

-- 2) Categoria que gera mais receita
SELECT
    categoria,
    COUNT(*) AS total_pedidos,
    SUM(quantidade) AS total_unidades,
    SUM(valor_total) AS receita_total,
    SUM(margem_lucro) AS lucro_total,
    ROUND(AVG(valor_total), 2) AS ticket_medio_categoria
FROM vw_base_vendas
GROUP BY categoria
ORDER BY receita_total DESC;

-- 3) Ticket médio por cliente
SELECT
    id_cliente,
    cidade,
    estado,
    tipo_cliente,
    COUNT(*) AS total_pedidos,
    SUM(valor_total) AS receita_total,
    ROUND(AVG(valor_total), 2) AS ticket_medio_cliente,
    SUM(margem_lucro) AS lucro_total_cliente
FROM vw_base_vendas
GROUP BY id_cliente, cidade, estado, tipo_cliente
ORDER BY ticket_medio_cliente DESC, receita_total DESC;
