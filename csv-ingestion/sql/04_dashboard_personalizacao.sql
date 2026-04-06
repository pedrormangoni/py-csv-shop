-- Dashboard de itens personalizados
-- Estas queries dependem de `dim_personalizacao` populada com dados reais
-- e de `fato_vendas.id_personalizacao_sk` relacionado corretamente.

-- 1) Quais configurações são mais escolhidas?
SELECT
    dpers.medidas,
    dpers.quantidade_modulos,
    dpers.adicionais,
    COUNT(*) AS total_vendas,
    SUM(fv.valor_total) AS receita_total,
    SUM(fv.margem_lucro) AS lucro_total
FROM fato_vendas fv
JOIN dim_personalizacao dpers ON dpers.id_personalizacao_sk = fv.id_personalizacao_sk
GROUP BY dpers.medidas, dpers.quantidade_modulos, dpers.adicionais
ORDER BY total_vendas DESC, receita_total DESC;

-- 2) Existe padrão de tamanho mais pedido?
SELECT
    dpers.medidas,
    COUNT(*) AS total_vendas,
    SUM(fv.quantidade) AS total_itens,
    SUM(fv.valor_total) AS receita_total
FROM fato_vendas fv
JOIN dim_personalizacao dpers ON dpers.id_personalizacao_sk = fv.id_personalizacao_sk
GROUP BY dpers.medidas
ORDER BY total_vendas DESC, total_itens DESC;

-- 3) Clientes preferem mais custo-benefício ou acabamento premium?
-- Suposição: `adicionais` ou atributos derivados indicam padrão premium.
SELECT
    CASE
        WHEN LOWER(dpers.adicionais) LIKE '%premium%' THEN 'premium'
        ELSE 'custo_beneficio'
    END AS perfil_escolha,
    COUNT(*) AS total_vendas,
    SUM(fv.valor_total) AS receita_total,
    SUM(fv.margem_lucro) AS lucro_total,
    ROUND(AVG(fv.valor_total), 2) AS ticket_medio
FROM fato_vendas fv
JOIN dim_personalizacao dpers ON dpers.id_personalizacao_sk = fv.id_personalizacao_sk
GROUP BY 1
ORDER BY total_vendas DESC, receita_total DESC;
