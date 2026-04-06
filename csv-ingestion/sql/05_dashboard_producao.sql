-- Dashboard de produção
-- Estas queries dependem de `fato_producao` populada.
-- Estrutura mínima sugerida:
-- fato_producao(id_producao, id_venda, id_produto_sk, data_inicio, data_fim, tempo_fabricacao_horas, retrabalho, atraso_dias)

-- 1) Quanto tempo leva para produzir cada tipo?
SELECT
    dp.tipo_produto,
    COUNT(*) AS total_ordens,
    ROUND(AVG(fp.tempo_fabricacao_horas), 2) AS tempo_medio_horas,
    MIN(fp.tempo_fabricacao_horas) AS menor_tempo_horas,
    MAX(fp.tempo_fabricacao_horas) AS maior_tempo_horas,
    ROUND(AVG(fp.atraso_dias), 2) AS atraso_medio_dias,
    SUM(CASE WHEN fp.retrabalho THEN 1 ELSE 0 END) AS total_retrabalho
FROM fato_producao fp
JOIN dim_produto dp ON dp.id_produto_sk = fp.id_produto_sk
GROUP BY dp.tipo_produto
ORDER BY tempo_medio_horas DESC;
