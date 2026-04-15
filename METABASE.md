# Configuração do Metabase

Este projeto já está preparado para usar Metabase com PostgreSQL via Docker Compose.

## Serviços configurados

- `postgres`: banco com os dados da ETL.
- `etl`: executa a carga dos CSVs e cria o modelo analítico v2.
- `metabase`: camada de dashboard.

Arquivo de orquestração: [docker-compose.yml](docker-compose.yml)

## Fluxo de uso

1. Subir os serviços do compose.
2. A ETL cria/atualiza as tabelas e views analíticas no PostgreSQL.
3. Acessar o Metabase em `http://localhost:3000`.
4. No primeiro acesso, conectar o Metabase ao PostgreSQL do projeto:
   - Tipo: PostgreSQL
   - Host: `postgres` (se dentro do compose) ou `localhost` (se acessar externamente)
   - Porta: `5432`
   - Banco: valor de `POSTGRES_DB` no `.env`
   - Usuário: valor de `POSTGRES_USER` no `.env`
   - Senha: valor de `POSTGRES_PASSWORD` no `.env`

## Mapa do modelo no banco (v2)

### Dimensões
- `dim_cliente`
- `dim_produto`
- `dim_tempo`
- `dim_origem`
- `dim_personalizacao`

### Fatos
- `fato_vendas`
- `fato_navegacao`

### Views analíticas
- `vw_base_vendas`
- `vw_funil_navegacao`
- `vw_kpi_vendas_por_tipo`
- `vw_kpi_receita_por_categoria`
- `vw_kpi_ticket_medio_cliente`

## Views recomendadas para dashboards no Metabase

Use estas fontes no Metabase:

- `vw_base_vendas`
- `vw_funil_navegacao`
- `vw_kpi_vendas_por_tipo`
- `vw_kpi_receita_por_categoria`
- `vw_kpi_ticket_medio_cliente`

As views são criadas pela ETL em [csv-ingestion/app/pipeline/schema_v2.py](csv-ingestion/app/pipeline/schema_v2.py).

## Perguntas do escopo prontas em SQL

As queries organizadas por dashboard estão em [csv-ingestion/sql](csv-ingestion/sql).

## Status dos dashboards com a base atual

- Vendas: pronto
- Comportamento do cliente: pronto
- Financeiro: pronto
- Itens personalizados: pronto
- Produção: depende de `fato_producao`

## Observação

No Metabase, após conectar o banco:
1. Clique em **Sync database schema now**.
2. Clique em **Re-scan field values now**.
3. Organize as perguntas por coleção usando os arquivos SQL em [csv-ingestion/sql](csv-ingestion/sql).
