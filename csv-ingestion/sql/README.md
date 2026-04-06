# SQL do escopo de BI

Esta pasta centraliza as queries SQL alinhadas às perguntas do escopo do projeto.

## Arquivos

- `00_kpis_executivos.sql`: KPIs gerais de receita, lucro, ticket médio e volume.
- `01_dashboard_vendas.sql`: perguntas do dashboard de vendas.
- `02_dashboard_comportamento_cliente.sql`: perguntas do dashboard de comportamento do cliente.
- `03_dashboard_financeiro.sql`: perguntas do dashboard financeiro.
- `04_dashboard_personalizacao.sql`: perguntas do dashboard de itens personalizados.
- `05_dashboard_producao.sql`: perguntas do dashboard de produção.

## Status de uso

### Executáveis com a ETL atual
- `00_kpis_executivos.sql`
- `01_dashboard_vendas.sql`
- `03_dashboard_financeiro.sql`

Essas queries usam a view [vw_base_vendas](../app/pipeline/schema.py) criada a partir da ETL atual.

### Dependem de dados adicionais
- `02_dashboard_comportamento_cliente.sql`
- `04_dashboard_personalizacao.sql`
- `05_dashboard_producao.sql`

Essas consultas assumem a existência e carga das tabelas abaixo:
- `fato_navegacao`
- `fato_producao`
- `dim_personalizacao` populada com dados reais

## Convenções

- Banco alvo: PostgreSQL
- Camada principal de leitura atual: `vw_base_vendas`
- Todas as queries foram separadas por pergunta de negócio, com comentários para facilitar uso no Power BI e em validações analíticas.
