# Início Rápido — BI Marcenaria

Este guia coloca o projeto para rodar com ETL + PostgreSQL + Metabase.

## Pré-requisitos

- Docker Desktop ativo
- Docker Compose ativo
- Arquivo [.env](.env) preenchido

## 1) Subir banco, ETL e Metabase

Na raiz do projeto, execute:

```bash
docker compose up -d --build
```

O que acontece:
- `postgres` sobe o banco
- `etl` processa os arquivos `vendas_*.csv`
- `metabase` sobe a camada de dashboard

## 2) Validar execução da ETL

Ver logs da ETL:

```bash
docker compose logs -f etl
```

Ver logs do banco:

```bash
docker compose logs -f postgres
```

## 3) Acessar Metabase

Abra:
- http://localhost:3000

No primeiro acesso, configure conexão PostgreSQL com os dados do [.env](.env):
- Host: `postgres` (dentro do compose)
- Porta: `5432`
- Banco: `POSTGRES_DB`
- Usuário: `POSTGRES_USER`
- Senha: `POSTGRES_PASSWORD`

## 4) Fontes para dashboards

Use estas views:
- `vw_base_vendas`
- `vw_kpi_vendas_por_tipo`
- `vw_kpi_receita_por_categoria`
- `vw_kpi_ticket_medio_cliente`

As queries do escopo estão em [csv-ingestion/sql](csv-ingestion/sql).

## 5) Subir novamente após mudanças

```bash
docker compose up -d --build etl
```

Se mudar SQL/modelo e quiser recriar tudo:

```bash
docker compose down -v
docker compose up -d --build
```

## 6) Execução local sem Docker (opcional)

Com ambiente virtual ativo:

```bash
python csv-ingestion/main.py
```

## 7) Documentos úteis

- Escopo: [Escopo.md](Escopo.md)
- Instruções funcionais: [INSTRUCOES.md](INSTRUCOES.md)
- Setup Metabase: [METABASE.md](METABASE.md)
