# Instruções do Projeto — BI para Vendas de Marcenaria

## 1) Objetivo
Construir uma solução de Business Intelligence para transformar dados operacionais do site e da produção em insights para:
- aumentar vendas,
- melhorar conversão,
- otimizar produção,
- maximizar lucratividade.

---

## 2) Problema de negócio
Hoje existe baixa visibilidade estratégica sobre:
- desempenho comercial,
- comportamento do cliente,
- eficiência produtiva.

Isso reduz a qualidade da tomada de decisão orientada a dados.

---

## 3) Escopo funcional
A solução deve coletar, armazenar e analisar dados de:
1. Usuários
2. Navegação
3. Produtos
4. Personalização
5. Vendas
6. Produção

Ao final de cada mês, gerar CSV consolidado e alimentar o Data Warehouse para análise no Power BI.

---

## 4) Fontes e dados mínimos

### 4.1 Usuários
- identificação
- localização (cidade/estado)
- tipo de cliente
- histórico de interação

### 4.2 Navegação
- páginas acessadas
- tempo de permanência
- produtos visualizados
- abandono de carrinho
- origem do tráfego

### 4.3 Produtos
- tipo de móvel (planejado, modulado, padrão)
- categoria
- material
- dimensões
- custo e preço

### 4.4 Personalização
- medidas
- acabamentos
- quantidade de módulos
- adicionais

### 4.5 Vendas
- pedidos
- data
- valor
- status
- tempo de fechamento
- margem de lucro

### 4.6 Produção
- tempo de fabricação
- uso de materiais
- retrabalho
- atrasos

---

## 5) Arquitetura e tecnologias
- **Python**: pipeline ETL (extração, transformação e carga)
- **PostgreSQL**: Data Warehouse
- **Power BI**: dashboards e relatórios

---

## 6) Processo mensal (ETL)
1. Extrair dados do banco transacional.
2. Limpar e padronizar campos.
3. Calcular métricas derivadas (ex.: margem).
4. Gerar arquivo CSV mensal.
5. Carregar dados no DW (modelo dimensional).
6. Atualizar datasets no Power BI.

---

## 7) Modelo dimensional (DW)

### 7.1 Tabela fato principal
**Fato_vendas**
- id_venda
- id_cliente
- id_produto
- id_tempo
- id_origem
- valor_total
- custo_total
- margem_lucro
- status (orçamento, aprovado, produzido, entregue)
- quantidade

### 7.2 Dimensões
**Dim_cliente**
- id_cliente, nome, cidade, estado, tipo_cliente

**Dim_produto**
- id_produto, tipo, categoria, material, acabamento

**Dim_tempo**
- id_tempo, data, mês, ano, trimestre

**Dim_origem**
- id_origem, origem

**Dim_personalizacao**
- id_personalizacao, medidas, quantidade_modulos, adicionais

### 7.3 Opcional (nível avançado)
**Fato_navegacao**
- id_evento, id_cliente, id_produto, id_tempo, pagina, tempo_permanencia, acao

---

## 8) Dashboards obrigatórios

### 8.1 Dashboard de Vendas
Responder:
- Quais tipos de móveis vendem mais?
- Qual categoria gera mais receita?
- Qual ticket médio por cliente?

### 8.2 Dashboard de Comportamento do Cliente
Responder:
- Em que etapa ocorre desistência (visualização → orçamento → compra)?
- Quais produtos são mais vistos e pouco comprados?
- Qual origem traz mais clientes compradores?

### 8.3 Dashboard Financeiro
Responder:
- Quais produtos têm maior margem de lucro?
- Existe diferença de lucro entre planejado e modulado?
- Qual período do mês tem mais vendas?

### 8.4 Dashboard de Personalização
Responder:
- Quais configurações são mais escolhidas?
- Existe padrão de tamanho mais pedido?
- Clientes preferem custo-benefício ou acabamento premium?

### 8.5 Dashboard de Produção
Responder:
- Quanto tempo leva para produzir cada tipo?

---

## 9) Regras de qualidade de dados
- Padronizar tipos de data, moeda e texto.
- Tratar valores nulos e duplicidades.
- Validar chaves de dimensão antes de carregar fatos.
- Garantir consistência entre `valor_total`, `custo_total` e `margem_lucro`.

---

## 10) Entregáveis
1. Pipeline ETL funcional em Python.
2. CSVs mensais gerados automaticamente.
3. DW em PostgreSQL com modelo estrela.
4. 5 dashboards no Power BI.
5. Documentação técnica e de negócio.

---

## 11) Critérios de aceite
- ETL executa sem erro para ao menos 12 meses de dados.
- Consultas principais retornam em tempo adequado para uso analítico.
- Dashboards respondem todas as perguntas de negócio listadas.
- Métricas financeiras conferem com os dados de origem.

---

## 12) Valor esperado
- Maior controle operacional e comercial.
- Redução de desperdícios e retrabalho.
- Melhoria na conversão de vendas.
- Aumento da competitividade da marcenaria.
