# Business Intelligence em Vendas de Marcenarias

## Modelo de negócio

Análise de dados de acesso e vendas de um site de móveis sob medida, modulados e padrão.

## Problema

Falta de visão estratégica sobre vendas, comportamento do cliente e produção na marcenaria, dificultando a tomada de decisões baseadas em dados e reduzindo a eficiência operacional e comercial.

## Solução

Desenvolvimento de um sistema de vendas integrado a uma camada de Business Intelligence capaz de coletar, armazenar e analisar dados provenientes da navegação dos usuários, configuração de produtos, orçamentos, vendas e produção.

O objetivo é transformar dados em informações estratégicas para apoiar decisões do negócio.

## Dados coletados

- **Dados de usuários:** identificação, localização, tipo de cliente e histórico de interação com o site.
- **Dados de navegação:** páginas acessadas, tempo de permanência, produtos visualizados, abandono de carrinho e origem do tráfego.
- **Dados de produtos:** tipo de móvel (planejado, modulado ou padrão), categoria, materiais, dimensões, custo e preço.
- **Dados de personalização:** medidas, acabamentos, quantidade de módulos e adicionais escolhidos.
- **Dados de vendas:** pedidos, datas, valores, status, tempo de fechamento e margem de lucro.
- **Dados de produção:** tempo de fabricação, uso de materiais, retrabalho e atrasos.

## Objetivo

Gerar insights que permitam aumentar as vendas, melhorar a experiência do cliente, otimizar processos produtivos e maximizar a lucratividade da marcenaria.

## Dashboards

### 1. Dashboard de vendas

Apresenta indicadores gerais de desempenho comercial, permitindo analisar resultados e tendências de vendas.

**Perguntas-chave:**

- Quais tipos de móveis vendem mais (planejado, modulado ou padrão)?
- Qual categoria gera mais receita?
- Qual é o ticket médio por cliente?

### 2. Dashboard de comportamento do cliente

Focado na jornada do usuário dentro do site, identificando padrões de navegação e pontos de abandono.

**Perguntas-chave:**

- Em que etapa os clientes desistem (visualização -> orçamento -> compra)?
- Quais produtos são mais visualizados, mas pouco comprados?
- Qual origem traz mais clientes que realmente compram?

### 3. Dashboard financeiro

Analisa a rentabilidade do negócio, auxiliando na definição de estratégias de precificação e investimento.

**Perguntas-chave:**

- Quais produtos têm maior margem de lucro?
- Existe diferença de lucro entre planejado e modulado?
- Qual período do mês tem mais vendas?

### 4. Dashboard de itens personalizados pelos clientes

Explora os dados de customização dos móveis, permitindo identificar preferências e padrões de consumo.

**Perguntas-chave:**

- Quais configurações são mais escolhidas?
- Existe padrão de tamanho mais pedido?
- Clientes preferem mais custo-benefício ou acabamento premium?

### 5. Dashboard de produção

Acompanha o desempenho operacional da marcenaria, ajudando a identificar ineficiências e melhorar processos.

**Pergunta-chave:**

- Quanto tempo leva para produzir cada tipo?

## Valor gerado

A implementação deste modelo permite uma gestão orientada a dados, proporcionando:

- maior controle sobre o negócio;
- redução de desperdícios;
- melhoria na conversão de vendas;
- aumento da competitividade no mercado.

## Captação de dados

A captação de dados consiste no registro contínuo das informações geradas no site e nos processos internos da marcenaria, incluindo navegação dos usuários, dados de produtos, personalizações, vendas e produção.

Esses dados são armazenados no banco de dados do sistema e, ao final de cada mês, é realizado um processo automatizado que extrai e organiza essas informações em um arquivo no formato CSV.

## Tecnologias utilizadas

### Python para ETL

Utilizado para realizar o processo de extração, transformação e carga dos dados. Será responsável por coletar as informações do banco transacional, tratar os dados, padronizar estruturas e gerar os arquivos CSV mensais para análise.

### PostgreSQL para Data Warehouse

Utilizado como banco de dados para armazenamento estruturado dos dados históricos. Permitirá organizar as informações em tabelas otimizadas para consulta, facilitando análises e garantindo desempenho no processamento dos dados.

### Power BI para visualização

Utilizado para criação de dashboards e relatórios interativos. A ferramenta permitirá a análise dos dados de forma visual, auxiliando na identificação de padrões, indicadores e insights para a tomada de decisão.

## Estrutura do Data Warehouse

O Data Warehouse será estruturado utilizando o modelo dimensional (modelo estrela), com o objetivo de facilitar consultas analíticas e otimizar a geração de dashboards no Power BI.

### Tabela fato

#### Fato_vendas

Tabela central que armazena os eventos de venda e orçamento, contendo as principais métricas do negócio.

**Principais campos:**

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

### Tabelas dimensão

#### Dim_cliente

Armazena informações dos clientes.

**Campos:** id_cliente, nome, cidade, estado, tipo_cliente

#### Dim_produto

Contém informações dos móveis.

**Campos:** id_produto, tipo (planejado, modulado, padrão), categoria, material, acabamento

#### Dim_tempo

Responsável pela análise temporal dos dados.

**Campos:** id_tempo, data, mês, ano, trimestre

#### Dim_origem

Identifica a origem do acesso do cliente.

**Campos:** id_origem, origem (Google, Instagram, direto, etc.)

#### Dim_personalizacao

Armazena dados de configuração dos móveis.

**Campos:** id_personalizacao, medidas, quantidade_modulos, adicionais

### Fato_navegacao (opcional – nível mais avançado)

Armazena eventos de navegação dos usuários no site.

**Campos:** id_evento, id_cliente, id_produto, id_tempo, pagina, tempo_permanencia, acao




