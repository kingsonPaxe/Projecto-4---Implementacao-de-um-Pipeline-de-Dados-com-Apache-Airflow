# Projecto 4 — Implementação de um Pipeline de Dados com Apache Airflow

## Visão Geral

Este projeto tem como objetivo construir e orquestrar um pipeline de dados completo utilizando o Apache Airflow, desde a extração de dados até o carregamento final em uma base de dados. O foco principal é demonstrar a aplicação prática de processos ETL (Extract, Transform, Load) em um ambiente automatizado e controlado.

## Conteúdo do Projeto

- **dags/**: Contém os arquivos Python dos DAGs do Airflow:
  - `project1.py`: Pipeline ETL para logs de site de vendas (extrai, transforma e carrega dados para MySQL).
  - `ETL_Bitcoin.py`: Pipeline que extrai dados de Bitcoin de uma API e carrega no MongoDB.
  - `exampledag.py`: DAG exemplo que consulta astronautas em missão e imprime informações utilizando TaskFlow API.
- **Dockerfile**: Imagem Astro Runtime para rodar o ambiente do Airflow.
- **requirements.txt**: Adicione aqui os pacotes Python necessários.
- **packages.txt**: Adicione pacotes de sistema operacional necessários.
- **plugins/**: Plugins customizados/comunitários para o Airflow.
- **airflow_settings.yaml**: Configurações locais de conexões, variáveis e pools do Airflow.

## Principais Funcionalidades

- **ETL de Logs**: Automatiza a coleta, tratamento e persistência dos logs de acesso do site, normalizando dados, geolocalizando IPs e salvando no MySQL.
- **ETL de Bitcoin**: Extrai dados de cotação de Bitcoin via API, transforma e carrega em MongoDB.
- **Exemplo de Astronautas**: Consulta a lista de astronautas no espaço, imprime informações por meio de mapeamento dinâmico de tarefas.

## Dependências

- Python
- Apache Airflow
- SQLAlchemy
- Pandas
- Requests
- PyMySQL
- Pymongo
- Docker (para ambiente local)
- Astronomer CLI (opcional para deploy em Astronomer)

## Como Executar Localmente

1. Instale o [Docker](https://www.docker.com/) e o [Astronomer CLI](https://docs.astronomer.io/astro/cli-install).
2. Clone o repositório:
   ```bash
   git clone https://github.com/kingsonPaxe/Projecto-4---Implementacao-de-um-Pipeline-de-Dados-com-Apache-Airflow.git
   cd Projecto-4---Implementacao-de-um-Pipeline-de-Dados-com-Apache-Airflow
   ```
3. Execute o ambiente de desenvolvimento:
   ```bash
   astro dev start
   ```
4. Acesse o Airflow Webserver em `http://localhost:8080`, e visualize/execute os DAGs disponíveis.

## Como Fazer Deploy para Astronomer

Se possuir uma conta Astronomer, consulte a [documentação oficial](https://www.astronomer.io/docs/astro/deploy-code/) para instruções detalhadas de deploy.

## Contato

Para dúvidas, sugestões ou bugs, entre em contato via [GitHub Issues](https://github.com/kingsonPaxe/Projecto-4---Implementacao-de-um-Pipeline-de-Dados-com-Apache-Airflow/issues).

---
Projeto mantido por [kingsonPaxe](https://github.com/kingsonPaxe)