# Autor: Jeovani Paxe
# Data: 01/09/2025
# Descrição: DAG do Airflow para extrair dados de Bitcoin de uma API, 
                # transformar esses dados e carregá-los em um banco de dados MongoDB.

# GitHub:

import requests
import os
import dotenv
from datetime import datetime
import pytz
from pymongo import MongoClient

# Bibliotecas para integração com o Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

# Carrega variáveis de ambiente do arquivo .env
dotenv.load_dotenv()
chave = os.getenv("API_key")  # Obtém a chave da API

fuso_horario = pytz.timezone("Africa/Luanda")  # Define o fuso horário de Luanda

url = "https://api.api-ninjas.com/v1/bitcoin"  # URL da API de Bitcoin
header = {"X-API-Key": chave}  # Cabeçalho com a chave da API

# Função para extrair dados da API
def extract(url, header):
    dados = requests.get(url, header)  # Faz a requisição GET
    dados = dados.json()  # Converte a resposta para JSON
    return dados

# Função para transformar os dados extraídos
def transform(data_transform):
    data_hora = datetime.fromtimestamp(data_transform["timestamp"], fuso_horario)  # Converte timestamp para data/hora
    data = {
        "Preco": data_transform["price"],  # Preço atual do Bitcoin
        "Timestamp": data_hora,  # Data e hora formatada
        "variação_de_preço_em_24_horas": data_transform["24h_price_change"],  # Variação de preço em 24h
        "variação_de_preço_em_24_horas_em_porcentagem": data_transform["24h_price_change_percent"],  # Variação em %
        "máxima_em_24_horas": data_transform["24h_high"],  # Preço máximo em 24h
        "minima_em_24_horas": data_transform["24h_low"],  # Preço mínimo em 24h
        "volume_em_24_horas": data_transform["24h_volume"],  # Volume negociado em 24h
    }
    return data

# Função para carregar os dados transformados no MongoDB
def load(dataLoad: dict):
    client = MongoClient("mongodb://192.168.0.25:27017/")  # Conecta ao MongoDB, host do MongoDB no doker-compose
    db = client["Dados_BitCoin"]  # Cria/seleciona o banco de dados
    colecao = db["tabela"]  # Cria/seleciona a coleção (tabela)
    colecao.insert_one(dataLoad)  # Insere os dados na coleção

    if colecao is not None:
        return "Dados Salvo"  # Confirmação de sucesso
    else:
        return "Erro ao salvar os dados"  # Mensagem de erro

# Definição do DAG do Airflow
with DAG(
    dag_id="BitCoin",  # Identificador do DAG
    start_date=datetime(2025,9,29),  # Data de início
    schedule="* * * * *",  # Agendamento (a cada minuto)
    catchup=False,  # Não executa tarefas passadas
    tags=["ETL", "API", "MongoDB"],  # Tags para identificação
) as dag:
    # Tarefa de extração dos dados
    extrair = PythonOperator(
        task_id="extrair",
        python_callable=extract,
        op_kwargs={"url": url, "header": header}
    )
    # Tarefa de transformação dos dados
    trasnformar = PythonOperator(
        task_id="transformar",
        python_callable=transform,
        op_kwargs={"data_transform": extrair.output}
    )
    # Tarefa de carregamento dos dados no MongoDB
    carregar = PythonOperator(
        task_id="carregar",
        python_callable=load,
        op_kwargs={"dataLoad": trasnformar.output}
    )

# Define a ordem de execução das tarefas
extrair >> trasnformar >> carregar
