import re
import pandas as pd
import requests as rqts
import dotenv
import os

# Importando módulos do Airflow para criar DAGs e operadores Python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

dotenv.load_dotenv()
password = os.getenv("senha") 
# Importando SQLAlchemy para conexão com banco de dados
from sqlalchemy import create_engine



# Definindo credenciais e informações do banco de dados
usuario = "root"
senha = password
host = "172.17.0.1"
port = "3307"
nome_do_banco = "log_site_vendas"

# Caminho do arquivo de log a ser processado
file = r"dags/server_example.log"

# Função para extrair os dados do arquivo de log
def extract(file):
    with open(file) as file_log:
        log = file_log.readlines()
    return log 

# Função para transformar os dados extraídos
def transform(log):
    padrao =r'(\d+\.\d+\.\d+\.\d+).*?\[(.*?)\] "(.*?)" (\d+) (\d+)' # Regex para extrair informações do log
    data = []
    id = 1
    for cada_log in log:
        match = re.match(padrao, cada_log)
        data.append({
            "ID":id,
            "Endereço Ip": match.group(1),
            "Data_hora":match.group(2),
            "Requisição":match.group(3),
            "Status code": int(match.group(4)), # 400, 200, 500
            "Bytes": int(match.group(5))
        })
        id+=1

    # Criando DataFrame com os dados extraídos
    df = pd.DataFrame(data)
    # Convertendo coluna de data/hora para datetime
    df['Data_hora'] = pd.to_datetime(df['Data_hora'], format='%d/%b/%Y:%H:%M:%S %z')
    # Removendo o fuso horário
    df['Data_hora'] = df['Data_hora'].dt.tz_localize(None)
    # Separando a requisição em método, recurso e protocolo
    df[['Método', 'Recurso', 'Protocolo']] = df['Requisição'].str.split(' ', expand=True)
    # Reorganizando as colunas
    df = df[["ID", "Endereço Ip","Data_hora",'Método', 'Recurso', 'Protocolo', "Status code", "Bytes"]]

    # Realizando geolocalização dos endereços IP
    dados_ip = []
    for ips in df["Endereço Ip"].values:
        url = f"http://ip-api.com/json/{ips}"
        resposta = rqts.get(url=url)
        if resposta.status_code == 200:
            try:
                d = resposta.json()
            except ValueError:
                d = {}
        else:
            d = {}
    dados_ip .append(d)
    # Criando DataFrame com dados de geolocalização
    dados_ip = pd.DataFrame(dados_ip)
    # Garantir que as colunas existam, preenchendo com 'Unknown' se necessário
    for col in ["country", "regionName", "city"]:
        if col not in dados_ip.columns:
            dados_ip[col] = "Unknown"
    # Concatenando dados de geolocalização ao DataFrame principal
    df = pd.concat([
        df.reset_index(drop=True),
        dados_ip[["country", "regionName", "city"]].rename(columns={
            "country": "Pais",
            "regionName": "Regiao",
            "city": "Cidade"
        })
    ], axis=1)
    return df


# Função para carregar os dados transformados no banco de dados e salvar em CSV
def load(data):
    conn_str = f"mysql+pymysql://{usuario}:{senha}@{host}:{port}/{nome_do_banco}"
    engine = create_engine(conn_str)

    data.to_sql('logs', con=engine, index=False, method='multi', if_exists='replace')
    engine.dispose()
    return f"Dados Salvos"

# Criando a DAG do Airflow e definindo as tarefas
with DAG(
    dag_id="Log_site_vendas",  # Identificador do DAG
    start_date=datetime(2025,9,29),  # Data de início
    schedule="0 0 * * *",  # Agendamento (diariamente à meia-noite)
    catchup=False,  # Não executa tarefas passadas
    tags=["ETL", "Logs", "MySQL"],  # Tags para identificação
) as dag:
    # Tarefa de extração dos dados
    extrair = PythonOperator(
        task_id="extrair",
        python_callable=extract,
        op_kwargs={"file": file},
    )

    # Tarefa de transformação dos dados
    transformar = PythonOperator(
        task_id="transformar",
        python_callable=transform,
        op_kwargs={"log": extrair.output},
    )

    # Tarefa de carregamento dos dados
    carregar = PythonOperator(
        task_id="carregar",
        python_callable=load,
        op_kwargs={"data": transformar.output},
    )

    # Definindo a ordem das tarefas
    extrair >> transformar >> carregar