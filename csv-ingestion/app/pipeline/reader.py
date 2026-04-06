# O Reader tem como responsabilidade extrair os dados dos arquivos já validados e transferidos para a pasta 'read'. Ele pode ser implementado para ler os arquivos CSV, processar os dados conforme necessário e prepará-los para a etapa de transformação.

import pandas as pd
from pathlib import Path    

# Função para ler os arquivos CSV da pasta 'read' e retornar uma lista de DataFrames
def read_csv_files():
    # Caminho absoluto para a pasta 'read' a partir do diretório do script
    base_dir = Path(__file__).parent.parent / "datas" / "read"

    # Verifica se a pasta existe
    base_dir.mkdir(parents=True, exist_ok=True)

    # Lista de arquivos CSV na pasta 'read'
    csv_files = base_dir.glob("*.csv")

    # Lista para armazenar os DataFrames lidos dos arquivos CSV
    dataframes = []

    # Lê cada arquivo CSV e armazena o DataFrame na lista
    for csv_file in csv_files:
        df = pd.read_csv(csv_file, delimiter=";")
        dataframes.append(df)

    return dataframes

# Função para ler um arquivo CSV específico e retornar um DataFrame
def read_csv_file(file_path):
    # Lê o arquivo CSV e retorna um DataFrame
    df = pd.read_csv(file_path, delimiter=";")
    return df

# Função para ler os arquivos CSV de um diretório específico e retornar uma lista de DataFrames
def read_csv_files_from_directory(directory_path):
    # Verifica se a pasta existe
    directory = Path(directory_path)
    if not directory.exists():
        raise FileNotFoundError(f"A pasta {directory_path} não foi encontrada.")

    # Lista de arquivos CSV na pasta especificada
    csv_files = directory.glob("*.csv")

    # Lista para armazenar os DataFrames lidos dos arquivos CSV
    dataframes = []

    # Lê cada arquivo CSV e armazena o DataFrame na lista
    for csv_file in csv_files:
        df = pd.read_csv(csv_file, delimiter=";")
        dataframes.append(df)

    return dataframes
