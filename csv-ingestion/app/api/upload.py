# O Upload tem como responsabilidade validar os arquivos CSV que são colocados na pasta 'unread' e transferi-los para a pasta 'read' se estiverem corretos. Ele pode ser implementado para verificar se os arquivos possuem as colunas esperadas, se estão no formato correto e se não estão vazios antes de movê-los para a pasta 'read'.

import csv
import shutil
from pathlib import Path
import logging
from app.api.config import EXPECTED_COLUMNS, FILE_DELIMITER
logging.basicConfig(level=logging.INFO)

 # Colunas esperadas

# Função para validar as colunas do arquivo CSV
def validate_columns_file(file_path: Path) -> bool:
    # Verifica se o arquivo existe
    if not file_path.exists():
        logging.warning(f"{file_path.name} não encontrado.")
        return False
    if file_path.suffix.lower() != ".csv":
        logging.warning(f"{file_path.name} não é um arquivo CSV.")
        return False
    try:
        # Lê o arquivo CSV e verifica as colunas
        with open(file_path, "r", encoding="utf-8") as csvfile:
            # O delimitador é definido como ';' (no arquivo de configuração) para arquivos CSV
            reader = csv.reader(csvfile, delimiter=FILE_DELIMITER)
            # Lê a primeira linha do arquivo para obter as colunas
            header = next(reader)

            # Compara as colunas do arquivo com as colunas esperadas
            if header != EXPECTED_COLUMNS:
                logging.warning(f"{file_path.name} colunas inválidas: {header}")
                return False
            return True
        
    # Captura qualquer exceção que possa ocorrer durante a leitura do arquivo
    except Exception as e:
        logging.error(f"Erro ao validar {file_path.name}: {e}")
        return False


def get_unread_directory() -> Path:
    unread_dir = Path(__file__).parent.parent / "datas" / "unread"
    unread_dir.mkdir(parents=True, exist_ok=True)
    return unread_dir


def get_read_directory() -> Path:
    read_dir = Path(__file__).parent.parent / "datas" / "read"
    read_dir.mkdir(parents=True, exist_ok=True)
    return read_dir


def get_unread_csv_files() -> list[Path]:
    unread_dir = get_unread_directory()
    return sorted(unread_dir.glob("*.csv"))


def move_file_to_read(file_path: Path) -> None:
    read_dir = get_read_directory()
    shutil.move(str(file_path), str(read_dir / file_path.name))



def transfer_csv_files():
    base_dir = get_unread_directory()

    # Contador arquivos transferidos
    transfered_files = 0
    # Contador arquivos não transferidos
    untransfered_files = 0

    # Move os arquivos lidosa
    for arquivo in get_unread_csv_files():
        # Valida o tamanho do arquivo
        if arquivo.stat().st_size > 0:
            if validate_columns_file(arquivo):
                move_file_to_read(arquivo)
                transfered_files += 1
        else:
            untransfered_files += 1

    print(transfered_files, "Arquivos transferidos")
    print(untransfered_files, "Arquivos não transferidos")

