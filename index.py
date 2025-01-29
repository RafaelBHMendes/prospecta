import requests
import pandas as pd
from sqlalchemy import create_engine
from zipfile import ZipFile
from io import BytesIO
import os


DB_USER = 'seu_usuario'
DB_PASS = 'sua_senha'
DB_HOST = 'localhost'
DB_PORT = '3306'
DB_NAME = 'empresas_db'

CNPJ_URL = 'https://dadosabertos.rfb.gov.br/CNPJ/K3241.K03200Y0.D20814.EMPRECSV.zip'

temp_dir = 'temp_cnpj_data'
os.makedirs(temp_dir, exist_ok=True)

try:
    import pymysql
except ImportError:
    raise ImportError(
        "O módulo 'PyMySQL' não está instalado. Instale-o com 'pip install pymysql'.")


def download_and_extract_cnpj_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        with ZipFile(BytesIO(response.content)) as zfile:
            zfile.extractall(temp_dir)
        print(f"Dados extraídos para {temp_dir}")
    else:
        raise Exception(f"Erro ao baixar os dados: {response.status_code}")


def process_and_store_data(file_path, engine):
    chunks = pd.read_csv(file_path, sep=';',
                         encoding='latin1', dtype=str, chunksize=100000)
    for chunk in chunks:
        chunk = chunk.rename(columns={
            'CNPJ Básico': 'cnpj',
            'Razão Social / Nome Empresarial': 'razao_social',
            'Nome Fantasia': 'nome_fantasia',
            'Capital Social da Empresa': 'capital_social',
            'UF': 'uf',
            'Data de Início Atividade': 'data_abertura'
        })
        chunk.to_sql('empresas', engine, if_exists='append', index=False)
        print(f"Processado chunk de {len(chunk)} registros")


def update_database():
    try:
        engine = create_engine(
            f'mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        print("Conexão com o banco de dados estabelecida com sucesso.")
    except Exception as e:
        raise Exception(f"Erro ao conectar com o banco de dados: {e}")

    try:
        download_and_extract_cnpj_data(CNPJ_URL)
    except Exception as e:
        raise Exception(f"Erro ao baixar e extrair os dados: {e}")

    csv_file = os.path.join(temp_dir, 'K3241.K03200Y0.D20814.EMPRECSV')
    if not os.path.exists(csv_file):
        raise FileNotFoundError(
            f"Arquivo CSV não encontrado no caminho esperado: {csv_file}")

    try:
        process_and_store_data(csv_file, engine)
    except Exception as e:
        raise Exception(f"Erro ao processar e armazenar os dados: {e}")


if __name__ == "__main__":
    try:
        update_database()
        print("Atualização do banco de dados concluída com sucesso.")
    except Exception as e:
        print(f"Erro durante a execução: {e}")
