import os
import requests
import pandas as pd
from zipfile import ZipFile
from io import BytesIO
from datetime import datetime
from sqlalchemy import (
    create_engine, Column, String, Integer, Date, Numeric, Boolean, ForeignKey
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

DATABASE_URL = "sqlite:///database.db"
engine = create_engine(DATABASE_URL, echo=True)
Base = declarative_base()

temp_dir = 'temp_cnpj_data'
os.makedirs(temp_dir, exist_ok=True)

CNPJ_URL = 'https://dadosabertos.rfb.gov.br/CNPJ/K3241.K03200Y0.D20814.EMPRECSV.zip'

class Empresa(Base):
    __tablename__ = 'empresas'
    
    # Campos mapeados a partir do CSV e outros para futuras informações.
    cnpj = Column(String(14), primary_key=True)  # 'CNPJ Básico'
    nome_empresarial = Column(String(150))        # 'Razão Social / Nome Empresarial'
    nome_fantasia = Column(String(150))           # 'Nome Fantasia'
    capital_social = Column(Numeric(precision=15, scale=2))  # 'Capital Social da Empresa'
    uf = Column(String(2))                        # UF da empresa
    data_abertura = Column(Date)                  # 'Data de Início Atividade'
    
    # Outros campos (opcionais ou para uso futuro)
    indicador_matriz_filial = Column(String(20), nullable=True)
    situacao_cadastral = Column(String(50), nullable=True)
    data_situacao_cadastral = Column(Date, nullable=True)
    cidade_no_exterior = Column(String(100), nullable=True)
    codigo_pais = Column(String(10), nullable=True)
    nome_pais = Column(String(100), nullable=True)
    natureza_juridica = Column(String(10), nullable=True)
    endereco = Column(String(255), nullable=True)
    referencia = Column(String(255), nullable=True)
    telefone = Column(String(20), nullable=True)
    email = Column(String(100), nullable=True)
    opcao_simei = Column(Boolean, nullable=True)
    porte = Column(String(50), nullable=True)
    opcao_simples_nacional = Column(Boolean, nullable=True)
    motivo_situacao_cadastral = Column(String(100), nullable=True)
    situacao_especial = Column(String(100), nullable=True)
    data_situacao_especial = Column(Date, nullable=True)
    
    
    def __repr__(self):
        return f"<Empresa(cnpj='{self.cnpj}', nome_empresarial='{self.nome_empresarial}')>"

# Caso queira incluir os modelos de CNAE secundário e QSA, descomente e adapte conforme necessário.
# class EmpresaCNAE(Base):
#     __tablename__ = 'empresa_cnaes'
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     empresa_cnpj = Column(String(14), ForeignKey('empresas.cnpj'), nullable=False)
#     cnae = Column(String(10), nullable=False)
#     empresa = relationship("Empresa", back_populates="cnaes_secundarios")
#
# class QSA(Base):
#     __tablename__ = 'qsa'
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     empresa_cnpj = Column(String(14), ForeignKey('empresas.cnpj'), nullable=False)
#     cpf = Column(String(11), nullable=False)
#     qualificacao = Column(String(50), nullable=False)
#     empresa = relationship("Empresa", back_populates="qsa")

def create_database():
    """Cria as tabelas no banco de dados, se ainda não existirem."""
    Base.metadata.create_all(engine)
    print("Banco de dados e tabelas criadas com sucesso!")

def get_session():
    """Retorna uma nova sessão do banco de dados."""
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal()

def download_and_extract_cnpj_data(url):
    """Faz o download do arquivo ZIP a partir da URL e extrai o conteúdo para o diretório temporário."""
    response = requests.get(url)
    if response.status_code == 200:
        with ZipFile(BytesIO(response.content)) as zfile:
            zfile.extractall(temp_dir)
        print(f"Dados extraídos para {temp_dir}")
    else:
        raise Exception(f"Erro ao baixar os dados: {response.status_code}")

def process_and_store_data(file_path, session):
    """
    Processa os dados do arquivo CSV (em chunks) e armazena os registros na tabela 'empresas'.
    
    O CSV possui, por exemplo, as seguintes colunas:
      - 'CNPJ Básico'
      - 'Razão Social / Nome Empresarial'
      - 'Nome Fantasia'
      - 'Capital Social da Empresa'
      - 'UF'
      - 'Data de Início Atividade'
    
    Esses campos são mapeados para os atributos do modelo Empresa.
    """
    # Lendo o CSV em chunks para economizar memória
    chunks = pd.read_csv(file_path, sep=';', encoding='latin1', dtype=str, chunksize=100000)
    
    total_registros = 0
    for chunk in chunks:
        # Renomeia as colunas conforme necessário
        chunk = chunk.rename(columns={
            'CNPJ Básico': 'cnpj',
            'Razão Social / Nome Empresarial': 'nome_empresarial',
            'Nome Fantasia': 'nome_fantasia',
            'Capital Social da Empresa': 'capital_social',
            'UF': 'uf',
            'Data de Início Atividade': 'data_abertura'
        })
        # Itera sobre as linhas do chunk
        for _, row in chunk.iterrows():
            # Converte capital_social para número (se possível)
            try:
                cap_social = float(row['capital_social'].replace(',', '.')) if pd.notnull(row['capital_social']) else None
            except Exception:
                cap_social = None

            # Converte data de abertura para o tipo date
            try:
                dt_abertura = datetime.strptime(row['data_abertura'], '%d/%m/%Y').date() if pd.notnull(row['data_abertura']) else None
            except Exception:
                dt_abertura = None

            # Cria o objeto Empresa (os campos não mapeados ficam como None)
            empresa = Empresa(
                cnpj=row['cnpj'],
                nome_empresarial=row.get('nome_empresarial'),
                nome_fantasia=row.get('nome_fantasia'),
                capital_social=cap_social,
                uf=row.get('uf'),
                data_abertura=dt_abertura
            )
            session.merge(empresa)  # merge: insere ou atualiza se já existir
            total_registros += 1
        session.commit()
        print(f"Processado chunk com {len(chunk)} registros. Total inserido/atualizado: {total_registros}")

def update_database():
    """Função que orquestra o download, extração, processamento e armazenamento dos dados."""
    # Cria as tabelas (caso ainda não existam)
    create_database()

    # Obtem a sessão do banco de dados
    session = get_session()

    # Baixa e extrai o arquivo ZIP com os dados do CNPJ
    try:
        download_and_extract_cnpj_data(CNPJ_URL)
    except Exception as e:
        raise Exception(f"Erro ao baixar e extrair os dados: {e}")

    # Define o caminho do arquivo CSV extraído
    # Observe que o nome do arquivo pode variar conforme a versão; ajuste se necessário.
    csv_file = os.path.join(temp_dir, 'K3241.K03200Y0.D20814.EMPRECSV')
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"Arquivo CSV não encontrado no caminho esperado: {csv_file}")

    # Processa o CSV e insere os dados no banco
    try:
        process_and_store_data(csv_file, session)
    except Exception as e:
        raise Exception(f"Erro ao processar e armazenar os dados: {e}")
    finally:
        session.close()


def main():
    try:
        update_database()
        print("Atualização do banco de dados concluída com sucesso.")
    except Exception as e:
        print(f"Erro durante a execução: {e}")

if __name__ == "__main__":
    main()
