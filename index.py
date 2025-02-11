import os
import requests
import pandas as pd
from zipfile import ZipFile
from io import BytesIO
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Numeric, Date, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker
from requests.adapters import HTTPAdapter, Retry

# Configuração do banco de dados
DATABASE_URL = "sqlite:///database.db"
engine = create_engine(DATABASE_URL, echo=True)
Base = declarative_base()

# Diretório temporário para os dados do CNPJ
temp_dir = 'temp_cnpj_data'
os.makedirs(temp_dir, exist_ok=True)

# URL do arquivo ZIP com os dados do CNPJ
CNPJ_URL = 'https://dadosabertos.rfb.gov.br/CNPJ/K3241.K03200Y0.D20814.EMPRECSV.zip'

# Definição do modelo Empresa (tabela "empresas")


class Empresa(Base):
    __tablename__ = 'empresas'

    cnpj = Column(String(14), primary_key=True)  # 'CNPJ Básico'
    # 'Razão Social / Nome Empresarial'
    nome_empresarial = Column(String(150))
    nome_fantasia = Column(String(150))            # 'Nome Fantasia'
    # 'Capital Social da Empresa'
    capital_social = Column(Numeric(15, 2))
    uf = Column(String(2))                         # UF da empresa
    data_abertura = Column(Date)                   # 'Data de Início Atividade'

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

# Funções de criação e obtenção de sessão


def create_database():
    """Cria as tabelas no banco de dados, caso ainda não existam."""
    Base.metadata.create_all(engine)
    print("Banco de dados e tabelas criadas com sucesso!")


def get_session():
    """Retorna uma nova sessão do banco de dados."""
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal()

# Função para download e extração dos dados


def download_and_extract_cnpj_data(url, timeout=30):
    try:
        session_req = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retries)
        session_req.mount('http://', adapter)
        session_req.mount('https://', adapter)

        response = session_req.get(url, timeout=timeout)
        response.raise_for_status()

        with ZipFile(BytesIO(response.content)) as zfile:
            zfile.extractall(temp_dir)
        print(f"Dados extraídos para {temp_dir}")
    except requests.exceptions.RequestException as e:
        raise Exception(f"Erro ao baixar os dados: {e}")

# Função para processar o CSV e armazenar os dados


def process_and_store_data(file_path, session):
    chunks = pd.read_csv(file_path, sep=';',
                         encoding='latin1', dtype=str, chunksize=100000)
    total_registros = 0
    for chunk in chunks:
        chunk = chunk.rename(columns={
            'CNPJ Básico': 'cnpj',
            'Razão Social / Nome Empresarial': 'nome_empresarial',
            'Nome Fantasia': 'nome_fantasia',
            'Capital Social da Empresa': 'capital_social',
            'UF': 'uf',
            'Data de Início Atividade': 'data_abertura'
        })
        for _, row in chunk.iterrows():
            try:
                cap_social = float(row['capital_social'].replace(
                    ',', '.')) if pd.notnull(row['capital_social']) else None
            except Exception:
                cap_social = None
            try:
                dt_abertura = datetime.strptime(
                    row['data_abertura'], '%d/%m/%Y').date() if pd.notnull(row['data_abertura']) else None
            except Exception:
                dt_abertura = None

            empresa = Empresa(
                cnpj=row['cnpj'],
                nome_empresarial=row.get('nome_empresarial'),
                nome_fantasia=row.get('nome_fantasia'),
                capital_social=cap_social,
                uf=row.get('uf'),
                data_abertura=dt_abertura
            )
            session.merge(empresa)
            total_registros += 1
        session.commit()
        print(
            f"Processado chunk com {len(chunk)} registros. Total inserido/atualizado: {total_registros}")

# Função para orquestrar o update do banco de dados


def update_database():
    create_database()
    session = get_session()
    try:
        download_and_extract_cnpj_data(CNPJ_URL)
    except Exception as e:
        raise Exception(f"Erro ao baixar e extrair os dados: {e}")

    csv_file = os.path.join(temp_dir, 'K3241.K03200Y0.D20814.EMPRECSV')
    if not os.path.exists(csv_file):
        raise FileNotFoundError(
            f"Arquivo CSV não encontrado no caminho esperado: {csv_file}")

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

# FUNÇÃO DE FILTRO INTERATIVO


def filtrar_empresas():
    session = get_session()
    query = session.query(Empresa)

    print("\n=== Filtro de Empresas ===")
    print("Digite os filtros desejados. Para ignorar um filtro, deixe em branco e pressione Enter.\n")

    # Filtro por CNPJ (igual exato)
    cnpj = input("CNPJ (14 dígitos, exato): ").strip()
    if cnpj:
        query = query.filter(Empresa.cnpj == cnpj)

    # Filtro por Nome Empresarial (busca parcial)
    nome_empresarial = input("Nome Empresarial (busca parcial): ").strip()
    if nome_empresarial:
        query = query.filter(
            Empresa.nome_empresarial.ilike(f"%{nome_empresarial}%"))

    # Filtro por Nome Fantasia (busca parcial)
    nome_fantasia = input("Nome Fantasia (busca parcial): ").strip()
    if nome_fantasia:
        query = query.filter(Empresa.nome_fantasia.ilike(f"%{nome_fantasia}%"))

    # Filtro por Capital Social (mínimo e máximo)
    capital_min = input("Capital Social Mínimo (ex: 1000.00): ").strip()
    if capital_min:
        try:
            capital_min_val = float(capital_min.replace(',', '.'))
            query = query.filter(Empresa.capital_social >= capital_min_val)
        except Exception:
            print("Valor inválido para Capital Social Mínimo.")

    capital_max = input("Capital Social Máximo (ex: 100000.00): ").strip()
    if capital_max:
        try:
            capital_max_val = float(capital_max.replace(',', '.'))
            query = query.filter(Empresa.capital_social <= capital_max_val)
        except Exception:
            print("Valor inválido para Capital Social Máximo.")

    # Filtro por UF
    uf = input("UF (estado, ex: SP): ").strip()
    if uf:
        query = query.filter(Empresa.uf.ilike(f"%{uf}%"))

    # Filtro por Data de Abertura (intervalo)
    data_abertura_inicio = input(
        "Data de Abertura - Início (dd/mm/aaaa): ").strip()
    if data_abertura_inicio:
        try:
            dt_inicio = datetime.strptime(
                data_abertura_inicio, '%d/%m/%Y').date()
            query = query.filter(Empresa.data_abertura >= dt_inicio)
        except Exception:
            print("Data inválida para Data de Abertura Início.")

    data_abertura_fim = input("Data de Abertura - Fim (dd/mm/aaaa): ").strip()
    if data_abertura_fim:
        try:
            dt_fim = datetime.strptime(data_abertura_fim, '%d/%m/%Y').date()
            query = query.filter(Empresa.data_abertura <= dt_fim)
        except Exception:
            print("Data inválida para Data de Abertura Fim.")

    # Filtro por Situação Cadastral (busca parcial)
    situacao_cadastral = input("Situação Cadastral (busca parcial): ").strip()
    if situacao_cadastral:
        query = query.filter(
            Empresa.situacao_cadastral.ilike(f"%{situacao_cadastral}%"))

    # Filtro por Opção SIMEI (boolean)
    opcao_simei = input("Opção SIMEI? (sim/não): ").strip().lower()
    if opcao_simei in ["sim", "s"]:
        query = query.filter(Empresa.opcao_simei == True)
    elif opcao_simei in ["não", "nao", "n"]:
        query = query.filter(Empresa.opcao_simei == False)

    # Filtro por Opção Simples Nacional (boolean)
    opcao_simples = input(
        "Opção Simples Nacional? (sim/não): ").strip().lower()
    if opcao_simples in ["sim", "s"]:
        query = query.filter(Empresa.opcao_simples_nacional == True)
    elif opcao_simples in ["não", "nao", "n"]:
        query = query.filter(Empresa.opcao_simples_nacional == False)

    print("\nExecutando consulta...")
    resultados = query.all()

    print(f"\nForam encontrados {len(resultados)} resultados.\n")

    # Armazena todos os resultados em um objeto: uma lista de dicionários com todas as informações.
    empresas_obj = []
    for empresa in resultados:
        empresa_dict = {
            "cnpj": empresa.cnpj,
            "nome_empresarial": empresa.nome_empresarial,
            "nome_fantasia": empresa.nome_fantasia,
            "capital_social": float(empresa.capital_social) if empresa.capital_social is not None else None,
            "uf": empresa.uf,
            "data_abertura": empresa.data_abertura.strftime('%d/%m/%Y') if empresa.data_abertura else None,
            "indicador_matriz_filial": empresa.indicador_matriz_filial,
            "situacao_cadastral": empresa.situacao_cadastral,
            "data_situacao_cadastral": empresa.data_situacao_cadastral.strftime('%d/%m/%Y') if empresa.data_situacao_cadastral else None,
            "cidade_no_exterior": empresa.cidade_no_exterior,
            "codigo_pais": empresa.codigo_pais,
            "nome_pais": empresa.nome_pais,
            "natureza_juridica": empresa.natureza_juridica,
            "endereco": empresa.endereco,
            "referencia": empresa.referencia,
            "telefone": empresa.telefone,
            "email": empresa.email,
            "opcao_simei": empresa.opcao_simei,
            "porte": empresa.porte,
            "opcao_simples_nacional": empresa.opcao_simples_nacional,
            "motivo_situacao_cadastral": empresa.motivo_situacao_cadastral,
            "situacao_especial": empresa.situacao_especial,
            "data_situacao_especial": empresa.data_situacao_especial.strftime('%d/%m/%Y') if empresa.data_situacao_especial else None,
        }
        empresas_obj.append(empresa_dict)

    # Exibe o objeto contendo todas as empresas encontradas
    print("Empresas armazenadas no objeto:")
    print(empresas_obj)

    session.close()

    # Retorna o objeto caso você precise utilizá-lo em outras partes da aplicação
    return empresas_obj


# Bloco principal
if __name__ == "__main__":
    main()  # Atualiza e popula o banco de dados

    # Após atualizar o banco, pergunta se o usuário deseja realizar uma consulta filtrada
    opcao = input(
        "\nDeseja realizar uma consulta filtrada? (sim/não): ").strip().lower()
    if opcao in ["sim", "s"]:
        filtrar_empresas()
