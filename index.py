import os
import re
import requests
import pandas as pd
from zipfile import ZipFile
from io import BytesIO
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Numeric, Date, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker
from requests.adapters import HTTPAdapter, Retry

# ===========================================================
# CONFIGURAÇÃO DO BANCO DE DADOS (SQLite + SQLAlchemy)
# ===========================================================
DATABASE_URL = "sqlite:///database.db"
engine = create_engine(DATABASE_URL, echo=True)
Base = declarative_base()


class Empresa(Base):
    __tablename__ = 'empresas'
    cnpj = Column(String(14), primary_key=True)  # CNPJ (parte básica)
    nome_empresarial = Column(String(150))
    nome_fantasia = Column(String(150))
    capital_social = Column(Numeric(15, 2))
    uf = Column(String(2))
    data_abertura = Column(Date)

    # Campos adicionais (para uso futuro)
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


def create_database():
    Base.metadata.create_all(engine)
    print("Banco de dados e tabelas criadas com sucesso!")


def get_session():
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal()

# ===========================================================
# FUNÇÃO: Obter a lista de arquivos do diretório remoto
# ===========================================================


def get_file_list(base_url):
    """
    Obtém a lista de arquivos disponíveis no diretório remoto e filtra
    os arquivos de CNAE e Empresas segundo os padrões indicados.
    """
    try:
        response = requests.get(base_url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Erro ao acessar {base_url}: {e}")

    html = response.text
    # Procura por links: pega os valores de href
    files = re.findall(r'href="([^"]+)"', html)
    # Filtra apenas os itens que não são diretórios (não terminam com '/')
    files = [f for f in files if not f.endswith('/')]
    # Filtra os arquivos com os padrões desejados (case-insensitive)
    cnae_files = [f for f in files if "CNAECSV" in f.upper()]
    empresa_files = [f for f in files if "EMPRECSV" in f.upper()]
    return cnae_files, empresa_files

# ===========================================================
# FUNÇÃO: Download e extração com verificação local
# ===========================================================


def download_and_extract_file(url, dest_dir, filename, timeout=30):
    """
    Baixa o arquivo a partir de `url` e salva em `dest_dir/filename`.
    Se o arquivo já existir, o download é pulado.
    Em seguida, tenta extrair o conteúdo como ZIP (caso o arquivo seja um zip).
    Retorna uma tupla: (caminho_local, extraido_booleano).
    """
    dest_path = os.path.join(dest_dir, filename)
    if os.path.exists(dest_path):
        print(
            f"Arquivo '{filename}' já está presente em '{dest_dir}'. Pulando download.")
        return dest_path, True  # Considera que o arquivo já está disponível/extrado

    try:
        session_req = requests.Session()
        retries = Retry(total=5, backoff_factor=1,
                        status_forcelist=[502, 503, 504],
                        allowed_methods=["GET"])
        adapter = HTTPAdapter(max_retries=retries)
        session_req.mount("http://", adapter)
        session_req.mount("https://", adapter)

        print(f"Baixando {url}...")
        response = session_req.get(url, timeout=timeout)
        response.raise_for_status()
        # Salva o arquivo localmente
        with open(dest_path, "wb") as f:
            f.write(response.content)
        print(f"Arquivo '{filename}' salvo em '{dest_path}'.")

        # Tenta extrair o arquivo – se for um zip válido
        extracted = False
        try:
            with ZipFile(dest_path, "r") as zfile:
                zfile.extractall(dest_dir)
                extracted = True
                print(f"Arquivo '{filename}' extraído com sucesso.")
        except Exception as e:
            print(
                f"Arquivo '{filename}' não é um zip ou não pôde ser extraído: {e}.")
        return dest_path, extracted
    except requests.exceptions.RequestException as e:
        raise Exception(f"Erro ao baixar {url}: {e}")

# ===========================================================
# FUNÇÃO: Processar o arquivo de Empresas
# ===========================================================


def process_empresas_file(file_path, session):
    """
    Processa o arquivo CSV das Empresas extraído (ou baixado) e insere os registros no banco.
    O arquivo é esperado com 7 colunas, conforme o exemplo:

      "98768179";"ANGELINA SANTANA DE OLIVEIRA";"2135";"50";"0,00";"05";""

    O mapeamento é:
      - Coluna 0: cnpj
      - Coluna 1: nome_empresarial
      - Coluna 4: capital_social (convertido para float)
      - Coluna 5: uf
      - Coluna 6: data_abertura (formato dd/mm/aaaa, se presente)
    """
    print(f"Processando arquivo de Empresas: {file_path}")
    chunks = pd.read_csv(
        file_path,
        sep=';',
        header=None,
        names=['cnpj', 'nome_empresarial', 'col2', 'col3',
               'capital_social', 'uf', 'data_abertura'],
        encoding='latin1',
        dtype=str,
        chunksize=100000,
        quotechar='"'
    )
    total_registros = 0
    for chunk in chunks:
        # Limpa os dados (remove espaços e aspas residuais)
        chunk['cnpj'] = chunk['cnpj'].str.strip().str.replace('"', '')
        chunk['nome_empresarial'] = chunk['nome_empresarial'].str.strip(
        ).str.replace('"', '')
        chunk['capital_social'] = chunk['capital_social'].str.strip(
        ).str.replace('"', '')
        chunk['uf'] = chunk['uf'].str.strip().str.replace('"', '')
        chunk['data_abertura'] = chunk['data_abertura'].str.strip(
        ).str.replace('"', '')

        for _, row in chunk.iterrows():
            # Converte capital_social (tratando vírgula como separador decimal)
            try:
                cap_social = float(row['capital_social'].replace(
                    ',', '.')) if row['capital_social'] not in [None, ''] else None
            except Exception:
                cap_social = None

            # Converte data_abertura, se presente
            dt_abertura = None
            if row['data_abertura'] not in [None, '']:
                try:
                    dt_abertura = datetime.strptime(
                        row['data_abertura'], '%d/%m/%Y').date()
                except Exception:
                    dt_abertura = None

            empresa = Empresa(
                cnpj=row['cnpj'],
                nome_empresarial=row['nome_empresarial'],
                nome_fantasia=None,  # Não há dado para este campo no arquivo
                capital_social=cap_social,
                uf=row['uf'],
                data_abertura=dt_abertura
            )
            session.merge(empresa)
            total_registros += 1
        session.commit()
        print(
            f"Processado um chunk com {len(chunk)} registros. Total inserido/atualizado: {total_registros}")

# ===========================================================
# FUNÇÃO: Orquestração geral (download, extração e processamento)
# ===========================================================


def update_database():
    create_database()
    session = get_session()

    # Diretório temporário (já existente na raiz do projeto)
    temp_dir = 'temp_cnpj_data'
    os.makedirs(temp_dir, exist_ok=True)

    # URL base onde os arquivos estão hospedados
    base_url = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2023-05/"

    # Obtém a lista de arquivos disponíveis, filtrando por padrões
    try:
        cnae_files, empresa_files = get_file_list(base_url)
    except Exception as e:
        print(f"Erro ao obter a lista de arquivos: {e}")
        session.close()
        return

    print("Arquivos de CNAE encontrados:", cnae_files)
    print("Arquivos de Empresas encontrados:", empresa_files)

    # Baixa e extrai os arquivos de CNAE (para uso futuro, se necessário)
    for file_name in cnae_files:
        file_url = base_url + file_name
        try:
            download_and_extract_file(file_url, temp_dir, file_name)
        except Exception as e:
            print(e)
            continue

    # Para cada arquivo de Empresas: baixa, extrai (se aplicável) e processa os dados
    for file_name in empresa_files:
        file_url = base_url + file_name
        try:
            local_path, extracted = download_and_extract_file(
                file_url, temp_dir, file_name)
        except Exception as e:
            print(e)
            continue

        # Se o arquivo foi extraído como ZIP, assumimos que o CSV gerado tem o mesmo nome
        # (ou, se o zip contiver apenas um arquivo, usamos esse nome)
        if file_name.lower().endswith('.zip'):
            csv_filename = file_name[:-4]
        else:
            # Tenta verificar se o arquivo baixado é um zip (mesmo sem extensão .zip)
            try:
                with ZipFile(local_path, "r") as zfile:
                    lista_extraidos = zfile.namelist()
                    if lista_extraidos:
                        csv_filename = lista_extraidos[0]
                    else:
                        csv_filename = file_name
            except Exception:
                csv_filename = file_name

        csv_path = os.path.join(temp_dir, csv_filename)
        if os.path.exists(csv_path):
            try:
                process_empresas_file(csv_path, session)
            except Exception as e:
                print(f"Erro ao processar {csv_path}: {e}")
        else:
            print(
                f"Arquivo CSV '{csv_filename}' não encontrado após extração.")
    session.close()
    print("Atualização do banco de dados concluída.")


# ===========================================================
# BLOCO PRINCIPAL
# ===========================================================
if __name__ == "__main__":
    try:
        update_database()
    except Exception as e:
        print(f"Erro durante a execução: {e}")

    # Exemplo de função de filtro interativo (mantida do exemplo original)
    def filtrar_empresas():
        session = get_session()
        query = session.query(Empresa)

        print("\n=== Filtro de Empresas ===")
        print("Digite os filtros desejados. Para ignorar um filtro, deixe em branco e pressione Enter.\n")

        cnpj = input("CNPJ (14 dígitos, exato): ").strip()
        if cnpj:
            query = query.filter(Empresa.cnpj == cnpj)

        nome_empresarial = input("Nome Empresarial (busca parcial): ").strip()
        if nome_empresarial:
            query = query.filter(
                Empresa.nome_empresarial.ilike(f"%{nome_empresarial}%"))

        nome_fantasia = input("Nome Fantasia (busca parcial): ").strip()
        if nome_fantasia:
            query = query.filter(
                Empresa.nome_fantasia.ilike(f"%{nome_fantasia}%"))

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

        uf = input("UF (estado, ex: SP): ").strip()
        if uf:
            query = query.filter(Empresa.uf.ilike(f"%{uf}%"))

        data_abertura_inicio = input(
            "Data de Abertura - Início (dd/mm/aaaa): ").strip()
        if data_abertura_inicio:
            try:
                dt_inicio = datetime.strptime(
                    data_abertura_inicio, '%d/%m/%Y').date()
                query = query.filter(Empresa.data_abertura >= dt_inicio)
            except Exception:
                print("Data inválida para Data de Abertura Início.")

        data_abertura_fim = input(
            "Data de Abertura - Fim (dd/mm/aaaa): ").strip()
        if data_abertura_fim:
            try:
                dt_fim = datetime.strptime(
                    data_abertura_fim, '%d/%m/%Y').date()
                query = query.filter(Empresa.data_abertura <= dt_fim)
            except Exception:
                print("Data inválida para Data de Abertura Fim.")

        print("\nExecutando consulta...")
        resultados = query.all()
        print(f"\nForam encontrados {len(resultados)} resultados.\n")

        empresas_obj = []
        for empresa in resultados:
            empresa_dict = {
                "cnpj": empresa.cnpj,
                "nome_empresarial": empresa.nome_empresarial,
                "nome_fantasia": empresa.nome_fantasia,
                "capital_social": float(empresa.capital_social) if empresa.capital_social is not None else None,
                "uf": empresa.uf,
                "data_abertura": empresa.data_abertura.strftime('%d/%m/%Y') if empresa.data_abertura else None,
            }
            empresas_obj.append(empresa_dict)

        print("Empresas armazenadas no objeto:")
        print(empresas_obj)
        session.close()
        return empresas_obj

    opcao = input(
        "\nDeseja realizar uma consulta filtrada? (sim/não): ").strip().lower()
    if opcao in ["sim", "s"]:
        filtrar_empresas()
