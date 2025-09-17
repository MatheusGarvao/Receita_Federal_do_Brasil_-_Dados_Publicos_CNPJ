# ETL_coletar_dados_e_gravar_BD.py
# Completo • Mantém features originais • Acelera com COPY • Tabelas finais TIPADAS

import os
import re
import sys
import time
import pathlib
import zipfile
import urllib.request
from urllib.parse import urljoin

import requests
import wget
import bs4 as bs
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# ===================== CONTROLES =====================
RUN_DOWNLOAD = False    # baixa os .zip da RFB
RUN_EXTRACT  = False    # extrai os .zip para EXTRACTED_FILES_PATH
RUN_LOAD     = True     # carrega para o Postgres a partir de EXTRACTED_FILES_PATH

# método de carga (mantém seus fluxos + adiciona COPY)
# "copy"  -> mais rápido (recomendado)
# "pandas"-> usa seus reads com pandas (inclui chunk ESTABELE e split SIMPLES)
LOAD_METHOD = "copy"

# performance extra
CREATE_UNLOGGED = True          # staging e finais como UNLOGGED durante a carga
MAKE_LOGGED_AFTER_LOAD = True   # promover finais para LOGGED ao final
# ====================================================


# ------------------ UTILIDADES ------------------
def listar_zips_locais(output_dir: str):
    if not output_dir or not os.path.isdir(output_dir):
        return []
    return sorted([f for f in os.listdir(output_dir) if f.lower().endswith('.zip')])

def listar_extraidos(extracted_dir: str):
    if not extracted_dir or not os.path.isdir(extracted_dir):
        return []
    return sorted([name for name in os.listdir(extracted_dir) if name.endswith('')])

def check_diff(url, file_name):
    """Se o remoto (content-length) difere do local, retorna True (para baixar)."""
    if not os.path.isfile(file_name):
        return True
    try:
        resp = requests.head(url, timeout=30, allow_redirects=True)
        if resp.status_code != 200:
            return True
        new_size = int(resp.headers.get('content-length', 0))
    except Exception:
        return True
    old_size = os.path.getsize(file_name)
    if new_size and new_size != old_size:
        try:
            os.remove(file_name)
        except Exception:
            pass
        return True
    return False

def bar_progress(current, total, width=80):
    progress_message = "Downloading: %d%% [%d / %d] bytes - " % (current / total * 100, current, total)
    sys.stdout.write("\r" + progress_message)
    sys.stdout.flush()

def makedirs(path):
    if path and not os.path.exists(path):
        os.makedirs(path)

def getEnv(env):
    return os.getenv(env)

# Mantido: sua função to_sql (não é usada no modo COPY, mas fica disponível)
def to_sql(dataframe, **kwargs):
    size = 4096
    total = len(dataframe)
    name = kwargs.get('name')
    def chunker(df):
        return (df[i:i + size] for i in range(0, len(df), size))
    for i, df in enumerate(chunker(dataframe)):
        df.to_sql(**kwargs)
        index = (i + 1) * size
        if index > total:
            index = total
        percent = (index * 100) / total if total else 100
        progress = f'{name} {percent:.2f}% {index:0{len(str(total))}}/{total}'
        sys.stdout.write(f'\r{progress}')
    sys.stdout.write('\n')

# ------------------ ENGINE / SESSÃO ------------------
def criar_engine_pg(user: str, password: str, host: str, port: int | str, database: str):
    user = (user or "").strip()
    password = (password or "").strip()
    host = (host or "").strip()
    database = (database or "").strip()
    try:
        port = int(str(port).strip())
    except Exception:
        port = 5432

    os.environ.setdefault("PGCLIENTENCODING", "UTF8")

    engine_url = URL.create(
        "postgresql+psycopg2",
        username=user, password=password, host=host, port=port, database=database
    )
    # pool_pre_ping melhora estabilidade
    return create_engine(engine_url, pool_pre_ping=True)

def set_session_speed_hints(engine):
    with engine.begin() as conn:
        conn.exec_driver_sql("SET synchronous_commit = off;")
        conn.exec_driver_sql("SET work_mem = '256MB';")
        conn.exec_driver_sql("SET maintenance_work_mem = '2097151kB';")
        conn.exec_driver_sql("SET temp_buffers = '256MB';")

# ------------------ COPY RÁPIDO ------------------
def copy_from_file(engine, table: str, filepath: str, columns: list[str],
                   file_encoding: str = "cp1252", chunk_size: int = 8 * 1024 * 1024):
    """
    COPY FROM STDIN com limpeza de bytes NUL (\x00) em streaming.
    - file_encoding: 'cp1252' é mais permissivo que latin-1 p/ dados RFB em Windows.
    - não usamos ENCODING no COPY: psycopg2 envia UTF-8 e o servidor converte.
    """
    cols = ", ".join(columns)
    copy_sql = f"""
        COPY {table} ({cols})
        FROM STDIN
        WITH (FORMAT CSV, DELIMITER ';', QUOTE '"', ESCAPE '"', HEADER FALSE)
    """

    class SanitizedReader:
        def __init__(self, f, chunk):
            self.f = f
            self.chunk = chunk
        def read(self, size=-1):
            # lê em blocos, remove NUL e devolve str (psycopg2 envia como UTF-8)
            data = self.f.read(self.chunk if size == -1 else size)
            if not data:
                return ""
            # remove \x00 (e eventuais outros control chars problemáticos, se quiser)
            data = data.replace('\x00', '')
            return data

    raw = engine.raw_connection()
    try:
        with open(filepath, "r", encoding=file_encoding, errors="replace", newline="") as fh:
            cur = raw.cursor()
            cur.copy_expert(copy_sql, SanitizedReader(fh, chunk_size))
        raw.commit()
    finally:
        try: raw.close()
        except: pass


def drop_create_staging(engine, table: str, columns: list[str]):
    cols = ", ".join([f"{c} text" for c in columns])
    unlogged = "UNLOGGED" if CREATE_UNLOGGED else ""
    ddl = f"""
        DROP TABLE IF EXISTS stg_{table};
        CREATE {unlogged} TABLE stg_{table} ({cols});
        ALTER TABLE stg_{table} SET (autovacuum_enabled = off);
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(ddl)

def make_logged(engine, table: str):
    if MAKE_LOGGED_AFTER_LOAD:
        with engine.begin() as conn:
            conn.exec_driver_sql(f"ALTER TABLE {table} SET LOGGED;")

# ------------------ HELPERS SQL (tipagens) ------------------
def sql_date(expr: str) -> str:
    # valores 'AAAAMMDD' → DATE | ''/00000000 → NULL
    return f"CASE WHEN {expr} ~ '^[0-9]{{8}}$' AND {expr} <> '00000000' THEN to_date({expr}, 'YYYYMMDD') ELSE NULL END"

def sql_int(expr: str) -> str:
    return f"NULLIF({expr}, '')::int"

def sql_num(expr: str, scale: int = 2) -> str:
    return f"NULLIF(REPLACE({expr}, ',', '.'), '')::numeric(18,{scale})"

def sql_char(expr: str, n: int) -> str:
    return f"NULLIF({expr}, '')::char({n})"

def sql_text(expr: str) -> str:
    return f"NULLIF({expr}, '')"

# ------------------ PROCESSO ------------------
if __name__ == "__main__":
    # -------- .env --------
    current_path = pathlib.Path().resolve()
    dotenv_path = os.path.join(current_path, '.env')
    if not os.path.isfile(dotenv_path):
        print(r'Especifique o local do seu arquivo de configuração ".env". Por exemplo: C:\...\Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ\code')
        local_env = input()
        dotenv_path = os.path.join(local_env, '.env')
    print(dotenv_path)
    load_dotenv(dotenv_path=dotenv_path, encoding="utf-8")

    # Base do mês (com barra no final!)
    dados_rf = 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-09/'

    # Diretórios
    output_files     = getEnv('OUTPUT_FILES_PATH');     makedirs(output_files)
    extracted_files  = getEnv('EXTRACTED_FILES_PATH');  makedirs(extracted_files)
    print('Diretórios definidos: \n' +
          'output_files: ' + str(output_files)  + '\n' +
          'extracted_files: ' + str(extracted_files))
    print(f'RUN_DOWNLOAD={RUN_DOWNLOAD} | RUN_EXTRACT={RUN_EXTRACT} | RUN_LOAD={RUN_LOAD} | LOAD_METHOD={LOAD_METHOD}')

    # -------- LISTAGEM DE ZIPS --------
    Files = []
    if RUN_DOWNLOAD:
        try:
            raw_html = urllib.request.urlopen(dados_rf).read()
            page_items = bs.BeautifulSoup(raw_html, 'lxml')
            html_str = str(page_items)

            tmp = []
            for m in re.finditer('.zip', html_str):
                i_start = m.start() - 40
                i_end = m.end()
                i_loc = html_str[i_start:i_end].find('href=') + 6
                tmp.append(html_str[i_start + i_loc:i_end])

            Files_clean = []
            for x in tmp:
                if not x.find('.zip">') > -1:
                    Files_clean.append(x)
            Files = Files_clean

            print('Arquivos que serão baixados:')
            for i, f in enumerate(Files, start=1):
                print(f'{i} - {f}')
        except Exception as e:
            print(f'Falha ao listar remoto: {e}')
            Files = []
    else:
        Files = listar_zips_locais(output_files)
        print('Usando ZIPs locais já baixados:')
        for i, f in enumerate(Files, start=1):
            print(f'{i} - {f}')

    # -------- DOWNLOAD --------
    if RUN_DOWNLOAD:
        i_l = 0
        for l in Files:
            i_l += 1
            print('Baixando arquivo:')
            print(str(i_l) + ' - ' + l)
            url = urljoin(dados_rf, l)
            file_name = os.path.join(output_files, l)
            if check_diff(url, file_name):
                wget.download(url, out=output_files, bar=bar_progress)
        print('\nDownload finalizado.')

    # -------- EXTRAÇÃO --------
    if RUN_EXTRACT:
        i_l = 0
        for l in Files:
            try:
                i_l += 1
                print('Descompactando arquivo:')
                print(str(i_l) + ' - ' + l)
                full_path = os.path.join(output_files, l)
                with zipfile.ZipFile(full_path, 'r') as zip_ref:
                    zip_ref.extractall(extracted_files)
            except Exception as e:
                print(f'Falha ao descompactar {l}: {e}')
        print('Extração finalizada.')

    # -------- CARGA --------
    if not RUN_LOAD:
        print('RUN_LOAD=False. Encerrando sem carregar dados.')
        sys.exit(0)

    insert_start = time.time()

    # Files extraídos:
    Items = listar_extraidos(extracted_files)

    # Separar por domínio:
    arquivos_empresa         = sorted([it for it in Items if 'EMPRE'     in it])
    arquivos_estabelecimento = sorted([it for it in Items if 'ESTABELE'  in it])
    arquivos_socios          = sorted([it for it in Items if 'SOCIO'     in it])
    arquivos_simples         = sorted([it for it in Items if 'SIMPLES'   in it])
    arquivos_cnae            = sorted([it for it in Items if 'CNAE'      in it])
    arquivos_moti            = sorted([it for it in Items if 'MOTI'      in it])
    arquivos_munic           = sorted([it for it in Items if 'MUNIC'     in it])
    arquivos_natju           = sorted([it for it in Items if 'NATJU'     in it])
    arquivos_pais            = sorted([it for it in Items if 'PAIS'      in it])
    arquivos_quals           = sorted([it for it in Items if 'QUALS'     in it])

    # Engine + tunings
    engine = criar_engine_pg(
        user=getEnv('DB_USER'),
        password=getEnv('DB_PASSWORD'),
        host=getEnv('DB_HOST'),
        port=getEnv('DB_PORT'),
        database=getEnv('DB_NAME'),
    )
    set_session_speed_hints(engine)

    # ===== EMPRESA =====
    print("""
#######################
## Arquivos de EMPRESA:
#######################
""")
    empresa_cols = [
        'cnpj_basico','razao_social','natureza_juridica','qualificacao_responsavel',
        'capital_social','porte_empresa','ente_federativo_responsavel'
    ]
    with engine.begin() as conn:
        conn.exec_driver_sql('DROP TABLE IF EXISTS "empresa";')
    drop_create_staging(engine, "empresa", empresa_cols)

    if LOAD_METHOD == "copy":
        for f in arquivos_empresa:
            print('COPY para STAGING empresa: ' + f)
            copy_from_file(engine, "stg_empresa", os.path.join(extracted_files, f), empresa_cols)
    else:
        for f in arquivos_empresa:
            print('Lendo com pandas (empresa): ' + f)
            empresa_dtypes = {0: object, 1: object, 2: 'Int32', 3: 'Int32', 4: object, 5: 'Int32', 6: object}
            df = pd.read_csv(
                filepath_or_buffer=os.path.join(extracted_files, f),
                sep=';', header=None, dtype=empresa_dtypes, encoding='latin-1'
            )
            df.columns = empresa_cols
            # grava em staging via COPY temporário (csv em disco) para manter performance
            tmp_csv = os.path.join(extracted_files, f"{f}.tmp.csv")
            df.to_csv(tmp_csv, sep=';', index=False, header=False)
            copy_from_file(engine, "stg_empresa", tmp_csv, empresa_cols)
            os.remove(tmp_csv)

    unlogged = "UNLOGGED" if CREATE_UNLOGGED else ""
    sql_empresa = f"""
        DROP TABLE IF EXISTS empresa;
        CREATE {unlogged} TABLE empresa AS
        SELECT
            {sql_char('cnpj_basico', 8)}          AS cnpj_basico,
            {sql_text('razao_social')}            AS razao_social,
            {sql_int('natureza_juridica')}        AS natureza_juridica,
            {sql_int('qualificacao_responsavel')} AS qualificacao_responsavel,
            {sql_num('capital_social', 2)}        AS capital_social,
            {sql_int('porte_empresa')}            AS porte_empresa,
            {sql_text('ente_federativo_responsavel')} AS ente_federativo_responsavel
        FROM stg_empresa;
        ANALYZE empresa;
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(sql_empresa)
    make_logged(engine, "empresa")

    # ===== ESTABELECIMENTO =====
    print("""
###############################
## Arquivos de ESTABELECIMENTO:
###############################
""")
    with engine.begin() as conn:
        conn.exec_driver_sql('DROP TABLE IF EXISTS "estabelecimento";')

    estab_cols = [
        "cnpj_basico","cnpj_ordem","cnpj_dv","identificador_matriz_filial",
        "nome_fantasia","situacao_cadastral","data_situacao_cadastral","motivo_situacao_cadastral",
        "nome_cidade_exterior","pais","data_inicio_atividade","cnae_fiscal_principal",
        "cnae_fiscal_secundaria","tipo_logradouro","logradouro","numero","complemento",
        "bairro","cep","uf","municipio","ddd_1","telefone_1","ddd_2","telefone_2",
        "ddd_fax","fax","correio_eletronico","situacao_especial","data_situacao_especial"
    ]
    drop_create_staging(engine, "estabelecimento", estab_cols)
    print('Tem %i arquivos de estabelecimento!' % len(arquivos_estabelecimento))

    if LOAD_METHOD == "copy":
        for f in arquivos_estabelecimento:
            print('COPY para STAGING estabelecimento: ' + f)
            copy_from_file(engine, "stg_estabelecimento", os.path.join(extracted_files, f), estab_cols)
    else:
        # caminho "pandas" mantém seu chunk gigante
        for f in arquivos_estabelecimento:
            print('Trabalhando no arquivo: ' + f + ' [...]')
            estabelecimento_dtypes = {
                0: object, 1: object, 2: object, 3: 'Int32', 4: object, 5: 'Int32', 6: 'Int32',
                7: 'Int32', 8: object, 9: object, 10: 'Int32', 11: 'Int32', 12: object, 13: object,
                14: object, 15: object, 16: object, 17: object, 18: object, 19: object,
                20: 'Int32', 21: object, 22: object, 23: object, 24: object, 25: object,
                26: object, 27: object, 28: object, 29: 'Int32'
            }
            NROWS = 2_000_000
            part = 0
            while True:
                df = pd.read_csv(
                    filepath_or_buffer=os.path.join(extracted_files, f),
                    sep=';', nrows=NROWS, skiprows=NROWS * part,
                    header=None, dtype=estabelecimento_dtypes, encoding='latin-1'
                )
                if df.empty:
                    break
                df.columns = estab_cols
                tmp_csv = os.path.join(extracted_files, f"{f}.part{part}.tmp.csv")
                df.to_csv(tmp_csv, sep=';', index=False, header=False)
                copy_from_file(engine, "stg_estabelecimento", tmp_csv, estab_cols)
                os.remove(tmp_csv)
                print('Arquivo ' + f + ' / ' + str(part) + ' inserido (staging).')
                if len(df) == NROWS:
                    part += 1
                else:
                    break

    sql_estab = f"""
        DROP TABLE IF EXISTS estabelecimento;
        CREATE {unlogged} TABLE estabelecimento AS
        SELECT
            {sql_char('cnpj_basico', 8)}                    AS cnpj_basico,
            {sql_char('cnpj_ordem', 4)}                     AS cnpj_ordem,
            {sql_char('cnpj_dv', 2)}                        AS cnpj_dv,
            {sql_int('identificador_matriz_filial')}        AS identificador_matriz_filial,
            {sql_text('nome_fantasia')}                     AS nome_fantasia,
            {sql_int('situacao_cadastral')}                 AS situacao_cadastral,
            {sql_date('data_situacao_cadastral')}           AS data_situacao_cadastral,
            {sql_int('motivo_situacao_cadastral')}          AS motivo_situacao_cadastral,
            {sql_text('nome_cidade_exterior')}              AS nome_cidade_exterior,
            {sql_int('pais')}                               AS pais,
            {sql_date('data_inicio_atividade')}             AS data_inicio_atividade,
            {sql_int('cnae_fiscal_principal')}              AS cnae_fiscal_principal,
            {sql_text('cnae_fiscal_secundaria')}            AS cnae_fiscal_secundaria,
            {sql_text('tipo_logradouro')}                   AS tipo_logradouro,
            {sql_text('logradouro')}                        AS logradouro,
            {sql_text('numero')}                            AS numero,
            {sql_text('complemento')}                       AS complemento,
            {sql_text('bairro')}                            AS bairro,
            {sql_char('cep', 8)}                            AS cep,
            {sql_char('uf', 2)}                             AS uf,
            {sql_int('municipio')}                          AS municipio,
            {sql_text('ddd_1')}                             AS ddd_1,
            {sql_text('telefone_1')}                        AS telefone_1,
            {sql_text('ddd_2')}                             AS ddd_2,
            {sql_text('telefone_2')}                        AS telefone_2,
            {sql_text('ddd_fax')}                           AS ddd_fax,
            {sql_text('fax')}                               AS fax,
            {sql_text('correio_eletronico')}                AS correio_eletronico,
            {sql_text('situacao_especial')}                 AS situacao_especial,
            {sql_date('data_situacao_especial')}            AS data_situacao_especial,
            -- cnpj completo pronto
            ({sql_char('cnpj_basico', 8)} || {sql_char('cnpj_ordem', 4)} || {sql_char('cnpj_dv', 2)})::char(14) AS cnpj
        FROM stg_estabelecimento;
        ANALYZE estabelecimento;
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(sql_estab)
    make_logged(engine, "estabelecimento")

    # ===== SOCIOS =====
    print("""
######################
## Arquivos de SOCIOS:
######################
""")
    with engine.begin() as conn:
        conn.exec_driver_sql('DROP TABLE IF EXISTS "socios";')

    socios_cols = [
        'cnpj_basico','identificador_socio','nome_socio_razao_social','cpf_cnpj_socio',
        'qualificacao_socio','data_entrada_sociedade','pais','representante_legal',
        'nome_do_representante','qualificacao_representante_legal','faixa_etaria'
    ]
    drop_create_staging(engine, "socios", socios_cols)

    if LOAD_METHOD == "copy":
        for f in arquivos_socios:
            print('COPY para STAGING socios: ' + f)
            copy_from_file(engine, "stg_socios", os.path.join(extracted_files, f), socios_cols)
    else:
        for f in arquivos_socios:
            print('Lendo com pandas (socios): ' + f)
            socios_dtypes = {
                0: object, 1: 'Int32', 2: object, 3: object, 4: 'Int32', 5: 'Int32', 6: 'Int32',
                7: object, 8: object, 9: 'Int32', 10: 'Int32'
            }
            df = pd.read_csv(os.path.join(extracted_files, f),
                             sep=';', header=None, dtype=socios_dtypes, encoding='latin-1')
            df.columns = socios_cols
            tmp_csv = os.path.join(extracted_files, f"{f}.tmp.csv")
            df.to_csv(tmp_csv, sep=';', index=False, header=False)
            copy_from_file(engine, "stg_socios", tmp_csv, socios_cols)
            os.remove(tmp_csv)

    sql_socios = f"""
        DROP TABLE IF EXISTS socios;
        CREATE {unlogged} TABLE socios AS
        SELECT
            {sql_char('cnpj_basico', 8)}                   AS cnpj_basico,
            {sql_int('identificador_socio')}               AS identificador_socio,
            {sql_text('nome_socio_razao_social')}          AS nome_socio_razao_social,
            {sql_text('cpf_cnpj_socio')}                   AS cpf_cnpj_socio,
            {sql_int('qualificacao_socio')}                AS qualificacao_socio,
            {sql_date('data_entrada_sociedade')}           AS data_entrada_sociedade,
            {sql_int('pais')}                              AS pais,
            {sql_text('representante_legal')}              AS representante_legal,
            {sql_text('nome_do_representante')}            AS nome_do_representante,
            {sql_int('qualificacao_representante_legal')}  AS qualificacao_representante_legal,
            {sql_int('faixa_etaria')}                      AS faixa_etaria
        FROM stg_socios;
        ANALYZE socios;
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(sql_socios)
    make_logged(engine, "socios")

    # ===== SIMPLES =====
    print("""
################################
## Arquivos do SIMPLES NACIONAL:
################################
""")
    with engine.begin() as conn:
        conn.exec_driver_sql('DROP TABLE IF EXISTS "simples";')

    simples_cols = [
        'cnpj_basico','opcao_pelo_simples','data_opcao_simples',
        'data_exclusao_simples','opcao_mei','data_opcao_mei','data_exclusao_mei'
    ]
    drop_create_staging(engine, "simples", simples_cols)

    if LOAD_METHOD == "copy":
        for f in arquivos_simples:
            print('COPY para STAGING simples: ' + f)
            copy_from_file(engine, "stg_simples", os.path.join(extracted_files, f), simples_cols)
    else:
        # mantém sua divisão em partes, com contagem de linhas
        for f in arquivos_simples:
            print('Lendo o arquivo ' + f + ' [...]')
            extracted_file_path = os.path.join(extracted_files, f)
            with open(extracted_file_path, "r", encoding="latin-1", errors="ignore") as fh:
                simples_length = sum(1 for _ in fh)
            print('Linhas no arquivo do Simples ' + f + ': ' + str(simples_length))

            tamanho_das_partes = 1_000_000
            partes = round(simples_length / tamanho_das_partes) if simples_length else 1
            nrows = tamanho_das_partes
            skiprows = 0
            print('Este arquivo será dividido em ' + str(partes) + ' partes para inserção no banco de dados')
            simples_dtypes = ({0: object, 1: object, 2: 'Int32', 3: 'Int32', 4: object, 5: 'Int32', 6: 'Int32'})

            for i in range(0, partes):
                print('Iniciando a parte ' + str(i + 1) + ' [...]')
                df = pd.read_csv(
                    filepath_or_buffer=extracted_file_path,
                    sep=';', nrows=nrows, skiprows=skiprows,
                    header=None, dtype=simples_dtypes, encoding='latin-1'
                )
                if df.empty:
                    break
                df.columns = simples_cols
                skiprows = skiprows + nrows
                tmp_csv = os.path.join(extracted_files, f"{f}.part{i}.tmp.csv")
                df.to_csv(tmp_csv, sep=';', index=False, header=False)
                copy_from_file(engine, "stg_simples", tmp_csv, simples_cols)
                os.remove(tmp_csv)
                print('Arquivo ' + f + ' inserido (staging)! - Parte ' + str(i + 1))

    sql_simples = f"""
        DROP TABLE IF EXISTS simples;
        CREATE {unlogged} TABLE simples AS
        SELECT
            {sql_char('cnpj_basico', 8)}        AS cnpj_basico,
            {sql_char('opcao_pelo_simples', 1)} AS opcao_pelo_simples,
            {sql_date('data_opcao_simples')}    AS data_opcao_simples,
            {sql_date('data_exclusao_simples')} AS data_exclusao_simples,
            {sql_char('opcao_mei', 1)}          AS opcao_mei,
            {sql_date('data_opcao_mei')}        AS data_opcao_mei,
            {sql_date('data_exclusao_mei')}     AS data_exclusao_mei
        FROM stg_simples;
        ANALYZE simples;
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(sql_simples)
    make_logged(engine, "simples")

    # ===== DIMENSÕES: CNAE, MOTI, MUNIC, NATJU, PAIS, QUALS =====
    def load_dim(table: str, files: list[str], codigo_text=False):
        print(f"""
##########################
## Arquivos de {table}:
##########################
""")
        with engine.begin() as conn:
            conn.exec_driver_sql(f'DROP TABLE IF EXISTS "{table}";')
        cols = ["codigo","descricao"]
        drop_create_staging(engine, table, cols)

        if LOAD_METHOD == "copy":
            for f in files:
                print(f'COPY para STAGING {table}: ' + f)
                copy_from_file(engine, f"stg_{table}", os.path.join(extracted_files, f), cols)
        else:
            for f in files:
                print('Lendo com pandas (' + table + '): ' + f)
                df = pd.read_csv(os.path.join(extracted_files, f),
                                 sep=';', header=None, dtype='object', encoding='latin-1')
                df.columns = cols
                tmp_csv = os.path.join(extracted_files, f"{f}.tmp.csv")
                df.to_csv(tmp_csv, sep=';', index=False, header=False)
                copy_from_file(engine, f"stg_{table}", tmp_csv, cols)
                os.remove(tmp_csv)

        if codigo_text:  # CNAE pode ter zeros à esquerda
            sql_dim = f"""
                DROP TABLE IF EXISTS {table};
                CREATE {unlogged} TABLE {table} AS
                SELECT {sql_text('codigo')} AS codigo,
                       {sql_text('descricao')} AS descricao
                FROM stg_{table};
                ANALYZE {table};
            """
        else:
            sql_dim = f"""
                DROP TABLE IF EXISTS {table};
                CREATE {unlogged} TABLE {table} AS
                SELECT {sql_int('codigo')} AS codigo,
                       {sql_text('descricao')} AS descricao
                FROM stg_{table};
                ANALYZE {table};
            """
        with engine.begin() as conn:
            conn.exec_driver_sql(sql_dim)
        make_logged(engine, table)

    load_dim("cnae",  arquivos_cnae,  codigo_text=True)
    load_dim("moti",  arquivos_moti)
    load_dim("munic", arquivos_munic)
    load_dim("natju", arquivos_natju)
    load_dim("pais",  arquivos_pais)
    load_dim("quals", arquivos_quals)

    # ===== REMOVER INATIVOS (pós-carga) =====
    print("""
    ##############################################
    ## Removendo inativos das tabelas finais [...]
    ##############################################
    """)
    unlogged_kw = "UNLOGGED" if CREATE_UNLOGGED else ""

    with engine.begin() as conn:
        # 1) 'estabelecimento' somente ativos (situacao_cadastral = 2)
        conn.exec_driver_sql(f"""
            DROP TABLE IF EXISTS estabelecimento_filtrado;
            CREATE {unlogged_kw} TABLE estabelecimento_filtrado AS
            SELECT *
            FROM estabelecimento
            WHERE situacao_cadastral = 2
            -- se quiser excluir "situação especial", descomente a linha abaixo
            --   AND (situacao_especial IS NULL OR situacao_especial = '')
            ;
            ANALYZE estabelecimento_filtrado;

            DROP TABLE estabelecimento;
            ALTER TABLE estabelecimento_filtrado RENAME TO estabelecimento;
        """)

        # 2) Manter só CNPJs básicos que tenham ao menos 1 estabelecimento ativo
        conn.exec_driver_sql(f"""
            -- EMPRESA
            DROP TABLE IF EXISTS empresa_filtrada;
            CREATE {unlogged_kw} TABLE empresa_filtrada AS
            SELECT e.*
            FROM empresa e
            WHERE EXISTS (
                SELECT 1 FROM estabelecimento est
                WHERE est.cnpj_basico = e.cnpj_basico
            );
            ANALYZE empresa_filtrada;
            DROP TABLE empresa;
            ALTER TABLE empresa_filtrada RENAME TO empresa;

            -- SOCIOS
            DROP TABLE IF EXISTS socios_filtrados;
            CREATE {unlogged_kw} TABLE socios_filtrados AS
            SELECT s.*
            FROM socios s
            WHERE EXISTS (
                SELECT 1 FROM estabelecimento est
                WHERE est.cnpj_basico = s.cnpj_basico
            );
            ANALYZE socios_filtrados;
            DROP TABLE socios;
            ALTER TABLE socios_filtrados RENAME TO socios;

            -- SIMPLES
            DROP TABLE IF EXISTS simples_filtrados;
            CREATE {unlogged_kw} TABLE simples_filtrados AS
            SELECT si.*
            FROM simples si
            WHERE EXISTS (
                SELECT 1 FROM estabelecimento est
                WHERE est.cnpj_basico = si.cnpj_basico
            );
            ANALYZE simples_filtrados;
            DROP TABLE simples;
            ALTER TABLE simples_filtrados RENAME TO simples;
        """)

    print("Inativos removidos. Prosseguindo para criação de índices...")

    # ===== ÍNDICES =====
    index_start = time.time()
    print("""
#######################################
## Criar índices na base de dados [...]
#######################################
""")
    with engine.begin() as conn:
        conn.exec_driver_sql('create index if not exists empresa_cnpj on empresa(cnpj_basico);')
        conn.exec_driver_sql('create index if not exists estabelecimento_cnpj on estabelecimento(cnpj_basico);')
        conn.exec_driver_sql('create index if not exists estabelecimento_cnpj_full on estabelecimento(cnpj);')
        conn.exec_driver_sql('create index if not exists socios_cnpj on socios(cnpj_basico);')
        conn.exec_driver_sql('create index if not exists simples_cnpj on simples(cnpj_basico);')
        conn.exec_driver_sql("ANALYZE empresa;")
        conn.exec_driver_sql("ANALYZE estabelecimento;")
        conn.exec_driver_sql("ANALYZE socios;")
        conn.exec_driver_sql("ANALYZE simples;")
        conn.exec_driver_sql("ANALYZE cnae;")
        conn.exec_driver_sql("ANALYZE moti;")
        conn.exec_driver_sql("ANALYZE munic;")
        conn.exec_driver_sql("ANALYZE natju;")
        conn.exec_driver_sql("ANALYZE pais;")
        conn.exec_driver_sql("ANALYZE quals;")

    print("""
############################################################
## Índices criados nas tabelas, para a coluna `cnpj_basico`:
   - empresa
   - estabelecimento (+ cnpj completo)
   - socios
   - simples
############################################################
""")
    index_end = time.time()
    index_time = round(index_end - index_start)
    print('Tempo para criar os índices (em segundos): ' + str(index_time))

    # (Opcional) limpar staging para liberar espaço
    with engine.begin() as conn:
        conn.exec_driver_sql("DROP TABLE IF EXISTS stg_empresa;")
        conn.exec_driver_sql("DROP TABLE IF EXISTS stg_estabelecimento;")
        conn.exec_driver_sql("DROP TABLE IF EXISTS stg_socios;")
        conn.exec_driver_sql("DROP TABLE IF EXISTS stg_simples;")
        conn.exec_driver_sql("DROP TABLE IF EXISTS stg_cnae;")
        conn.exec_driver_sql("DROP TABLE IF EXISTS stg_moti;")
        conn.exec_driver_sql("DROP TABLE IF EXISTS stg_munic;")
        conn.exec_driver_sql("DROP TABLE IF EXISTS stg_natju;")
        conn.exec_driver_sql("DROP TABLE IF EXISTS stg_pais;")
        conn.exec_driver_sql("DROP TABLE IF EXISTS stg_quals;")

    insert_end = time.time()
    Tempo_insert = round((insert_end - insert_start))

    print("""
#############################################
## Processo de carga dos arquivos finalizado!
#############################################
""")
    print('Tempo total de execução do processo de carga (em segundos): ' + str(Tempo_insert))

    print("""Processo 100% finalizado! Você já pode usar seus dados no BD!
 - Desenvolvido por: Aphonso Henrique do Amaral Rafael
 - Contribua com esse projeto aqui: https://github.com/aphonsoar/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ
""")
