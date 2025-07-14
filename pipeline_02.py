import os
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from typing import List

from duckdb import DuckDBPyRelation
from pandas import DataFrame
from datetime import datetime

load_dotenv()

def conectar_banco():
    """Conecta ao banco de dados Duckdb; cria o banco se não existir."""
    return duckdb.connect(database='duckdb.db', read_only=False)

def inicializar_tabela(con):
    """Cria a tabela se ela não existir."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS historico_arquivos (
            nome_arquivo VARCHAR,
            horario_processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
def registrar_arquivo(con, nome_arquivo):
    """Registra o nome do arquivo processado e o horário."""
    con.execute("""
        INSERT INTO historico_arquivos (nome_arquivo, horario_processamento)
        VALUES (?, ?)
    """, (nome_arquivo, datetime.now()))

def arquivos_processados(con):
    """Retorna um set com os nomes dos arquivos já processados."""
    resultado = con.execute("SELECT nome_arquivo FROM historico_arquivos").fetchall()
    return {row[0] for row in resultado}

# Função para listar arquivos .csv da pasta local
def listar_arquivos_e_tipos(diretorio: str) -> List[str]:
    """Lista arquivos e identifica se são CSV, JSON ou Parquet."""
    arquivos_e_tipos = []
    todos_os_arquivos = os.listdir(diretorio)
    for arquivo in todos_os_arquivos:
        if arquivo.endswith('.csv') or arquivo.endswith('.json') or arquivo.endswith('.parquet'):
            caminho_completo = os.path.join(diretorio, arquivo)
            tipo = arquivo.split('.')[-1]
            arquivos_e_tipos.append((caminho_completo, tipo))
    return arquivos_e_tipos

# Função para ler um arquivo CSV usando duckdb e retornar um DataFrame do duckdb
def ler_arquivo(caminho_arquivos, tipo) -> DuckDBPyRelation:
    """Lê o arquivo de acordo com o seu tipo e retorna um DataFrame do DuckDB.""",
    if tipo == 'csv':
        return duckdb.read_csv(caminho_arquivos)
    elif tipo == 'json':
        return duckdb.read_json(caminho_arquivos)
    elif tipo == 'parquet':
        return duckdb.read_parquet(caminho_arquivos)
    else:
        raise ValueError(f"Tipo de arquivo {tipo} não suportado. Use csv, json ou parquet.")

 # Transformação do df_duckdb para um df_Pandas
def transformar(df: DuckDBPyRelation) -> DataFrame:
    df_transformado = duckdb.sql("SELECT *, (quantidade * valor) AS total_vendas FROM df").df()
    return df_transformado

# Função para salvar o df_Pandas no PostgreSQL
def salvar_no_postgres(df_pandas, tabela):
    # Carregar as variáveis de ambiente
    usuario = os.getenv('POSTGRES_USER')
    senha = os.getenv('POSTGRES_PASSWORD')
    host = os.getenv('POSTGRES_HOST')
    porta = os.getenv('POSTGRES_PORT')
    banco = os.getenv('POSTGRES_DB')

    # Criar a string de conexão
    conexao_str = f'postgresql://{usuario}:{senha}@{host}:{porta}/{banco}'
    
    # Criar o engine do SQLAlchemy
    engine = create_engine(conexao_str)

    # Salvar o DataFrame Pandas no PostgreSQL
    df_pandas.to_sql(tabela, con=engine, if_exists='append', index=False)


# Execução principal
def pipeline() -> List[str]:
    """Executa o pipeline de processamento de arquivos e retorna logs."""
    diretorio_local = './data'

    con = conectar_banco()
    inicializar_tabela(con)
    arquivos_já_processados = arquivos_processados(con)
    arquivos_e_tipos = listar_arquivos_e_tipos(diretorio_local)

    logs = []
    for caminho_do_arquivo, tipo in arquivos_e_tipos:
        nome_arquivo = os.path.basename(caminho_do_arquivo)
        if nome_arquivo not in arquivos_já_processados:
            duckdb_df = ler_arquivo(caminho_do_arquivo, tipo)
            pandas_df_transformado = transformar(duckdb_df)
            salvar_no_postgres(pandas_df_transformado, 'vendas_calculado')
            registrar_arquivo(con, nome_arquivo)
            print(f"Arquivo {nome_arquivo} processado e salvo na tabela 'vendas_calculado'.")
            logs.append(f"Arquivo {nome_arquivo} processado com sucesso.")
        else:
            print(f"Arquivo {nome_arquivo} já foi processado anteriormente.")
            logs.append(f"Arquivo {nome_arquivo} já processado anteriormente.")
    con.close()
    return logs


if __name__ == "__main__":
    pipeline()

