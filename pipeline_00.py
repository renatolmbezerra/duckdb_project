import os
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from typing import List

from duckdb import DuckDBPyRelation
from pandas import DataFrame

load_dotenv()

# Função para listar arquivos .csv da pasta local
def listar_arquivos_csv(diretorio: str) -> List[str]:
    arquivos_csv = []
    todos_os_arquivos = os.listdir(diretorio)
    for arquivo in todos_os_arquivos:
        if arquivo.endswith('.csv'):
            caminho_completo = os.path.join(diretorio, arquivo)
            arquivos_csv.append(caminho_completo)
    return arquivos_csv

# Função para ler um arquivo CSV usando duckdb e retornar um DataFrame do duckdb
def ler_csv(caminho_arquivos):
    df_duckdb = duckdb.read_csv(caminho_arquivos)
    return df_duckdb

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
if __name__ == "__main__":
    diretorio_local = './data'
    lista_de_arquivos = listar_arquivos_csv(diretorio_local)
    for arquivo in lista_de_arquivos:
        duckdb_df = ler_csv(arquivo)
        pandas_df_transformado = transformar(duckdb_df)
        salvar_no_postgres(pandas_df_transformado, 'vendas_calculado')
        print(f"Arquivo {arquivo} processado e salvo na tabela 'vendas_calculado'.")

