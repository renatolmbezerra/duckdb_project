import os
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv


# Função para listar arquivos .csv da pasta local
def listar_arquivos_csv(diretorio):
    arquivos_csv = []
    for arquivo in os.listdir(diretorio):
        if arquivo.endswith('.csv'):
            arquivos_csv.append(arquivo)
    return arquivos_csv

# Função para ler um arquivo CSV usando duckdb
def ler_csv(caminho_arquivo):
    return duckdb.read_csv(caminho_arquivo)

# Função principal para empilhar todos os CSVs da pasta
def empilhar_csvs(diretorio):
    arquivos_csv = listar_arquivos_csv(diretorio)
    dataframes = []

    for nome_arquivo in arquivos_csv:
        caminho_completo = os.path.join(diretorio, nome_arquivo)
        df = ler_csv(caminho_completo)
        dataframes.append(df)

    if len(dataframes) > 0:
        df_concatenado = duckdb.sql("SELECT * FROM dataframes").df()
        return df_concatenado
    else:
        return pd.DataFrame()  # DataFrame vazio

# Execução principal
if __name__ == "__main__":
    diretorio_local = './data'
    arquivos = listar_arquivos_csv(diretorio_local)

    if len(arquivos) == 0:
        print("⚠️ Nenhum arquivo CSV encontrado.")
    else:
        print("📁 Arquivos encontrados:")
        for arquivo in arquivos:
            print(f" - {arquivo}")

        df_final = empilhar_csvs(diretorio_local)
        print("\n✅ Dados empilhados com sucesso.")
        print(df_final.head())