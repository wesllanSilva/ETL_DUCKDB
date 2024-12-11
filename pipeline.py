import os
import gdown
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

from duckdb import DuckDBPyRelation
from pandas import DataFrame

from datetime import datetime



def baixar_arquivos_google_drive(url_pasta, diretorio_local):
    os.makedirs(diretorio_local, exist_ok=True)
    gdown.download_folder(url_pasta, output=diretorio_local, quiet=False, use_cookies=False)

def listar_arquivos_csv(diretorio):
    arquivos_csv = []
    todos_os_arquivos = os.listdir(diretorio)
    for arquivo in todos_os_arquivos:
        if arquivo.endswith(".csv"):
            caminho_completo = os.path.join(diretorio, arquivo)
            arquivos_csv.append(caminho_completo)
    return arquivos_csv


if __name__ == "__main__":
    url_pasta = 'https://drive.google.com/drive/folders/1QIrSU88_aAxeVagZ-fiU-8jtKTR0xs3x'
    diretorio_local = './pasta_gdown'
    # baixar_arquivos_google_drive(url_pasta,diretorio_local)
    listar_arquivos_csv(diretorio_local)