import pandas as pd
import psycopg2
from psycopg2 import extensions
from datetime import datetime
import os

def extract_from_postgres():
    """Extrai as tabelas do banco de dados PostgreSQL"""
    try:
        # Registrar tipos Unicode
        extensions.register_type(extensions.UNICODE)
        extensions.register_type(extensions.UNICODEARRAY)
        
        print("1")
        # Conectar ao banco de dados
        conn = psycopg2.connect(
            host="source_db",
            database="northwind",
            user="northwind_user",
            password="thewindisblowing",
            port=5432,
            client_encoding='UTF8'
        )
        
        print("2")
        # Configurar a sessão
        cursor = conn.cursor()
        cursor.execute('SET client_encoding TO UTF8;')
        
        # Query que retorna as tabelas
        tables_retrieve_query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE';
        """
        cursor.execute(tables_retrieve_query)
        tables = [table[0] for table in cursor.fetchall()]
        
        print(f"Tabelas extraídas: {tables}")

        # Data para organizar os arquivos
        date_path = datetime.now().strftime("%Y-%m-%d")
        silver_path = f'data/silver/{date_path}'
        os.makedirs(silver_path, exist_ok=True)

        # Iterar pelas tabelas e extrair os dados
        for table in tables:
            print(f"Extraindo dados da tabela: {table}")
            df = pd.read_sql_query(
                f"SELECT * FROM {table}",
                conn,
                coerce_float=True
            )
            df.to_parquet(f"{silver_path}/{table}.parquet", engine="fastparquet")

        cursor.close()
        conn.close()
        print("Dados extraídos do banco PostgreSQL com sucesso!")

    except Exception as e:
        print(f"Erro durante processo de extração: {str(e)}")
        raise

def extract_from_csv():
    """Extrai dados de arquivos CSV"""
    try:
        date_path = datetime.now().strftime("%Y-%m-%d")
        silver_path = f'data/silver/{date_path}'
        os.makedirs(silver_path, exist_ok=True)

        # Tentar diferentes abordagens de leitura
        try:
            df = pd.read_csv('data/bronze/order_details.csv', encoding='utf-8')
        except:
            try:
                df = pd.read_csv('data/bronze/order_details.csv', encoding='latin1')
            except:
                df = pd.read_csv('data/bronze/order_details.csv', encoding='iso-8859-1')
        
        df.to_parquet(f"{silver_path}/order_details.parquet", engine="fastparquet")
        print("Dados extraídos da planilha com sucesso!")

    except Exception as e:
        print(f"Erro durante processo de extração: {str(e)}")
        raise

if __name__ == "__main__":
    print("Iniciando a extração de dados...")
    try:
        extract_from_postgres()
        extract_from_csv()
        print("Extração de dados concluída com sucesso!")
    except Exception as e:
        print(f"Erro durante processo: {str(e)}")