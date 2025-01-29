import pandas as pd
import psycopg2 
from datetime import datetime
import os

def extract_from_postgres():
    """Extrai as tabelas do banco de dados PostgreSQL"""
    try:
        # Primeiro, configurar o encoding antes da conexão
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

        # Conectar ao banco de dados
        conn = psycopg2.connect(
            host="source_db",
            database="northwind",
            user="northwind_user",
            password="thewindisblowing",
            port=5432,
            options="-c client_encoding=UTF8"  # Matching the SQL file
        )

        # Configurar a sessão
        cursor = conn.cursor()
        cursor.execute("SET client_encoding TO 'UTF8';")
        
        # Query que retorna as tabelas
        tables_retrieve_query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE';
        """
        
        cursor.execute(tables_retrieve_query)
        tables = [table[0] for table in cursor.fetchall()]
        cursor.close()

        # Data para organizar os arquivos na estrutura de dados
        date_path = datetime.now().strftime("%Y-%m-%d")
        silver_path = f'data/silver/{date_path}'
        os.makedirs(silver_path, exist_ok=True)

        print(f"Tabelas extraídas: {tables}")

        # Iterar pelas tabelas e extrair os dados
        for table in tables:
            print(f"Extraindo dados da tabela: {table}")
            df = pd.read_sql(
                f"SELECT * FROM {table}", 
                conn,
                coerce_float=True
            )
            df.to_parquet(
                f"{silver_path}/{table}.parquet",
                engine="pyarrow"
            )

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

        df = pd.read_csv('data/bronze/order_details.csv')
        df.to_parquet(
            f"{silver_path}/order_details.parquet",
            engine="pyarrow"
        )

        print("Dados extraídos da planilha com sucesso!")

    except Exception as e:
        print(f"Erro durante processo de extração: {str(e)}")
        raise

if __name__ == "__main__":
    print("Iniciando a extração de dados...")
    try:
        extract_from_postgres()
        extract_from_csv()
        print("Extração de dados concluida com sucesso!")
    except Exception as e:
        print(f"Erro durante processo: {str(e)}")