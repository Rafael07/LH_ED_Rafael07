import pandas as pd
import psycopg2
from datetime import datetime
import os
import csv
import io

def extract_from_postgres():
    """Extrai as tabelas do banco de dados PostgreSQL"""
    try:
        # Conectar ao banco de dados
        conn = psycopg2.connect(
            host="source_db",
            database="northwind",
            user="northwind_user",
            password="thewindisblowing",
            port=5432
        )
        
        # Query que retorna as tabelas
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE';
        """)
        tables = [table[0] for table in cursor.fetchall()]
        
        # Data para organizar os arquivos
        date_path = datetime.now().strftime("%Y-%m-%d")
        silver_path = f'data/silver/{date_path}'
        os.makedirs(silver_path, exist_ok=True)

        print(f"Tabelas encontradas: {tables}")

        # Extrair cada tabela
        for table in tables:
            print(f"Extraindo tabela: {table}")
            
            # Usar StringIO para armazenar os dados em memória
            output = io.StringIO()
            
            # Copiar dados para o StringIO
            cursor.copy_expert(f"COPY {table} TO STDOUT WITH CSV HEADER", output)
            
            # Voltar para o início do StringIO
            output.seek(0)
            
            # Ler com pandas
            df = pd.read_csv(output)
            
            # Salvar como parquet
            df.to_parquet(
                f"{silver_path}/{table}.parquet",
                engine="pyarrow"
            )
            
            # Fechar o StringIO
            output.close()

        cursor.close()
        conn.close()
        print("Dados extraídos do PostgreSQL com sucesso!")

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
        print("Dados extraídos do CSV com sucesso!")

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