import psycopg
import pandas as pd
import pyarrow
from datetime import datetime
import os

def connect_to_db():
    """Conecta ao banco de dados"""
    try:
        conn = psycopg.connect(
            host="localhost",
            dbname="northwind",
            user="northwind_user",
            password="thewindisblowing",
            port=5432
        )
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        raise

def get_tables(conn):
    """Lista todas as tabelas do banco de dados"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'               
        """)
        tables = [table[0] for table in cursor.fetchall()]
        cursor.close()
        return tables
    except Exception as e:
        print(f"Erro ao listar as tabelas: {e}")
        raise

def extract_data(conn, table_name, date_path):
    """Extrai os dados da tabela e salva em arquivo"""
    try:
        print(f"Extraindo tabela: {table_name}")

        # Criação dos diretório de cada tabela
        bronze_path = os.path.join("data", "bronze", "postgres", table_name, date_path)
        os.makedirs(bronze_path, exist_ok=True)

        # Extração dos dados
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, conn)

        # Salvar em arquivo parquet
        output_file = f"{bronze_path}/{table_name}.parquet"
        df.to_parquet(
            output_file,
            engine="pyarrow",
            compression="snappy",
            index=False
        )

        print(f"Tabela {table_name} salva em {output_file}")
        return True
    except Exception as e:
        print(f"Erro ao extrair tabela {table_name}: {e}")
        return False
    
def extract_postgresdb():
    """Extrai os dados do banco de dados"""
    try:
        # Recuperar data corrente
        date_path = datetime.now().strftime("%Y-%m-%d")        

        # Conecta ao banco de dados
        conn = connect_to_db()

        # Obtém lista de todas as tabelas
        tables = get_tables(conn)
        print(f"Tabelas encontradas: {tables}")

        # Extrair os dados de cada tabela
        result = []
        for table in tables:
            success = extract_data(conn, table, date_path)
            result.append((table, success))
        
        # Fecha a conexão ao banco de dados
        conn.close()

        # Relatório do processo de extração
        print("\nRelatório do processo de extração:")
        for table, success in result:
            status = "Sucesso" if success else "Falha"
            print(f"Tabela {table}: {status}")

    except Exception as e:
        print(f"Erro no processo de extração: {e}")
        raise

if __name__ == "__main__":
    print("Iniciando extracao de dados...")
    extract_postgresdb()    
    print("Processo encerrado!!!")