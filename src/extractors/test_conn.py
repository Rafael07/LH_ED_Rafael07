import psycopg2
import os
from datetime import datetime
import traceback

def get_table_names(cursor):
    """Obtém todas as tabelas do schema public"""
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables
        WHERE table_schema = 'public'
    """)
    return [name[0] for name in cursor.fetchall()]

def extract_tables():
    try:
        print("Conectando ao banco...")
        conn = psycopg2.connect(
            host="localhost",
            database="northwind",
            user="northwind_user",
            password="thewindisblowing",
            port=5432
        )
        cursor = conn.cursor()
        
        # Data para organização dos arquivos
        date_path = datetime.now().strftime('%Y-%m-%d')
        
        # Extrair cada tabela
        tables = get_table_names(cursor)
        print(f"Tabelas encontradas: {tables}")
        
        for table in tables:
            print(f"Extraindo tabela: {table}")
            
            # Criar diretório se não existir
            output_path = f"data/silver/{date_path}/{table}"
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Comando COPY do PostgreSQL
            sql = f"COPY (SELECT * FROM {table}) TO STDOUT WITH CSV HEADER"
            
            # Extrair dados
            with open(f"{output_path}.csv", 'w') as f:
                cursor.copy_expert(sql, f)
            
            print(f"Tabela {table} extraída com sucesso!")
        
        cursor.close()
        conn.close()
        print("Processo concluído!")
        
    except Exception as e:
        print(f"Erro: {str(e)}")
        print("\nTraceback completo:")
        print(traceback.format_exc())
        raise  # Re-lança a exceção para ver a stack trace completa

if __name__ == "__main__":
    try:
        extract_tables()
    except Exception as e:
        print(f"\nErro principal: {str(e)}")