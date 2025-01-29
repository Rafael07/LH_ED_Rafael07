# test_conn.py
import psycopg2
import traceback

def test_minimal():
    try:
        print("Teste 1: Tentando conectar via localhost...")
        print("Host: localhost")
        print("Port: 5432")
        print("Database: northwind")
        print("User: northwind_user")
        
        conn = psycopg2.connect(
            host="localhost",     # Mudando para localhost
            database="northwind",
            user="northwind_user",
            password="thewindisblowing",
            port=5432
        )
        print("Conexão OK!")
        
        # Testar uma query simples
        cur = conn.cursor()
        cur.execute("SELECT * FROM categories LIMIT 1;")
        row = cur.fetchone()
        print(f"Dados: {row}")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Erro: {str(e)}")
        print("Traceback completo:")
        print(traceback.format_exc())

if __name__ == "__main__":
    test_minimal()