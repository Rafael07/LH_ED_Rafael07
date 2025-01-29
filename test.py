import psycopg

def test_connection():
    try:
        print("Iniciando teste de conexão...")
        print("Conectando ao PostgreSQL...")

        psycopg.connect(
            host="localhost",
            dbname="northwind",
            user="northwind_user",
            password="thewindisblowing",
            port=5432
        )

        print("Conexão estabelecida!")
    except psycopg.OperationalError as e:
        print(f"Erro ao conectar ao PostgreSQL: {e}")

if __name__ == "__main__":
    test_connection()