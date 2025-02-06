import os
import sys
import psycopg
import pandas as pd
from datetime import datetime

root_dir = os.getenv('DATA_PATH')
def export_orders_to_csv(execution_date):
    # Conexão com o banco de dados
    conn = psycopg.connect(
        dbname="northwind_target",
        user="target_user",
        password="thewindkeepsblowing",
        host="localhost",
        port="5433"
    )
    
    # Criação do cursor
    cur = conn.cursor()
    
    # Consulta SQL
    query = """
    SELECT 
        o.order_id,
        o.customer_id,
        o.order_date,
        od.product_id,
        od.quantity,
        od.unit_price
    FROM 
        orders o
    JOIN 
        order_details od ON o.order_id = od.order_id
    """
    
    # Executar a consulta
    cur.execute(query)
    
    # Obter os resultados
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    
    # Criar DataFrame
    df = pd.DataFrame(rows, columns=columns)
    
    # Nome do arquivo CSV
    output_dir = f"{root_dir}/data/gold/{execution_date}"
    os.makedirs(output_dir, exist_ok=True)
    csv_file = f"{output_dir}/final_query_orders_details.csv"
    
    # Escrever os resultados no arquivo CSV
    df.to_csv(csv_file, index=False)
    
    # Fechar cursor e conexão
    cur.close()
    conn.close()
    
    print(f"Dados exportados com sucesso para {csv_file}")

if __name__ == "__main__":
    execution_date = sys.argv[1]  # Recebe a data de execução como argumento
    export_orders_to_csv(execution_date)