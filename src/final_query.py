import psycopg
import csv

def export_orders_to_csv():
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
    
    # Nome do arquivo CSV
    csv_file = "final_query_orders_details.csv"
    
    # Escrever os resultados no arquivo CSV
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        # Escrever cabeçalhos
        writer.writerow([desc[0] for desc in cur.description])
        # Escrever dados
        writer.writerows(rows)
    
    # Fechar cursor e conexão
    cur.close()
    conn.close()
    
    print(f"Dados exportados com sucesso para {csv_file}")

if __name__ == "__main__":
    export_orders_to_csv()