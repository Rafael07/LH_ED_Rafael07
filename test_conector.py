import psycopg 

conn = psycopg.connect(
        host="localhost",
        dbname="northwind",
        user="northwind_user",
        password="thewindisblowing",
        port=5432
    )
conn.autocommit = True

cursor = conn.cursor()
cursor.execute("SELECT * FROM categories LIMIT 1;")        
print(cursor.fetchall())