import pandas as pd
import psycopg
from datetime import datetime
import os
import glob

def connect_to_target_db():
    """Conecta ao banco de dados de destino"""
    try:
        conn = psycopg.connect(
            host="localhost",
            dbname="northwind_target",
            user="target_user",
            password="thewindkeepsblowing",
            port=5433
        )
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao banco destino: {e}")
        raise

def load_ar