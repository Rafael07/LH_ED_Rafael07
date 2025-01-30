import pandas as pd
from datetime import datetime 
import os
import glob

def extract_csv():
    """Extrai os dados do arquivo CSV"""
    try:
        print("Iniciando extração do arquivo CSV...")
        # Criação dos diretório datados
        date_path = datetime.now().strftime("%Y-%m-%d")
        bronze_path = os.path.join("data", "bronze", "csv", date_path)
        os.makedirs(bronze_path, exist_ok=True)

        # Listar os arquivos CSV no diretório
        csv_format = os.path.join("data", "origin", "*.csv") 
        csv_files = glob.glob(csv_format)

        if not csv_files:
            print("Nenhum arquivo CSV encontrado no diretório.")
            return False

        results = []
        for csv_file in csv_files:
            try:
                # Extrair o nome do arquivo sem extensão
                file_name = os.path.basename(csv_file)
                base_name = os.path.splitext(file_name)[0]

                print(f"Extraindo dados do arquivo: {file_name}")
            
                # Ler o arquivo CSV
                df = pd.read_csv(csv_file)
                # Salvar em arquivo parquet
                output_file = os.path.join(bronze_path, f"{base_name}.parquet")
                df.to_parquet(
                    output_file,
                    engine="pyarrow",
                    compression="snappy",
                    index=False
                )

                print(f"Arquivo CSV salvo em {output_file}")
                results.append((file_name, True))
            
            except Exception as e:
                print(f"Erro ao processar {file_name}: {e}")
                return False

        # Relatório do processo de extração
        print("\nRelatório do processo de extração:")
        for file_name, success in results:
            status = "Sucesso" if success else "Falha"
            print(f"Arquivo {file_name}: {status}")

        return all(success for _, success in results)

    except Exception as e:
        print(f"Erro na extração de dados CSV: {e}")
        return False

if __name__ == "__main__":
    extract_csv()
