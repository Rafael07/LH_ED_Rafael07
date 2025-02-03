import os
import subprocess
import argparse

def run_extraction_csv():
    root_dir = '/home/rafael/projects/indicium/LH_ED_Rafael07/'
    meltano_dir = '/home/rafael/projects/indicium/LH_ED_Rafael07/meltano_dataloader'
    command = ['meltano', 'run', 'tap-csv', 'target-csv']
    result = subprocess.run(command, cwd=meltano_dir, capture_output=True, text=True)
    if result.returncode == 0:
        print("Extração concluída com sucesso.")
    else:
        print("Erro na extração.")
        print(result.stderr)

def run_extraction_postgres():
    root_dir = '/home/rafael/projects/indicium/LH_ED_Rafael07/'
    meltano_dir = '/home/rafael/projects/indicium/LH_ED_Rafael07/meltano_dataloader'
    command = 'source .venv/bin/activate && meltano run tap-postgres target-csv-postgres'
    result = subprocess.run(command, cwd=meltano_dir, shell=True, executable='/bin/bash', capture_output=True, text=True)
    if result.returncode == 0:
        print("Extração concluída com sucesso.")
    else:
        print("Erro na extração.")
        print(result.stderr)

def load_to_target_db():
    root_dir = '/home/rafael/projects/indicium/LH_ED_Rafael07'
    bronze_dir = '/home/rafael/projects/indicium/LH_ED_Rafael07/data/bronze'
    
    # Listar arquivos na pasta bronze
    for subdir, _, files in os.walk(bronze_dir):
        for file in files:
            file_path = os.path.join(subdir, file)
            print(f"Carregando arquivo: {file_path}")
            
            # Comando para carregar o arquivo no target_db
            command = f'source .venv/bin/activate && meltano run tap-csv-bronze target-postgres'
            result = subprocess.run(command, cwd=root_dir, shell=True, executable='/bin/bash', capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"Arquivo {file_path} carregado com sucesso.")
            else:
                print(f"Erro ao carregar o arquivo {file_path}.")
                print(result.stderr)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Executar steps do Meltano')
    parser.add_argument('step', choices=['extract', 'load', 'push'], help='Step a ser executado')
    args = parser.parse_args()

    if args.step == 'extract':
        run_extraction_csv()
    elif args.step == 'load':
        run_extraction_postgres()
    elif args.step == 'push':
        load_to_target_db()