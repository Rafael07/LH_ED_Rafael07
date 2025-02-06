#!/bin/bash
# set_all_env.sh - Script para configurar variáveis de ambiente do projeto, incluindo as configurações do Airflow

# Define AIRFLOW_HOME para que o Airflow use o diretório correto
export AIRFLOW_HOME="/home/rafael/projects/testes/LH_ED_Rafael07/airflow"
echo "AIRFLOW_HOME configurado para: $AIRFLOW_HOME"

# Define DATA_PATH
export DATA_PATH="/home/rafael/projects/testes/LH_ED_Rafael07"
echo "DATA_PATH configurado para: $DATA_PATH"

# Define EXECUTION_DATE com a data atual no formato AAAA-MM-DD
export EXECUTION_DATE=$(date +%Y-%m-%d)
echo "EXECUTION_DATE configurado para: $EXECUTION_DATE"

# Verifica se DATA_PATH está definido e se o diretório existe
if [ -z "$DATA_PATH" ]; then
  echo "Variável DATA_PATH não está definida. Configure-a antes de prosseguir."
  exit 1
fi

if [ ! -d "$DATA_PATH/data/bronze/csv" ]; then
  echo "O diretório $DATA_PATH/data/bronze/csv não existe."
  exit 1
fi

# Procura as pastas dentro de $DATA_PATH/data/bronze/csv, ordena e seleciona a última (mais recente)
export LAST_DATE=$(ls -1 "$DATA_PATH/data/bronze/csv" | sort | tail -n 1)
echo "LAST_DATE configurado para: $LAST_DATE"
