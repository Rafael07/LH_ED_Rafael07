#!/bin/bash
# set_all_env.sh - Script para configurar variáveis de ambiente do projeto, incluindo as configurações do Airflow

# Define DATA_PATH
DATA_PATH=$(pwd)
echo "DATA_PATH configurado para: $DATA_PATH"

# Define AIRFLOW_HOME para que o Airflow use o diretório correto
export AIRFLOW_HOME="$DATA_PATH/airflow"
echo "AIRFLOW_HOME configurado para: $AIRFLOW_HOME"

# Define EXECUTION_DATE com a data atual no formato AAAA-MM-DD
export EXECUTION_DATE=$(date +%Y-%m-%d)
echo "EXECUTION_DATE configurado para: $EXECUTION_DATE"

# Procura as pastas dentro de $DATA_PATH/data/bronze/csv, ordena e seleciona a última (mais recente)
export LAST_DATE=$(ls -1 "$DATA_PATH/data/bronze/csv" | sort | tail -n 1)
echo "LAST_DATE configurado para: $LAST_DATE"
