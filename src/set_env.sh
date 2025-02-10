#!/bin/bash
# set_env.sh - Script para configurar variáveis de ambiente do projeto, incluindo as configurações do Airflow

# Define DATA_PATH
export DATA_PATH=$(dirname $(pwd))
echo "DATA_PATH configurado para: $DATA_PATH"

# Define AIRFLOW_HOME para que o Airflow use o diretório correto
export AIRFLOW_HOME="$DATA_PATH/airflow"
echo "AIRFLOW_HOME configurado para: $AIRFLOW_HOME"

# Define EXECUTION_DATE com a data atual no formato AAAA-MM-DD
export EXECUTION_DATE=$(date +%Y-%m-%d)
echo "EXECUTION_DATE configurado para: $EXECUTION_DATE"

# Reescreve o arquivo airflow.cfg para as configurações do projeto
airflow config list --defaults > "${AIRFLOW_HOME}/airflow.cfg"

# Desabilita as DAGs de exemplo do Airflow
export AIRFLOW__CORE__LOAD_EXAMPLES=false
