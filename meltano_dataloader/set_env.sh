#!/bin/bash
# set_env.sh - Script para configurar variáveis de ambiente do projeto

# Define a variável DATA_PATH
export DATA_PATH="/home/rafael/projects/testes/LH_ED_Rafael07"
export EXECUTION_DATE=$(date +%Y-%m-%d)
export LAST_DATE=$(ls -1 "$DATA_PATH/data/bronze/csv" | sort | tail -n 1)

echo "Variáveis de ambiente configuradas:"
echo "DATA_PATH = $DATA_PATH"
echo "EXECUTION_DATE = $EXECUTION_DATE"
echo "LAST_DATE = $LAST_DATE"
