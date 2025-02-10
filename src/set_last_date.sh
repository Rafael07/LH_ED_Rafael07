#!/bin/bash

# Procura as pastas dentro de $DATA_PATH/data/bronze/csv, ordena e seleciona a última (mais recente)
export LAST_DATE=$(ls -1 "$DATA_PATH/data/bronze/csv" | sort | tail -n 1)
echo "LAST_DATE configurado para: $LAST_DATE"
