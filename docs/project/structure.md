# Criar/editar docs/project/structure.md
cat << 'EOF' > docs/project/structure.md
# Estrutura de Dados

## Organização das Camadas

### Bronze
- Dados brutos das fontes originais
- Northwind SQL dump
- Order details CSV
- Sem transformações

### Silver
- Dados limpos e padronizados
- Organizados por data de processamento (YYYY-MM-DD)
- Validações básicas aplicadas

### Gold
- Dados prontos para consumo
- Organizados por data de processamento (YYYY-MM-DD)
- Transformações de negócio aplicadas

## Estrutura de Diretórios

data/
├── bronze/ # Dados brutos
├── silver/ # Dados padronizados
└── gold/ # Dados prontos para consumo


## Padrões de Nomenclatura
- Diretórios de data: YYYY-MM-DD
- Arquivos: {table_name}_{timestamp}.format

## Justificativa
Esta estrutura segue o padrão medalhão (Bronze, Silver, Gold) comum em arquiteturas modernas de dados, permitindo:
- Rastreabilidade clara dos dados
- Processamento idempotente
- Organização temporal
- Facilidade de backup e recuperação
