# Indicium Tech Code Challenge

Desafio técnico do programa Indicium Lighthouse.

## Requisitos

- Docker
- Postgres
- Airflow
- Meltano
- UV
- MKDocs

## Objetivo

O objetivo deste projeto é criar uma pipeline de dados capaz de consumir dados de um banco Postgres em um container Docker e de uma planilha no sistema de arquivos, obedecendo à seguinte estrutura de escrita:

```
/data/postgres/{table}/2024-01-01/file.format
/data/postgres/{table}/2024-01-02/file.format
/data/csv/2024-01-02/file.format
```

Em seguida, outra pipeline deve ser capaz de ler o output da primeira pipeline e carregar os dados em um banco Postgres destino. Por fim, o usuário deve ser capaz de rodar uma query que mostre os pedidos e seus detalhes.

## Setup

### 1. Instalar o Docker

Caso já possua em seu dispositivo, passe para o próximo passo.
Para subir os bancos de dados em seu dispositivo, você deve ter o Docker instalado. Para isso, consulte a página oficial: [Docker](https://www.docker.com/).

### 2. Clonar o projeto

```bash
git clone https://github.com/Rafael07/LH_ED_Rafael07.git
```

### 3. Rodar o Docker

Agora, vamos subir os containers com as imagens do Postgres para disponibilizar o banco origem e o banco destino:

```bash
docker compose up -d
```

### 4. Gerenciador de Ambientes Virtuais

Este projeto utiliza o **UV**, um gerenciador de pacotes e projetos para Python. O isolamento de dependências e ambientes é uma boa prática, permitindo que múltiplos projetos sejam desenvolvidos sem interferir no Python do sistema.

Instale o `pipx` para gerenciar o `UV`:

```bash
sudo apt update
sudo apt install pipx
pipx ensurepath
sudo pipx ensurepath --global  # Opcional para permitir ações globais do pipx
```

Agora, instale o **UV**:

```bash
pipx install uv
```

Este projeto foi desenvolvido sob a versão **3.11.7** do Python. Recomendamos instalá-la para evitar problemas de compatibilidade:

```bash
uv python install 3.11.7
```

### 5. Configuração do Meltano

Vamos configurar o Meltano para operar o ETL do projeto. Primeiro, criaremos um ambiente virtual para isolar as dependências:

```bash
cd meltano_dataloader
uv venv --python 3.11.7
source .venv/bin/activate
uv sync
```

Agora, instalamos os componentes definidos no arquivo `meltano.yml`:

```bash
meltano install
deactivate
```

### 6. Configuração do Airflow

Passos semelhantes serão seguidos para configurar o **Airflow**:

```bash
cd airflow
uv venv --python 3.11.7
source .venv/bin/activate
uv sync
```

Agora, configuramos variáveis de ambiente importantes para os scripts do projeto:

```bash
source ../src/set_env.sh
```

Para garantir que as DAGs de exemplo estejam desabilitadas, verifique se o retorno do seguinte comando é `False`:

```bash
airflow config get-value core load_examples
```

Por fim, rodamos o **Airflow**, acessamos a UI e verificamos a execução do projeto:

```bash
airflow standalone
```

Durante a inicialização, a senha será exibida no terminal. Copie-a e acesse o serviço em:

```
http://localhost:8080
```

Utilize o usuário `admin` e a senha gerada. Agora é só checar as DAGs e verificar os arquivos gerados nos diretórios do projeto. A query resultante deve estar na pasta `data/gold`.

## Desafios

O projeto foi, sem dúvidas, desafiador. O **Meltano** era uma ferramenta desconhecida e exigiu uma curva de aprendizado para sua configuração. A incompatibilidade entre dependências também foi um obstáculo, pois diferentes partes da stack utilizavam versões distintas do **SQLAlchemy**, gerando conflitos na instalação. A solução foi isolar as ferramentas em ambientes virtuais separados.

O projeto ainda pode ser aprimorado com mais automação, idealmente permitindo sua execução com um único comando. Uma pendência importante é a execução do processo para datas anteriores à atual, um aspecto essencial que será resolvido em futuras atualizações.

