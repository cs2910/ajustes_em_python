from sqlalchemy.orm import sessionmaker
from sqlalchemy import extract, update,insert, text
from tabelatributos import Pensend
from conexao import engine, connection
import requests, json

#################################################
#           Conexão com banc de dados           #
#################################################
Session = sessionmaker(bind=engine)
session = Session()

#### Linka
urlPost = 'https://saude.betha.cloud/api-saude-service-layer/v2/api/atendimentoDomiciliarCboPermitidos/'
#### Token da Endidade
tokenEntidade = '38096b6b-d3b5-4895-8c0b-92d355c7dee3'

# Lista para armazenar todos os dados
all_cbo_data = []

# Parâmetros de paginação
offset = 0
limit = 25  # Limite de registros por requisição
has_next = True

# Loop para buscar todos os dados
while has_next:
    print(f"Buscando dados com offset: {offset}...")
    params = {'offset': offset, 'limit': limit}
    response = requests.get(
        urlPost,
        headers={'Authorization': f'Bearer {tokenEntidade}'},
        params=params
    )

    if response.status_code == 200:
        data = json.loads(response.content)
        content_list = data.get('content', [])
        all_cbo_data.extend(content_list)

        has_next = data.get('hasNext', False)
        if has_next:
            offset += limit
    else:
        print(f"Erro na requisição: {response.status_code}")
        break

    print("-" * 50)
    print(f"Total de registros encontrados: {len(all_cbo_data)}")
    print("-" * 50)

    # --- Listando o CBO ID e o ID Gerado de cada registro ---
    if all_cbo_data:
        for item in all_cbo_data:
            cbo_id = item['cbo']['id']
            id_gerado = item['idGerado']
            print(f"CBO ID: {cbo_id}, ID Gerado: {id_gerado}")
    else:
        print("Nenhum registro encontrado para listar.")

    print("-" * 50)
    print("Listagem concluída!")