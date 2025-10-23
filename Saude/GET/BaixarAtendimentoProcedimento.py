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
urlPost = 'https://saude.betha.cloud/api-saude-service-layer/v2/api/atendimentoProcedimentoRealizados/'
#### Token da Endidade
tokenEntidade = 'fe8704a7-f0da-41f9-91d1-acf75a1d5a0f'

# Lista para armazenar todos os dados
all_cbo_data = []

processed_ids = set()
# Parâmetros de paginação
offset = 134746
limit = 1000  # Limite de registros por requisição
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
        ##print(content_list)
        has_next = data.get('hasNext', False)
        if has_next:
            offset += limit
    else:
        print(f"Erro na requisição: {response.status_code}")
        break

    print("-" * 50)
    print(f"Total de registros encontrados: {len(all_cbo_data)}")
    print("-" * 50)

    if all_cbo_data:
        for item in all_cbo_data:
            idGerado = item['idGerado']

            # Verifica se o ID já foi processado
            if idGerado in processed_ids:
                print(f"Registro com idGerado {idGerado} já existe. Pulando.")
                continue  # Pula para o próximo item do loop

            # Se o ID não foi processado, adiciona ao conjunto
            processed_ids.add(idGerado)
            if item['atendimento'] is not None:
                atendimento = item['atendimento']["id"]

            stmt = text(
                f"insert into cnv_atendprocedimentoscloud values ({atendimento},{idGerado})")
            ##print(stmt)
            session.execute(stmt)
            session.commit()
    else:
        print("Nenhum registro encontrado para listar.")

    print("-" * 50)
    print("Listagem concluída!")