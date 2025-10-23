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
urlPost = 'https://saude.betha.cloud/api-saude-service-layer/v2/api/atendimentoDomiciliarProfissionais/'
#### Token da Endidade
tokenEntidade = '38096b6b-d3b5-4895-8c0b-92d355c7dee3'

# Lista para armazenar todos os dados
all_cbo_data = []

processed_ids = set()
# Parâmetros de paginação
offset = 0
limit = 500  # Limite de registros por requisição
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
        print(content_list)
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

            atendimentoDomiciliar = item['atendimentoDomiciliar']['id']
            unidade = item['unidade']['id']
            profissional = item['profissional']['id']
            especialidade = item['especialidade']['id']
            cbo = item['cbo']['id']
            if item['equipe'] is None:
                idequipe = 0
            else:
                idequipe = item['equipe']['id']
            equipe = idequipe
            principal = item['principal']
            excluido = item['excluido']
            stmt = text(
                f"insert into cnv_atenddomicprofissionalcloud values ({atendimentoDomiciliar},{unidade},{profissional},{especialidade},{cbo},{equipe},{principal},{excluido},{idGerado})")
            session.execute(stmt)
            session.commit()
    else:
        print("Nenhum registro encontrado para listar.")

    print("-" * 50)
    print("Listagem concluída!")