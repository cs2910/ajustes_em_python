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

# Parâmetros de paginação
offset = 0
limit = 500  # Limite de registros por requisição
has_next = True
processed_ids = set()

# Loop para buscar os dados em blocos
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

        # Lista para armazenar os registros do bloco atual
        registros_para_inserir = []

        for item in content_list:
            idGerado = item.get('idGerado')

            # Evita duplicatas, mesmo entre blocos
            if idGerado in processed_ids:
                print(f"Registro com idGerado {idGerado} já existe. Pulando.")
                continue

            processed_ids.add(idGerado)

            if item.get('atendimento') and item['atendimento'].get('id'):
                atendimento_id = item['atendimento']['id']

                # Adiciona o dicionário com os valores a serem inseridos
                registros_para_inserir.append({
                    'atendimento': atendimento_id,
                    'idgerado': idGerado
                })

        # --- LÓGICA DE INSERÇÃO EM LOTE ---
        if registros_para_inserir:
            session = Session()
            try:
                # Cria a instrução de INSERT. A palavra-chave `VALUES` é opcional
                # e o SQLAlchemy irá formatar a instrução corretamente.
                stmt = text(
                    "INSERT INTO cnv_atendprocedimentoscloud (idatend, id_gerado) VALUES (:atendimento, :idgerado)")

                # Usa `session.execute()` com a lista de dicionários
                session.execute(stmt, registros_para_inserir)
                session.commit()
                print(f"Bloco de {len(registros_para_inserir)} registros inserido com sucesso!")
            except Exception as e:
                session.rollback()
                print(f"Erro durante a inserção em lote: {e}")
            finally:
                session.close()
        else:
            print("Nenhum registro válido encontrado neste bloco.")

        has_next = data.get('hasNext', False)
        if has_next:
            offset += limit

    else:
        print(f"Erro na requisição: {response.status_code}")
        break

    print("-" * 50)
    print("Processo de um bloco concluído!")
    print("-" * 50)

print("Processo de todos os blocos concluído!")