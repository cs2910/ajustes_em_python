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
urlPost = 'https://saude.betha.cloud/api-saude-service-layer/v2/api/atendimentoDomiciliarProcedimentos/'
#### Token da Endidade
tokenEntidade = '38096b6b-d3b5-4895-8c0b-92d355c7dee3'

# Lista para armazenar todos os dados
all_cbo_data = []
batch_size = 3
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
        if content_list:
            # Lista para acumular os valores a serem inseridos
            valores_para_inserir = []

            for item in content_list:
                idGerado = item['idGerado']
                idAtendimento = item['atendimentoDomiciliar']['id']

                # Verifica se o ID já foi processado
                if idGerado not in processed_ids:
                    # Formata o par de valores e adiciona à lista
                    # As aspas simples são essenciais para valores de texto
                    valores_para_inserir.append(f"('{idAtendimento}', '{idGerado}')")
                    # Adiciona ao conjunto para evitar duplicatas
                    processed_ids.add(idGerado)
                else:
                    print(f"Registro com idGerado {idGerado} já existe. Pulando.")

            # Se a lista de valores não estiver vazia, execute a inserção em massa
            if valores_para_inserir:
                print(f"Inserindo lote de {len(valores_para_inserir)} registros...")

                # Junta todos os pares de valores em uma única string
                valores_em_sql = ", ".join(valores_para_inserir)

                # Constrói o comando INSERT em lote
                stmt = text(f"""
                            INSERT INTO cnv_delatenddomicprocedimentos (idgerado, idprocedimento)
                            VALUES {valores_em_sql}
                        """)

                # Executa a instrução e faz o commit
                session.execute(stmt)
                session.commit()
                print("Lote inserido com sucesso!")

        has_next = data.get('hasNext', False)
        if has_next:
            offset += limit
    else:
        print(f"Erro na requisição: {response.status_code}")
        break

    print("-" * 50)
    print("Listagem e inserção do lote atual concluídas.")
    print("-" * 50)

# Fecha a sessão no final
session.close()

print("Processo de importação finalizado.")