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

#### Linka7
urlPost = 'https://saude.betha.cloud/api-saude-service-layer/v2/api/atendimentoDomiciliarCondicaoAvaliadas/'
#### Token da Endidade
tokenEntidade = '38096b6b-d3b5-4895-8c0b-92d355c7dee3'

# Lista para armazenar todos os dados
all_cbo_data = []
processed_ids = set()
batch_size = 50
# Parâmetros de paginação


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
        data = []
        for item in all_cbo_data:
            idGerado = item['atendimentoDomiciliar']['id']
            idAvalicao = item['idGerado']
            print(f" idAtend {idGerado} idAvalicao {idAvalicao}")
            query = text(f"select a.id_gerado from cnv_atend_domiciliar ad "
                         f"inner join cnv_id_atendom a on a.sequenc = ad.sequenc  "
                         f"where  a.id_gerado = {idGerado}")
            resultados = session.execute(query).fetchall()
            for tabela in resultados:
                data.append({
                    "idIntegracao": f"{idAvalicao}",
                    "atendimentoDomiciliarCondicaoAvaliada": {
                        "idGerado": idAvalicao
                    }
                })
                print(len(data))
                if len(data) == batch_size:
                    try:
                        dados = json.dumps(data, ensure_ascii=False)  ## , indent=4 mostra o json em arvore
                        print("Conversão para JSON bem-sucedida!")
                        print(dados)
                    except TypeError as e:
                        print(f"Ocorreu um erro: {e}")
                    print(dados)
                    response = requests.delete(urlPost,
                                               headers={'Authorization': f'Bearer {tokenEntidade}',
                                                        'content-type': 'application/json'},
                                               data=json.dumps(data))
                    status = response.status_code
                    print(response.content)
                    print(status)
                    if status == 200:
                        ##        print("entrou")
                        recurrence = json.loads(response.content)
                        f_token_retorno = recurrence['idLote']
                        print(f'protocolo {f_token_retorno}')

                        lista_de_pares = [
                            (
                                item['idIntegracao'],  # Este será o seu 'idGerado'
                                f_token_retorno  # Este será o seu 'protocolo'
                            )
                            for item in data
                        ]

                        print(lista_de_pares)

                        # Formata cada par em uma string de valores SQL
                        valores_formatados = [
                            f"('{idgerado}', '{protocolo}')"
                            for idgerado, protocolo in lista_de_pares
                        ]

                        # Une todas as strings com vírgula para a cláusula VALUES
                        valores_in_sql = ", ".join(valores_formatados)

                        # Nome da sua tabela e colunas de destino
                        tabela_destino = "cnv_delcondavaliativas"
                        colunas = "(idgerado, protocolo)"

                        # Monta o comando INSERT completo
                        stmt = text(f"""
                            INSERT INTO {tabela_destino} {colunas}
                            VALUES {valores_in_sql}
                        """)
                        session.execute(stmt)
                        session.commit()



                    else:
                        print(f'erro execução')
                    ## Zerar lista
                    data = []
        print(len(data))
        if len(data) > 0:
            try:
                dados = json.dumps(data, ensure_ascii=False)  ## , indent=4 mostra o json em arvore
                print("Conversão para JSON bem-sucedida!")
                print(dados)
            except TypeError as e:
                print(f"Ocorreu um erro: {e}")
            print(dados)
            response = requests.delete(urlPost,
                                       headers={'Authorization': f'Bearer {tokenEntidade}',
                                                'content-type': 'application/json'},
                                       data=json.dumps(data))
            status = response.status_code
            print(response.content)
            print(status)
            if status == 200:
                ##        print("entrou")
                recurrence = json.loads(response.content)
                f_token_retorno = recurrence['idLote']
                print(f'protocolo {f_token_retorno}')

                lista_de_pares = [
                    (
                        item['idIntegracao'],  # Este será o seu 'idGerado'
                        f_token_retorno  # Este será o seu 'protocolo'
                    )
                    for item in data
                ]

                print(lista_de_pares)

                # Formata cada par em uma string de valores SQL
                valores_formatados = [
                    f"('{idgerado}', '{protocolo}')"
                    for idgerado, protocolo in lista_de_pares
                ]

                # Une todas as strings com vírgula para a cláusula VALUES
                valores_in_sql = ", ".join(valores_formatados)

                # Nome da sua tabela e colunas de destino
                tabela_destino = "cnv_delcondavaliativas"
                colunas = "(idgerado, protocolo)"

                # Monta o comando INSERT completo
                stmt = text(f"""
                    INSERT INTO {tabela_destino} {colunas}
                    VALUES {valores_in_sql}
                """)
                session.execute(stmt)
                session.commit()



            else:
                print(f'erro execução')
            ## Zerar lista
            data = []
        print("-" * 50)
        print("Listagem concluída!")