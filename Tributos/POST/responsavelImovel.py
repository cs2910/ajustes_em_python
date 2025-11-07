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
urlPost = 'https://tributos.betha.cloud/service-layer-tributos/api/imoveisResponsaveis'
#### Token da Endidade

tokenEntidade = '8b19d51d-684f-4b7f-889a-6e8fcd7ce3fe'

'''Utilizando select direto'''

query = text("select ")

resultados = session.execute(query).fetchall()
batch_size = 1
data = []
for tabela in resultados:
    print(f'{tabela.sequenc} -  {tabela.id_procedimento} - Id {tabela.idatendimentos}')
    data.append({
        "idIntegracao": "29164325",
        "imoveisResponsaveis": {
            "idPessoa": 82532203,
            "idImovel": 29164325,
            "percentual": 0,
            "inicioTitularidade": "2018-11-30",
            "fimTitularidade": "2024-08-14"
        }
    })
    print(len(data))
    if len(data) == batch_size:
        print(data)
    ##    dados = json.dumps(data, ensure_ascii=False)
        try:
            dados = json.dumps(data, ensure_ascii=False) ## , indent=4 mostra o json em arvore
            print("Conversão para JSON bem-sucedida!")
            print(dados)
        except TypeError as e:
            print(f"Ocorreu um erro: {e}")
        print(dados)

        response = requests.post(urlPost,
                                 headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                                 data=json.dumps(data))
        status = response.status_code
        ##status = 200
        print(response.content)
        print(status)
        if status == 200:
    ##        print("entrou")
            recurrence = json.loads(response.content)
            f_token_retorno = recurrence['idLote']
            print(f'protocolo {f_token_retorno}')
            lista_de_pares = [
                (
                    item['idIntegracao'],
                    item['atendimentoProcedimentoRealizado']['procedimento']['id']
                )
                for item in data
            ]

            # 2. Constrói e executa um ÚNICO comando UPDATE em lote
            # Isso atualiza todos os registros de uma vez, melhorando a velocidade drasticamente
            valores_formatados = [
                f"('{sequenc}', {id_procedimento})"
                for sequenc, id_procedimento in lista_de_pares
            ]
            valores_in_sql = ", ".join(valores_formatados)

            stmt = text(f"""
                    UPDATE cnv_atendprontuario 
                    SET protocoloprocante = '{f_token_retorno}' 
                    WHERE (sequenc, id_procedimento) IN ({valores_in_sql})
                """)

            session.execute(stmt)
            session.commit()
        else:
            print(f'erro execução')
        ## Zerar lista
        data = []
if data:
    print(data)
    dados = json.dumps(data)
    print(dados)
    response = requests.post(urlPost,
                             headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
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
                item['idIntegracao'],
                item['atendimentoProcedimentoRealizado']['procedimento']['id']
            )
            for item in data
        ]

        # 2. Constrói e executa um ÚNICO comando UPDATE em lote
        # Isso atualiza todos os registros de uma vez, melhorando a velocidade drasticamente
        valores_formatados = [
            f"('{sequenc}', {id_procedimento})"
            for sequenc, id_procedimento in lista_de_pares
        ]
        valores_in_sql = ", ".join(valores_formatados)

        stmt = text(f"""
                UPDATE cnv_atendprontuario 
                SET protocoloprocante = '{f_token_retorno}' 
                WHERE (sequenc, id_procedimento) IN ({valores_in_sql})
            """)

        session.execute(stmt)
        session.commit()
    else:
        print(f'erro execução')