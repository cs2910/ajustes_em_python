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
urlPost = 'https://tributos.betha.cloud/service-layer-tributos/api/manutCalcCreditosRec'
#### Token da Endidade

tokenEntidade = '8b19d51d-684f-4b7f-889a-6e8fcd7ce3fe'

'''Utilizando select direto'''

query = text("select n.id from bthsc245892novo n "
             "join bthsc245892manut m on "
             "m.id = n.id_manutencoes_calculos "
             "where situacao	in ('D','A') and id_creditos_tributarios_rec = 542055 and "
             "protocolodel is null")
resultados = session.execute(query).fetchall()
batch_size = 50
data = []
for tabela in resultados:
    print(f'{tabela.id} ')
    data.append({
        "idIntegracao": f"{tabela.id}",
        "manutCalcCreditosRec": {
            "idGerado": {
                "id":tabela.id
            }
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

        response = requests.delete(urlPost,
                                 headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                                 data=json.dumps(data))
        status = response.status_code
        ##status = 200
        print(response.content)
        print(status)
        status = 200
        if status == 200:
    ##        print("entrou")
            recurrence = json.loads(response.content)
            f_token_retorno = recurrence['idLote']
            print(f'protocolo {f_token_retorno}')
            lista_sequenc_formatada = ", ".join([f"'{item['idIntegracao']}'" for item in data])
            stmt = text(f"UPDATE bthsc245892novo SET protocolodel = '{f_token_retorno}' WHERE id IN ({lista_sequenc_formatada}) and id_creditos_tributarios_rec = 542055")
            print(stmt)
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
    response = requests.delete(urlPost,
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
        lista_sequenc_formatada = ", ".join([f"'{item['idIntegracao']}'" for item in data])
        stmt = text(
            f"UPDATE bthsc245892novo SET protocolodel = '{f_token_retorno}' WHERE id IN ({lista_sequenc_formatada}) and id_creditos_tributarios_rec = 542055")
        print(stmt)
        session.execute(stmt)
        session.commit()

    else:
        print(f'erro execução')