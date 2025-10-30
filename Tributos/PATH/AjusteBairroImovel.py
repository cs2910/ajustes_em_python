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
urlPost = 'https://tributos.betha.cloud/service-layer-tributos/api/imoveis'
#### Token da Endidade
tokenEntidade = '8b19d51d-684f-4b7f-889a-6e8fcd7ce3fe'

'''Utilizando select direto'''

##query = text("select * from ajustabairroimovel where protocolo is null")
query = text("select * from imovelbairromais where protocolo is null")

##query = text("SELECT * FROM ajustedtisencao where protocolo is null")
resultados = session.execute(query).fetchall()
batch_size = 50
data = []
for tabela in resultados:
    print(f'Imóvel  {tabela.codigo} - id Imovel - {tabela.idimovel} id Bairro  {tabela.idbairro} NomeBairro {tabela.bairroNovo}')
    data.append({
        "idIntegracao": f'{tabela.codigo}',
        "imoveis": {
            "idGerado": {
                "id": tabela.idimovel
            },
            "idBairro": tabela.idbairro
        }
    })

    ##print(len(data))
    if len(data) == batch_size:
        print(data)
    ##    dados = json.dumps(data, ensure_ascii=False)
        try:
            dados = json.dumps(data, ensure_ascii=False) ## , indent=4 mostra o json em arvore
            print("Conversão para JSON bem-sucedida!")
            print(dados)
        except TypeError as e:
            print(f"Ocorreu um erro: {e}")

        response = requests.patch(urlPost,
                                 headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                                 data=json.dumps(data))


        status = response.status_code
        print(response.content)
        print(status)
        if status == 200:
            recurrence = json.loads(response.content)
            f_token_retorno = recurrence['idLote']
            print(f'protocolo {f_token_retorno}')
            lista_sequenc_formatada = ", ".join([f"'{item['idIntegracao']}'" for item in data])
            stmt = text(f"UPDATE imovelbairromais SET protocolo = '{f_token_retorno}' WHERE codigo IN ({lista_sequenc_formatada})")
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
    response = requests.patch(urlPost,
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
        stmt = text(f"UPDATE imovelbairromais SET protocolo = '{f_token_retorno}' WHERE codigo IN ({lista_sequenc_formatada})")
        session.execute(stmt)
        session.commit()
    else:
        print(f'erro execução')