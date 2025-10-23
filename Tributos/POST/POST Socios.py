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
urlPost = 'https://tributos.betha.cloud/service-layer-tributos/api/pessoasJuridicasSocios'
#### Token da Endidade

tokenEntidade = '44d8ce63-03f0-4737-94c4-9d26bb98e2b5'
##tokenEntidade = ''


'''Utilizando select direto'''

query = text("select * from bthsc213544 where protocolo is null")
resultados = session.execute(query).fetchall()
batch_size = 50
data = []
for tabela in resultados:
    print(f'{tabela.codigojur} - {tabela.idsocio} - {tabela.codigo}')
    servPais = "S"
    data.append({
        "idIntegracao": f'{tabela.idsocio}',
        "pessoasJuridicasSocios": {
            "dtInclusao": f'{tabela.data}',
            "percSociedade": tabela.percentual,
            "idPessoa": f'{tabela.idsocio}',
            "idResponsavelLegal": f'{tabela.idempresa}',
            "idSocio": f'{tabela.idempresa}'
        }
    })


    print(len(data))
    if len(data) == batch_size:
        print(data)
        dados = json.dumps(data, ensure_ascii=False)
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

            for item in data:
                stmt = text(f"UPDATE bthsc213544 SET protocolo = '{f_token_retorno}' WHERE  idempresa = '{item['pessoasJuridicasSocios']['idSocio']}' and idsocio = '{item['pessoasJuridicasSocios']['idPessoa']}'")
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
        for item in data:
            stmt = text(f"UPDATE bthsc213544 SET protocolo = '{f_token_retorno}' WHERE  idempresa = '{item['pessoasJuridicasSocios']['idSocio']}' and idsocio = '{item['pessoasJuridicasSocios']['idPessoa']}'")
            session.execute(stmt)
        session.commit()
    else:
        print(f'erro execução')