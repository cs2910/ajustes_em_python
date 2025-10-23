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
urlPost = 'https://tributos.betha.cloud/service-layer-tributos/api/imoveis/'
#### Token da Endidade

tokenEntidade = 'cd207fc9-18dc-404f-aff7-66801e120654'
##tokenEntidade = ''


'''Utilizando select direto'''

query = text("select * from imovel where novocep  is null and protocolo is null")

##query = text("SELECT * FROM ajustedtisencao where protocolo is null")
resultados = session.execute(query).fetchall()

for tabela in resultados:
    f_token_retorno = tabela.id
    print(tabela.codigo)
    print(f'{tabela.ceps} - {tabela.id}')
    servico = requests.get(f'https://tributos.betha.cloud/service-layer-tributos/api/imoveis/{f_token_retorno}',
                               headers={'Authorization': f'Bearer {tokenEntidade}'})

    lista_dados = json.loads(servico.content)
    print(lista_dados)

##    print(data)
    dados = json.dumps(data)
    print(dados)
    response = requests.patch(urlPost,
                             headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                             data=dados)
    status = response.status_code
    print(response.content)
    print(status)
    if status == 200:
##        print("entrou")
        recurrence = json.loads(response.content)
        f_token_retorno = recurrence['idLote']
        print(f'protocolo {f_token_retorno}')
        stmt = text(f"UPDATE imovel SET protocolo = '{f_token_retorno}' WHERE id = '{tabela.id}'")
##        print(stmt)
        result = session.execute(stmt)
        session.commit()
    else:
        print(f'erro execução')

