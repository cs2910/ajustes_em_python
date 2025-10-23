from sqlalchemy.orm import sessionmaker
from sqlalchemy import update, insert
from tabelas import Bateponto
from conexao import engine,connection
from datetime import datetime
import requests, json


#################################################
#           Conexão com banc de dados           #
#################################################
Session = sessionmaker(bind=engine)
session = Session()

#### Linka
urlPost = 'https://pessoal.betha.cloud/service-layer/v1/api/historico-matricula/'
#### Token da Endidade

tokenEntidade = '82942137-3b63-4fc5-bb80-8b789dbefd74'

tabelas = session.query(Bateponto).where(Bateponto.matric >= '1')
##tabelas = session.query(Campoadd).where(Campoadd.protocolo.is_(None))
##tabelas = session.query(Campoadd).where(Campoadd.protocolo.is_(None))
lista_dados = []
versao      = "version"  ## Campo que deverá ser deletado da lista


##print(len(tabelas))

for tabela in tabelas:
    f_token_retorno = tabela.id
    print(f"func {tabela.matric} e id{f_token_retorno}")

    servico = requests.get(f'https://pessoal.betha.cloud/service-layer/v1/api/historico-matricula/{f_token_retorno}',
                                   headers={'Authorization': 'Bearer 82942137-3b63-4fc5-bb80-8b789dbefd74'})
    lista_dados = json.loads(servico.content)
    print(lista_dados)
    # deleta os campos Version
    lista_dados.pop(versao)
    print(lista_dados['registraPonto'])
    if lista_dados['registraPonto']:
        lista_dados['registraPonto'] = False
        print("Aqui")
    else:
        continue
    #print(lista_dados)

    #################################################
    #   Update da tabela com retorno do protocolo   #
    #################################################
    dados = (json.dumps(lista_dados, ensure_ascii=False))
    #print(dados)

    ###################################################
    #   Montar o cabeçalho para rodar o metodo PUT    #
    ###################################################
    montar_json = '[{'f'"idIntegracao": "string {tabela.id}","idGerado": "lote {tabela.id}","conteudo":{dados}''}]'
    print(montar_json)

    response = requests.post(urlPost,
                             headers={
                                 'Authorization': f'Bearer 82942137-3b63-4fc5-bb80-8b789dbefd74',
                                 'content-type': 'application/json'},
                             json=json.loads(montar_json))

    status = response.status_code
    print (status)
    if status == 200:
        recurrence = json.loads(response.content)
        f_token_retorno = recurrence['id']
        print(f'protocolo {f_token_retorno}')
    elif status == 500:
        print('Erro no envio - verifique os paramêtros')