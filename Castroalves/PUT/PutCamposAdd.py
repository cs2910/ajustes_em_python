from sqlalchemy.orm import sessionmaker
from sqlalchemy import update, insert
from tabelas import Campoadd
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

tokenEntidade = '5f8a32ac-701e-4282-a342-c16781f99713'

##tabelas = session.query(Campoadd).where(Campoadd.matric == '5', Campoadd.protocolo.is_(None))
tabelas = session.query(Campoadd).where(Campoadd.protocolo.is_(None))
##tabelas = session.query(Campoadd).where(Campoadd.protocolo.is_(None))
lista_dados = []
versao      = "version"  ## Campo que deverá ser deletado da lista


##print(len(tabelas))

for tabela in tabelas:
    f_token_retorno = tabela.id
    print(f"func {tabela.matric} e id{f_token_retorno}")

    servico = requests.get(f'https://pessoal.betha.cloud/service-layer/v1/api/historico-matricula/{f_token_retorno}',
                                   headers={'Authorization': 'Bearer 5f8a32ac-701e-4282-a342-c16781f99713'})
    lista_dados = json.loads(servico.content)
    #print(lista_dados)
    # deleta os campos Version
    lista_dados.pop(versao)
    for movimentacao in lista_dados['camposAdicionais']:
        if movimentacao['identificador'] == 'siope_ba':
            movimentacao['campos'][0]['valor'] = '70'
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
    ##print(montar_json)

    response = requests.post(urlPost,
                             headers={
                                 'Authorization': f'Bearer 5f8a32ac-701e-4282-a342-c16781f99713',
                                 'content-type': 'application/json'},
                             json=json.loads(montar_json))

    status = response.status_code
    print (status)
    if status == 200:
        recurrence = json.loads(response.content)
        f_token_retorno = recurrence['id']
        print(f'protocolo {f_token_retorno}')
        stmt = (update(Campoadd).where(Campoadd.id == tabela.id).values(protocolo=str(f_token_retorno)))
        ##print(stmt)
        session.execute(stmt)
        session.commit()
    elif status == 500:
        print('Erro no envio - verifique os paramêtros')