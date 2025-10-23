from sqlalchemy.orm import sessionmaker
from sqlalchemy import extract, update
from tabelatributos import Benparcial
from conexao import engine, connection
import requests, json

#################################################
#           Conexão com banc de dados           #
#################################################
Session = sessionmaker(bind=engine)
session = Session()

#### Linka
urlPost = 'https://tributos.betha.cloud/service-layer-tributos/api/manutencoesCalculos/'
#### Token da Endidade
tokenEntidade = 'ec6f1132-4a10-49b2-9457-e88c3e7a6aad'
##tokenEntidade = ''

#################################################
#           Selecionar dados na tabela          #
#################################################
tabelas = session.query(Benparcial).where(Benparcial.id > 4,
                                          Benparcial.protocolo.is_not(None),
                                          Benparcial.idcloud.is_(None))
for tabela in tabelas:
    idTabela = tabela.id
    f_token_retorno = tabela.protocolo
    print(f_token_retorno)
    f_situacao = ""
    print(f_token_retorno)
    servico = requests.get(f'https://tributos.betha.cloud/service-layer-tributos/api/manutencoesCalculos/{f_token_retorno}',
                               headers={'Authorization': f'Bearer {tokenEntidade}'})
    status = servico.status_code
    print (status)
    if status == 200:
        dadosGet = json.loads(servico.content)
        dados = servico.json()
        id_value = dados['retorno'][0]['idGerado']['id']
        print(id_value)
        upd = (update(Benparcial).where(Benparcial.id == idTabela).values(idcloud = id_value))
        print(upd)
        session.execute(upd)
        session.commit()
    elif (status == 500 or status == 401):
        print('Erro na execução da consulta')



