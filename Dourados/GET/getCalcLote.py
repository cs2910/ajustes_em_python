from sqlalchemy.orm import sessionmaker
from sqlalchemy import extract, update
from tabelatributos import Benparcial, Protbeneficio
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
tokenEntidade = '94c4ea28-76c5-4e34-94ba-165c1ce7a632'
##tokenEntidade = ''

#################################################
#           Selecionar dados na tabela          #
#################################################
tabelas = session.query(Protbeneficio).where(Protbeneficio.id > 1)
for tabela in tabelas:
    idTabela = tabela.idmanut
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
        print(dados)
        for item in dados['retorno']:
            idTabela = item['idIntegracao']
            id_value = int(item['idGerado']['id'])
            upd = (update(Benparcial).where(Benparcial.id == idTabela).values(idcloud = id_value))
            print(upd)
            session.execute(upd)
            session.commit()
    elif (status == 500 or status == 401):
        print('Erro na execução da consulta')



