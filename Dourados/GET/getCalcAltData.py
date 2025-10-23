from sqlalchemy.orm import sessionmaker
from sqlalchemy import extract, update, text
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
tokenEntidade = 'c668172c-0f6e-4917-900d-337e636a2821'
##tokenEntidade = ''

#################################################
#           Selecionar dados na tabela          #
#################################################
query = text("SELECT * FROM ajustedtisencao where protocolo is not null")
tabelas = session.execute(query).fetchall()
for tabela in tabelas:
    idTabela = tabela.idmanut
    f_token_retorno = tabela.protocolo
    print(f_token_retorno)
    f_situacao = ""
    print(f_token_retorno)
    servico = requests.get(f'https://tributos.betha.cloud/service-layer-tributos/api/manutencoesCalculos/{f_token_retorno}',
                               headers={'Authorization': f'Bearer {tokenEntidade}'}
    )
    status = servico.status_code
    print (status)
    if status == 200:
        dadosGet = json.loads(servico.content)
        dados = servico.json()
        print(dados)
        for item in dados['retorno']:
            idTabela = item['idIntegracao']
            id_value = int(item['idGerado']['id'])
            stmt = text(f"UPDATE ajustedtisencao SET idcloud = '{id_value}' WHERE idmanut = {tabela.idmanut}")
            print(stmt)
            result = session.execute(stmt)
            session.commit()
    elif (status == 500 or status == 401):
        print('Erro na execução da consulta')



