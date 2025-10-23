from sqlalchemy.orm import sessionmaker
from sqlalchemy import update
from flask_restful import Resource
from tabelas import Cargo, Tcargo, Conta
from conexao import engine,connection
from datetime import datetime
import requests, json

#################################################
#           Conexão com banc de dados           #
#################################################
Session = sessionmaker(bind=engine)
session = Session()

#### Link
urlPost = 'https://pessoal.betha.cloud/service-layer/v1/api/estado/lotes/'
#### Token da Endidade
tokenEntidade = '8edfad4d-63f4-43fd-83ce-462962f024d8'

#################################################
#           Selecionar dados na tabela          #
#################################################
tabelas = session.query(Conta).all()
total_dados = len(tabelas)
for tabela in tabelas:
    f_token_retorno = tabela.protocolo
    f_situacao = ""
##    print(f_token_retorno)
    servico = requests.get(f'https://pessoal.betha.cloud/service-layer/v1/api/estado/lotes/{f_token_retorno}',
                               headers={'Authorization': f'Bearer {tokenEntidade}'})
    status = servico.status_code
##    print (status)
    if status == 200:
        dadosGet = json.loads(servico.content)
        dados = servico.json()
        if dadosGet["retorno"][0]["mensagem"]:
            print(f"Status {status} protocolo {f_token_retorno} mensagem {dadosGet["retorno"][0]["mensagem"]}")
    elif (status == 500 or status == 401):
        print('Erro na execução da consulta')



