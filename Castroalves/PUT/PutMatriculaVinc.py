from sqlalchemy.orm import sessionmaker
from sqlalchemy import update, insert
from tabelas import Hmatricula, Matrievento
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

tokenEntidade = '3505909d-dab3-45f6-8917-ca171c713694'

tabelas = session.query(Matrievento).all()
##tabelas = session.query(Matrievento).where(Matrievento.id == '39259991')
##tabelas = session.query(Matrievento).where(Matrievento.nome == 'AMARILDO SANTOS LIMA')


lista_dados = []
versao      = "version"  # Campo que deverá ser deletado da lista

for tabela in tabelas:
    f_token_retorno = tabela.id
    print(f"func {tabela.nome} e id{f_token_retorno}")



    servico = requests.get(f'https://pessoal.betha.cloud/service-layer/v1/api/historico-matricula/{f_token_retorno}',
                                   headers={'Authorization': 'Bearer 3505909d-dab3-45f6-8917-ca171c713694'})
    lista_dados = json.loads(servico.content)
    print(lista_dados)
    # deleta os campos Version
    lista_dados.pop(versao)
    lista_dados["vinculoEmpregaticio"]["id"] = 31538

    #################################################
    #   Update da tabela com retorno do protocolo   #
    #################################################
    dados = (json.dumps(lista_dados))
    print(dados)

    ###################################################
    #   Montar o cabeçalho para rodar o metodo PUT    #
    ###################################################
    montar_json = '[{'f'"idIntegracao": "string {tabela.id}","idGerado": "lote {tabela.id}","conteudo":{dados}''}]'
    print(montar_json)

    response = requests.put(urlPost,
                            headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                            data=montar_json)
    status = response.status_code
    print (status)
    if status == 200:
        recurrence = json.loads(response.content)
        f_token_retorno = recurrence['id']
        print(f'protocolo {f_token_retorno}')
    elif status == 500:
        print('Erro no envio - verifique os paramêtros')
