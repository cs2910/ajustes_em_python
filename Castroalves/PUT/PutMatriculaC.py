from sqlalchemy.orm import sessionmaker
from sqlalchemy import update, insert
from tabelas import Hmatricula, Situacao
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

tokenEntidade = '8edfad4d-63f4-43fd-83ce-462962f024d8'

tabelas = session.query(Situacao).all()
##tabelas = session.query(Situacao).where(Situacao.matric =='37319153')
lista_dados = []
campo_alvo  = "situacao"  # Campo que será alterado
campo_alterar = "AFASTADO"
versao      = "version"  # Campo que deverá ser deletado da lista

for tabela in tabelas:
    f_token_retorno = tabela.matric
    print(f"func {tabela.func} e id{f_token_retorno}")



    servico = requests.get(f'https://pessoal.betha.cloud/service-layer/v1/api/historico-matricula/{f_token_retorno}',
                                   headers={'Authorization': 'Bearer 8edfad4d-63f4-43fd-83ce-462962f024d8'})
    lista_dados = json.loads(servico.content)
    print(lista_dados)
    # deleta os campos Version
    lista_dados.pop(versao)

    lista_dados[campo_alvo] = 'AFASTADO'
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
