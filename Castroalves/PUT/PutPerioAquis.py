from sqlalchemy.orm import sessionmaker
from sqlalchemy import update, insert
from tabelas import Hmatricula, Paquisicao
from conexao import engine,connection
from datetime import datetime
import requests, json


#################################################
#           Conexão com banc de dados           #
#################################################
Session = sessionmaker(bind=engine)
session = Session()

#### Linka
urlPost = 'https://pessoal.betha.cloud/service-layer/v1/api/periodo-aquisitivo-ferias/'
#### Token da Endidade

tokenEntidade = '8edfad4d-63f4-43fd-83ce-462962f024d8'

##tabelas = session.query(Aquisicao).all()
##tabelas = session.query(Peraquisicao).where(Peraquisicao.dataini == Peraquisicao.datafin)
##tabelas = session.query(Peraquisicao).where(Peraquisicao.id == 37285460)

tabelas = session.query(Paquisicao).where(Paquisicao.protocolo.is_(None))
lista_dados = []
versao      = "version"  ## Campo que deverá ser deletado da lista


##print(len(tabelas))

for tabela in tabelas:
    f_token_retorno = tabela.id
    print(f"func {tabela.idprincipal} e id{f_token_retorno}")

    servico = requests.get(f'https://pessoal.betha.cloud/service-layer/v1/api/periodo-aquisitivo-ferias/{f_token_retorno}',
                                   headers={'Authorization': 'Bearer 8edfad4d-63f4-43fd-83ce-462962f024d8'})
    lista_dados = json.loads(servico.content)
    print(tabela.novaini)
    print(lista_dados)
    # deleta os campos Version
    lista_dados.pop(versao)
    lista_dados['dataInicial'] = tabela.novaini.strftime('%Y-%m-%d')
    lista_dados['dataFinal'] = tabela.novafin.strftime('%Y-%m-%d')
    for movimentacao in lista_dados['movimentacoes']:
        movimentacao['dataMovimento'] = tabela.novaini.strftime('%Y-%m-%d %H:%M:%S')
        if movimentacao['tipo'] == 'CANCELAMENTO':
            movimentacao['dataCancelamento'] = tabela.novaini.strftime('%Y-%m-%d')
    print(lista_dados)

    #################################################
    #   Update da tabela com retorno do protocolo   #
    #################################################
    dados = (json.dumps(lista_dados, ensure_ascii=False))
    print(dados)

    ###################################################
    #   Montar o cabeçalho para rodar o metodo PUT    #
    ###################################################
    montar_json = '[{'f'"idIntegracao": "string {tabela.id}","idGerado": "lote {tabela.id}","conteudo":{dados}''}]'
    print(montar_json)

    response = requests.post(urlPost,
                             headers={
                                 'Authorization': f'Bearer 8edfad4d-63f4-43fd-83ce-462962f024d8',
                                 'content-type': 'application/json'},
                             json=json.loads(montar_json))

    status = response.status_code
    print (status)
    if status == 200:
        recurrence = json.loads(response.content)
        f_token_retorno = recurrence['id']
        print(f'protocolo {f_token_retorno}')
        stmt = (update(Paquisicao).where(Paquisicao.id == tabela.id).values(protocolo=str(f_token_retorno)))
        print(stmt)
        session.execute(stmt)
        session.commit()
    elif status == 500:
        print('Erro no envio - verifique os paramêtros')