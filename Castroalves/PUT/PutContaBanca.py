from sqlalchemy.orm import sessionmaker
from sqlalchemy import update, insert
from tabelas import Hmatricula, Conta
from conexao import engine,connection
from datetime import datetime
import requests, json


#################################################
#           Conexão com banc de dados           #
#################################################
Session = sessionmaker(bind=engine)
session = Session()

#### Linka
urlPost = 'https://pessoal.betha.cloud/service-layer/v1/api/historico-pessoa/'
#### Token da Endidade

tokenEntidade = '8edfad4d-63f4-43fd-83ce-462962f024d8'

tabelas = session.query(Conta).all()
##tabelas = session.query(Conta).where(Conta.idpessoa =='18275863')
lista_dados = []
campo_alvo  = "situacao"  # Campo que será alterado
campo_alterar = "ELETRONICA"
versao      = "version"  # Campo que deverá ser deletado da lista

for tabela in tabelas:
    f_token_retorno = tabela.idpessoa
    print(f"conta {tabela.idconta} e id{f_token_retorno}")
    servico = requests.get(f'https://pessoal.betha.cloud/service-layer/v1/api/historico-pessoa/{f_token_retorno}',
                                   headers={'Authorization': 'Bearer 8edfad4d-63f4-43fd-83ce-462962f024d8'})
    lista_dados = json.loads(servico.content)
    print(lista_dados)
    # deleta os campos Version
    lista_dados.pop(versao)

   ## print(lista_dados["contasBancarias"])
    i = 0
    while i < (len(lista_dados['contasBancarias'])):
        print(lista_dados['contasBancarias'][i]['id'])
        if lista_dados['contasBancarias'][i]['id'] == tabela.idconta:
            lista_dados['contasBancarias'][i]["tipo"] = "ELETRONICA"
        i += 1
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
        ##Update
        stmt = (update(Conta).where(Conta.idconta == tabela.idconta).values(protocolo=str(f_token_retorno)))
        print(stmt)
        session.execute(stmt)
        session.commit()
    elif status == 500:
        print('Erro no envio - verifique os paramêtros')
