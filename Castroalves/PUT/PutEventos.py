from sqlalchemy.orm import sessionmaker
from sqlalchemy import update, insert
from tabelas import Hmatricula, Conta, Eventos
from conexao import engine,connection
from datetime import datetime
import requests, json


#################################################
#           Conexão com banc de dados           #
#################################################
Session = sessionmaker(bind=engine)
session = Session()

#### Linka
urlPost = 'https://pessoal.betha.cloud/service-layer/v1/api/historico-configuracao-evento/'
#### Token da Endidade

tokenEntidade = '8edfad4d-63f4-43fd-83ce-462962f024d8'

tabelas = session.query(Eventos).all()
##tabelas = session.query(Conta).where(Conta.idpessoa =='18275863')
lista_dados = []
campo_alvo  = "situacao"  # Campo que será alterado
campo_alterar = "ELETRONICA"
versao      = "version"  # Campo que deverá ser deletado da lista

for tabela in tabelas:
    f_token_retorno = tabela.id
    print(f"conta {tabela.codigo} e id{f_token_retorno}")
    servico = requests.get(f'https://pessoal.betha.cloud/service-layer/v1/api/historico-configuracao-evento/{f_token_retorno}',
                                   headers={'Authorization': 'Bearer 8edfad4d-63f4-43fd-83ce-462962f024d8'})
    lista_dados = json.loads(servico.content)
    print(lista_dados)
    # deleta os campos Version
    lista_dados.pop(versao)
    lista_dados['codigoEsocial'] = tabela.codigo


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
        recurrence = json.loads(response.content)
        f_token_retorno = recurrence['id']
        print(f'protocolo {f_token_retorno}')
        print('Erro no envio - verifique os paramêtros')
