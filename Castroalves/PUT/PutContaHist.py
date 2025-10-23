from sqlalchemy.orm import sessionmaker
from sqlalchemy import update, insert
from tabelas import Hmatricula, Conta, Histpess, Pessoashist
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

##tabelas = session.query(Conta).all()
tabelas = session.query(Histpess).where(Histpess.origem != 0, Histpess.origem == 18280627)
lista_dados = []
campo_alvo  = "situacao"  # Campo que será alterado
campo_alterar = "ELETRONICA"
versao      = "version"  # Campo que deverá ser deletado da lista


for tabela in tabelas:
    f_token_retorno = tabela.ids
    print(f"Historico {tabela.origem} e id {f_token_retorno}")
    servico = requests.get(f'https://pessoal.betha.cloud/service-layer/v1/api/historico-pessoa/{f_token_retorno}',
                                   headers={'Authorization': 'Bearer 8edfad4d-63f4-43fd-83ce-462962f024d8'})
    lista_dados = json.loads(servico.content)
    print(lista_dados)
    temajuste = 'Não'
    # deleta os campos Version
    lista_dados.pop(versao)
    ## Verifica se tem CPF
    if lista_dados['cpf'] is None and tabela.cpf is not None:
        cpf_completo = tabela.cpf.zfill(11)
        print(cpf_completo)
        temajuste = 'Sim'
        lista_dados['cpf'] = cpf_completo
        print('cpf')
    ## Verifica se tem data de nascimento
    if lista_dados['dataNascimento'] is None and tabela.datanascimento is not None:
        temajuste = 'Sim'
        lista_dados['dataNascimento'] = tabela.datanascimento
        print('Dt')
    ## Verifica se tem sexo
    if lista_dados['sexo'] is None and tabela.sexo is not None:
        temajuste = 'Sim'
        lista_dados['sexo'] = tabela.sexo
        print('sx')
    ## Verifica se tem Estado Civil
    if lista_dados['estadoCivil'] is None and tabela.estadocivil is not None:
        temajuste = 'Sim'
        lista_dados['estadoCivil'] = tabela.estadocivil
        print('estcivi')
    ## Verifica se tem Nacionalidade
    if lista_dados['nacionalidade'] == []:
        temajuste = 'Sim'
        lista_dados['nacionalidade'] = '["id": 29]'
        print(f'Sem data Nacionalidade id {tabela.ids}')
    ## Verifica se tem PIS
    if lista_dados['pis'] is None and tabela.numeropispasep is not None:
        print(f'pis = {tabela.numeropispasep}')
        temajuste = 'Sim'
        lista_dados['pis'] = tabela.numeropispasep
        print('pis')
    ## Verifica se tem Grau de Escolaridade
    if lista_dados['grauInstrucao'] is None and tabela.grauescolaridade is not None:
        temajuste = 'Sim'
        lista_dados['grauInstrucao'] = tabela.grauescolaridade
        print('grau')



    ## print(lista_dados["contasBancarias"])

    if temajuste == 'Sim':
        print(f'Tem alteração {tabela.ids}')
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
            stmt = (update(Histpess).where(Histpess.ids == tabela.ids).values(protocolo=str(f_token_retorno)))
            print(stmt)
            session.execute(stmt)
            session.commit()
        elif status == 500:
            print('Erro no envio - verifique os paramêtros')
