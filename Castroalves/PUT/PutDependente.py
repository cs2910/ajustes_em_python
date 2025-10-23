from sqlalchemy.orm import sessionmaker
from sqlalchemy import update, insert
from tabelas import Hmatricula, Situacao, Dependente
from conexao import engine,connection
from datetime import datetime
import requests, json


#################################################
#           Conexão com banc de dados           #
#################################################
Session = sessionmaker(bind=engine)
session = Session()

#### Linka
urlPost = 'https://pessoal.betha.cloud/service-layer/v1/api/dependencia/'
#### Token da Endidade

tokenEntidade = '8edfad4d-63f4-43fd-83ce-462962f024d8'

tabelas = session.query(Dependente).all()
##tabelas = session.query(Dependente).where(Dependente.matric =='2878666')
lista_dados = []
campo_alvo  = "irrf"  # Campo que será alterado
campo_alterar = "false"
versao      = "version"  # Campo que deverá ser deletado da lista

for tabela in tabelas:
    f_token_retorno = tabela.matric
    print(f"func {tabela.matric} ")



    servico = requests.get(f'https://pessoal.betha.cloud/service-layer/v1/api/dependencia/{f_token_retorno}',
                                   headers={'Authorization': 'Bearer 8edfad4d-63f4-43fd-83ce-462962f024d8'})
    lista_dados = json.loads(servico.content)
    print(lista_dados)
    # deleta os campos Version
    lista_dados.pop(versao)
    lista_dados[campo_alvo] = 'false'

    if lista_dados["eventoPensao"] is None:
        lista_dados.pop("eventoPensao")
##        print("eventoPensao ")
    if lista_dados["grauDescricao"] is None:
        lista_dados.pop("grauDescricao")
        ##        print("grauDescricao ")
    if lista_dados["dataTermino"] is None:
        lista_dados.pop("dataTermino")
        ##print("dataTermino ")
    if lista_dados["motivoTermino"] is None:
        lista_dados.pop("motivoTermino")
        ##print("motivoTermino ")
    if lista_dados["dataCasamento"] is None:
        lista_dados.pop("dataCasamento")
        ##print("dataCasamento ")
    if lista_dados["dataInicioCurso"] is None:
        lista_dados.pop("dataInicioCurso")
        ##print("dataInicioCurso ")
    if lista_dados["dataFinalCurso"] is None:
        lista_dados.pop("dataFinalCurso")
        ##print("dataFinalCurso ")
    if lista_dados["dataInicioBeneficio"] is None:
        lista_dados.pop("dataInicioBeneficio")
        ##print("dataInicioBeneficio ")
    if lista_dados["duracao"] is None:
        lista_dados.pop("duracao")
        ##print("duracao ")
    if lista_dados["dataVencimento"] is None:
        lista_dados.pop("dataVencimento")
        ##print("dataVencimento ")
    if lista_dados["alvaraJudicial"] is None:
        lista_dados.pop("alvaraJudicial")
        ##print("alvaraJudicial ")
    if lista_dados["dataAlvara"] is None:
        lista_dados.pop("dataAlvara")
        ##print("dataAlvara ")
    if lista_dados["aplicacaoDesconto"] is None:
        lista_dados.pop("aplicacaoDesconto")
        ##print("aplicacaoDesconto ")
    if lista_dados["valorDesconto"] is None:
        lista_dados.pop("valorDesconto")
        ##print("valorDesconto ")
    if lista_dados["percentualDesconto"] is None:
        lista_dados.pop("percentualDesconto")
        ##print("percentualDesconto ")

    if lista_dados["percentualPensaoFgts"] is None:
        lista_dados.pop("percentualPensaoFgts")
        ##print("percentualPensaoFgts ")

    if lista_dados["formaPagamento"] is None:
        lista_dados.pop("formaPagamento")
        ##print("formaPagamento ")

    if lista_dados["contaBancaria"] is None:
        lista_dados.pop("contaBancaria")
        ##print("contaBancaria ")

    if lista_dados["camposAdicionais"] == []:
        lista_dados.pop("camposAdicionais")
        ##print("camposAdicionais ")
    if lista_dados["responsaveis"] == []:
        lista_dados.pop("responsaveis")
        ##print("responsaveis ")

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
        print(recurrence)
        f_token_retorno = recurrence['id']
        print(f'protocolo {f_token_retorno}')

    elif status == 500:
        print('Erro no envio - verifique os paramêtros')
