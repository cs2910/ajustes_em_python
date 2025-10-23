from sqlalchemy.orm import sessionmaker
from sqlalchemy import update, insert
from tabelas import Hmatricula, Conta, Histpessoas
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

##tabelas = session.query(Histpessoas).all()
tabelas = session.query(Histpessoas).where(Histpessoas.executar == 'S')
##tabelas = session.query(Histpessoas).where(Histpessoas.naturalidade = '')
lista_dados = []
versao = "version"  # Campo que deverá ser deletado da lista


for tabela in tabelas:
    f_token_retorno = tabela.ids
    print(f"Historico {tabela.nome} e id {f_token_retorno}")
    servico = requests.get(f'https://pessoal.betha.cloud/service-layer/v1/api/historico-pessoa/{f_token_retorno}',
                                   headers={'Authorization': 'Bearer 8edfad4d-63f4-43fd-83ce-462962f024d8'})
    lista_dados = json.loads(servico.content)
    ##print(lista_dados)
    temajuste = 'Não'
    # deleta os campos Version
    lista_dados.pop(versao)
    lista_dados.pop("camposAdicionais")
    print(lista_dados)
    for endereco in lista_dados.get('enderecos', []):
        if endereco.get('principal', False):  # Verifica se é principal
            if 'id' in endereco:  # Remove somente o campo "id"
                del endereco['id']

    print("Mapa atualizado:", lista_dados)

    print(endereco)
    ## Verifica se tem CPF
    if lista_dados['cpf'] is None and tabela.cpf is not None:
        cpf_completo = tabela.cpf.zfill(11)
        lista_dados['cpf'] = cpf_completo
        print('cpf')
        temajuste = 'Sim'
    ## Verifica se tem data de nascimento
    if lista_dados['dataNascimento'] is None and tabela.dtnasc is not None:
        temajuste = 'Sim'
        lista_dados['dataNascimento'] = tabela.dtnasc
        print('Dt')
    ## Verifica se tem sexo
    if lista_dados['sexo'] is None and tabela.sexo is not None:
        temajuste = 'Sim'
        lista_dados['sexo'] = tabela.sexo
        print('sx')
    ## Verifica se tem Estado Civil
    if lista_dados['estadoCivil'] is None and tabela.estadocivil is not None:
        lista_dados['estadoCivil'] = tabela.estadocivil
        print('estcivi')
        temajuste = 'Sim'
    ## Verifica se tem Nacionalidade
    if lista_dados['nacionalidade'] == []:
        lista_dados['nacionalidade'] = '["id": 29]'
        temajuste = 'Sim'
        print('nacio')
    ## Verifica se tem PIS
    ##print(f"Pis {lista_dados['pis']} pis tabela {tabela.pis}")
    if lista_dados['pis'] is None and tabela.pis is not None:
        print(f'pis = {tabela.pis}')
        temajuste = 'Sim'
        lista_dados['pis'] = tabela.pis
    ## Verifica se tem Raca
    if lista_dados['raca'] is None and tabela.raca is not None:
        print(f'raca = {tabela.raca} e {lista_dados['raca']}')
        temajuste = 'Sim'
        lista_dados['raca'] = tabela.raca
    ## Verifica se tem naturalidade
    ##print(tabela.naturalidade)
    if lista_dados['naturalidade'] is None and tabela.naturalidade is not None:
        print(f'naturalidade = {tabela.naturalidade} cloud {lista_dados['naturalidade']}')
        lista_dados['naturalidade'] = {'id': tabela.naturalidade}
        temajuste = 'Sim'

    ## Verifica se tem RG
    if lista_dados['identidade'] is None and tabela.rg is not None:
        print(f'Ident = {tabela.rg}')
        temajuste = 'Sim'
        lista_dados['identidade'] = tabela.rg

    ## Verifica se tem Orgao emissor RG
    if lista_dados['orgaoEmissorIdentidade'] is None and tabela.orgemissor is not None:
        print(f'orgaoEmissorRg = {tabela.orgemissor}')
        temajuste = 'Sim'
        lista_dados['orgaoEmissorIdentidade'] = tabela.orgemissor

    ## Verifica se tem UF RG
    if lista_dados['ufEmissaoIdentidade'] is None and tabela.uf is not None:
        print(f'orgaoEmissorRg = {tabela.uf}')
        temajuste = 'Sim'
        lista_dados['ufEmissaoIdentidade'] = tabela.uf

    ## Verifica se tem titulo
    if lista_dados['tituloEleitor'] is None and tabela.titulo is not None:
        print(f'orgaoEmissorRg = {tabela.titulo}')
        temajuste = 'Sim'
        lista_dados['tituloEleitor'] = tabela.titulo

    ## Verifica se tem zona
    if lista_dados['zonaEleitoral'] is None and tabela.zona is not None:
        print(f'zona = {tabela.zona}')
        temajuste = 'Sim'
        lista_dados['zonaEleitoral'] = tabela.zona

    ## Verifica se tem secao
    if lista_dados['secaoEleitoral'] is None and tabela.secao is not None:
        print(f'secaoEleitoral = {tabela.secao}')
        temajuste = 'Sim'
        lista_dados['secaoEleitoral'] = tabela.secao

    ## Verifica se tem ctps
    if lista_dados['ctps'] is None and tabela.ctps is not None:
        print(f'ctps = {tabela.ctps}')
        temajuste = 'Sim'
        lista_dados['ctps'] = tabela.ctps

    ## Verifica se tem serie
    if lista_dados['serieCtps'] is None and tabela.seriectps is not None:
        print(f'serieCtps = {tabela.seriectps}')
        temajuste = 'Sim'
        lista_dados['serieCtps'] = tabela.seriectps

    ## Verifica se tem uf ctps
    if lista_dados['ufEmissaoCtps'] is None and tabela.ufctps is not None:
        print(f'ufEmissaoCtps = {tabela.ufctps}')
        temajuste = 'Sim'
        lista_dados['ufEmissaoCtps'] = tabela.ufctps

    ## Verifica se tem emissao Ctps
    if lista_dados['dataEmissaoCtps'] is None and tabela.emissaoctps is not None:
        print(f'dataEmissaoCtps = {tabela.emissaoctps}')
        temajuste = 'Sim'
        data_format = datetime.strptime(tabela.emissaoctps, '%d/%m/%Y').strftime('%Y-%m-%d')
        print(data_format)
        lista_dados['dataEmissaoCtps'] = data_format

    ## Verifica se tem grauinstrucao
    if lista_dados['grauInstrucao'] is None and tabela.grauinstrucao is not None:
        print(f'grauinstrucao = {tabela.grauinstrucao}')
        temajuste = 'Sim'
        lista_dados['grauInstrucao'] = tabela.grauinstrucao

    ## Verifica se tem sitgrauinstrucao
    if lista_dados['situacaoGrauInstrucao'] is None and tabela.sitgrauinstrucao is not None:
        print(f'sitgrauinstrucao = {tabela.sitgrauinstrucao}')
        temajuste = 'Sim'
        lista_dados['situacaoGrauInstrucao'] = tabela.sitgrauinstrucao

    ## print(lista_dados["contasBancarias"])
    print(temajuste)
    temajuste = 'Sim'
    dados = (json.dumps(lista_dados))
    print(dados)
    if temajuste == 'Sim':
        print(f'Tem alteração {tabela.ids}')


    ###################################################
    #   Montar o cabeçalho para rodar o metodo PUT    #
    ###################################################
        montar_json = '[{'f'"idIntegracao": "string {f_token_retorno}","idGerado": "lote {f_token_retorno}","conteudo":{dados}''}]'
        print(montar_json)
        response = requests.post(urlPost,
                            headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                            data=montar_json)
        status = response.status_code
        print (status)
        if status == 200:
            recurrence = json.loads(response.content)
            f_token_retorno = recurrence['id']
            print(f'protocolo {f_token_retorno}')
        ##Update
            stmt = (update(Histpessoas).where(Histpessoas.ids == tabela.ids).values(protocolo=str(f_token_retorno)))
            print(stmt)
            session.execute(stmt)
            session.commit()
        elif status == 500:
            print('Erro no envio - verifique os paramêtros')
    else:
        stmt = (update(Histpessoas).where(Histpessoas.ids == tabela.ids).values(executar='N'))
        print(stmt)
        session.execute(stmt)
        session.commit()