from sqlalchemy.orm import sessionmaker
from sqlalchemy import extract, update,insert, text
from tabelatributos import Pensend
from conexao import engine, connection
import requests, json

#################################################
#           Conexão com banc de dados           #
#################################################
Session = sessionmaker(bind=engine)
session = Session()

#### Linka
urlPost = 'https://saude.betha.cloud/api-saude-service-layer/v2/api/atendimentosDomiciliares'
#### Token da Endidade

tokenEntidade = 'fe8704a7-f0da-41f9-91d1-acf75a1d5a0f'

'''Utilizando select direto'''

query = text("select ad.sequenc, ad.id_cidadao,ad.id_unidade,ad.id_profissional,classificacaocarater,"
             "ad.dataatendimento, ad.ds_filtro_cids, ad.modalidade from cnv_atend_domiciliar ad "
             "inner join cnv_id_atendom a on a.sequenc = ad.sequenc"
             " where a.protocolo is null and modalidade  in ('AD3','AD1','AD2') "
             " and id_gerado is null  and ad.sequenc > 538541 "
             "order by ad.sequenc ")

##query = text("SELECT * FROM ajustedtisencao where protocolo is null")
resultados = session.execute(query).fetchall()
batch_size = 40
cids = []
data = []
for tabela in resultados:
    print(f'{tabela.sequenc} - {tabela.id_cidadao} - {tabela.id_profissional}')
    ## Verifica o Cid
    def processar_codigos(dados):
        # Inicializa as variáveis com None
        cod_01 = None
        cod_02 = None
        cod_03 = None

        # Extrai os códigos para uma lista, independentemente da quantidade
        codigos = dados.split('|')[1:-1]

        # Verifica o tamanho da lista e atribui os valores
        if len(codigos) >= 1:
            cod_01 = codigos[0]
        if len(codigos) >= 2:
            cod_02 = codigos[1]
        if len(codigos) >= 3:
            cod_03 = codigos[2]

        return cod_01, cod_02, cod_03

    dados = tabela.ds_filtro_cids
    print(dados)
    c1, c2, c3 = processar_codigos(dados)
    idCids01 = ""
    idCids02 = None
    idCids03 = None
    cidprincipal = {}
    cidsecund01 = {}
    cidsecund02 = {}
    if c1 is not None:
        query1 = text(f"select  id_gerado from cnv_cids where codigo = :codigo")
        resultados = session.execute(query1, {"codigo": c1}).fetchall()
        if resultados:
            idCids01 = resultados[0][0]
            print(idCids01)
            cidprincipal = {'cidPrincipal': {'id': idCids01}}
            print(cidprincipal)
    if c2 is not None:
        query2 = text(f"select  id_gerado from cnv_cids where codigo = :codigo")
        resultados = session.execute(query2, {"codigo": c2}).fetchall()
        if resultados:
            idCids02 = resultados[0][0]
            print(idCids02)
            cidsecund01 = {'cidSecundario1': {'id': idCids02}}
    if c3 is not None:
        query3 = text(f"select  id_gerado from cnv_cids where codigo = :codigo")
        resultados = session.execute(query3, {"codigo": c3}).fetchall()
        if resultados:
            idCids03 = resultados[0][0]
            print(idCids03)
            cidsecund02 = {'cidSecundario2': {'id': idCids03}}

    print(f'Dados: {idCids01}-{idCids02}-{idCids03}')
    ## Modalidade
    modalid = ""
    modalidade = tabela.modalidade
    if modalidade == 'AD1':
        modalid = 5050
    elif modalidade == 'AD2':
        modalid = 5051
    elif modalidade == 'AD3':
        modalid = 5052
    print(f'Modalidade: {modalid} - {modalidade}')
    print(modalidade)
    data.append({
        "idIntegracao": f"{tabela.sequenc}",
        "atendimentoDomiciliar": {
            "cliente": {
                "id": tabela.id_cidadao
            },
            "profissionalSolicitante": {
                "id": tabela.id_profissional
            },
            "unidadeSolicitante": {
                "id": tabela.id_unidade
            },
            "classificacaoSituacao": {
                "id": 5025
            },
            "dataSolicitacao": f"{tabela.dataatendimento}",
            "classificacaoProcedencia": {
                "id": 5031
            },
            "dataAvaliacao": f"{tabela.dataatendimento}"

        }
	})
    print(len(data))
    if len(data) == batch_size:
        print(data)
        dados = json.dumps(data, ensure_ascii=False)
        print(dados)
        response = requests.post(urlPost,
                                 headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                                 data=json.dumps(data))
        status = response.status_code
        print(response.content)
        print(status)
        if status == 200:
    ##        print("entrou")
            recurrence = json.loads(response.content)
            f_token_retorno = recurrence['idLote']
            print(f'protocolo {f_token_retorno}')

            for item in data:
                stmt = text(f"UPDATE cnv_id_atendom SET protocolo = '{f_token_retorno}' WHERE  sequenc = '{item['idIntegracao']}'")
                session.execute(stmt)
            session.commit()
        else:
            print(f'erro execução')
        ## Zerar lista
        data = []
if data:
    print(data)
    dados = json.dumps(data)
    print(dados)
    response = requests.post(urlPost,
                             headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                             data=json.dumps(data))
    status = response.status_code
    print(response.content)
    print(status)
    if status == 200:
        ##        print("entrou")
        recurrence = json.loads(response.content)
        f_token_retorno = recurrence['idLote']
        print(f'protocolo {f_token_retorno}')
        for item in data:
            stmt = text(
                f"UPDATE cnv_id_atendom SET protocolo = '{f_token_retorno}' WHERE  sequenc = '{item['idIntegracao']}'")
            session.execute(stmt)
        session.commit()
    else:
        print(f'erro execução')