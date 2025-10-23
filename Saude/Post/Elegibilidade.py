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
##--classificacaocarater,
query = text("select sequenc, id_cidadao,id_unidade,id_profissional,cidprincipal,cidsec1,cidsec2, "
             "dataatendimento, modalidade, procedencia, elegib, cuidador, no_cuidador from cnv_elegibilidade e "
             "left join tb_atend_prof p on"
             " p.co_unico_atend_prof = e.nu_uuid_ficha "
             "left join tb_atend_prof_ad ad on "
             "ad.co_atend_prof_ad = p.co_seq_atend_prof"
             " where protocolo is null and sequenc > 133501"
             "order by sequenc ")

##query = text("SELECT * FROM ajustedtisencao where protocolo is null")
resultados = session.execute(query).fetchall()
batch_size = 50
cids = []
data = []
for tabela in resultados:
    print(f'{tabela.sequenc} - {tabela.id_cidadao} - {tabela.id_profissional}')
    ## Verifica o Cid
    cidprincipal = {}
    cidsecund01 = {}
    cidsecund02 = {}
    elegib = {}
    cuidador = {}
    nomeCuidador = {}
    if tabela.cidprincipal is not None:
        cidprincipal = {'cidPrincipal': {'id': tabela.cidprincipal}}
    if tabela.cidsec1 is not None and tabela.cidsec1 != 1:
        cidsecund01 = {'cidSecundario1': {'id': tabela.cidsec1}}
    if tabela.cidsec2 is not None and tabela.cidsec2 != 1:
        cidsecund02 = {'cidSecundario2': {'id': tabela.cidsec2}}

    ## Modalidade
    modalid = ""
    modalidade = tabela.modalidade
    if modalidade == 'AD1':
        modalid = 5050
    elif modalidade == 'AD2':
        modalid = 5051
    elif modalidade == 'AD3':
        modalid = 5052
    elif modalidade == 'Inelegível':
        modalid = 5053
    print(f'Modalidade: {modalid} - {modalidade}')
    ## Procedencia
    if tabela.procedencia == 1:
        procedencia = 5026
    elif tabela.procedencia == 2:
        procedencia = 5030
    elif tabela.procedencia == 3:
        procedencia = 5028
    elif tabela.procedencia == 4:
        procedencia = 5029
    elif tabela.procedencia == 5:
        procedencia = 5031
    ## Elegibilidade
    if tabela.elegib == 1:
        elegib = {'classificacaoDestino': {'id': 5046}}
    elif tabela.elegib == 2:
        elegib = {'classificacaoDestino': {'id': 5047}}
    elif tabela.elegib == 3:
        elegib = {'classificacaoDestino': {'id': 5048}}
    elif tabela.elegib == 4:
        elegib = {'classificacaoDestino': {'id': 5049}}

    ## Cuidador
    if tabela.cuidador == 1:
        cuidador = {'classificacaoTipoCuidador': {'id': 5038}}
    elif tabela.cuidador == 2:
        cuidador = {'classificacaoTipoCuidador': {'id': 5039}}
    elif tabela.cuidador == 3:
        cuidador = {'classificacaoTipoCuidador': {'id': 5040}}
    elif tabela.cuidador == 4:
        cuidador = {'classificacaoTipoCuidador': {'id': 5041}}
    elif tabela.cuidador == 5:
        cuidador = {'classificacaoTipoCuidador': {'id': 5042}}
    elif tabela.cuidador == 6:
        cuidador = {'classificacaoTipoCuidador': {'id': 5043}}
    elif tabela.cuidador == 7:
        cuidador = {'classificacaoTipoCuidador': {'id': 5044}}
    elif tabela.cuidador == 8:
        cuidador = {'classificacaoTipoCuidador': {'id': 5045}}
    ##NOme cuidador
    if tabela.no_cuidador is not None:
        nomeCuidador = {"clienteCuidador": {"nome": f"{tabela.no_cuidador}"}}


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
                "id": procedencia
            },
            "atendimentoDomiciliarAvaliarElegibilidade": {
                **cidprincipal,
                **cidsecund01,
                **cidsecund02,
                **elegib,
                "classificacaoFrequencia": {
                    "id": 5037
                },
                "classificacaoModalidade": {
                    "id": modalid
                },
                **cuidador,
                **nomeCuidador,
                "unidadePrestadora": {
                    "id": tabela.id_unidade
                }
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
                stmt = text(f"UPDATE cnv_elegibilidade SET protocolo = '{f_token_retorno}' WHERE  sequenc = '{item['idIntegracao']}'")
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
                f"UPDATE cnv_elegibilidade SET protocolo = '{f_token_retorno}' WHERE  sequenc = '{item['idIntegracao']}'")
            session.execute(stmt)
        session.commit()
    else:
        print(f'erro execução')