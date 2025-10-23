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
urlPost = 'https://saude.betha.cloud/api-saude-service-layer/v2/api/atendimentos'
#### Token da Endidade

tokenEntidade = '38096b6b-d3b5-4895-8c0b-92d355c7dee3'

'''Utilizando select direto'''

query = text("select ad.sequenc, ad.id_cidadao,ad.id_unidade,ad.id_profissional,classificacaocarater,"
             "ad.dataatendimento, ad.ds_filtro_cids, ad.modalidade, ad.id_cbo, desf.id_gerado as id_desfecho, a.codigo  from cnv_atend_domiciliar ad "
             "inner join cnv_id_atendom a on "
             "  a.sequenc = ad.sequenc "
             "inner join tb_fat_atendimento_domiciliar fad on "
             "  fad.co_seq_fat_atend_domiciliar = ad.sequenc "
             "left join tb_atend_prof ap on "
             "  ap.co_unico_atend_prof = fad.nu_uuid_ficha "
             "left join cnv_antenddesfecho desf on"
             " desf.sequenc = ad.sequenc "
             "where ap.co_unico_atend_prof is null "
             "and modalidade  in ('AD3','AD1','AD2') "
             "and not exists (select 1 from cnv_atendimento where sequenc = a.sequenc)"
             "and a.id_gerado is not null")

##query = text("SELECT * FROM ajustedtisencao where protocolo is null")
resultados = session.execute(query).fetchall()
batch_size = 50
cids = []
data = []
for tabela in resultados:
    print(f'{tabela.sequenc} - {tabela.id_cidadao} - {tabela.id_profissional} - {tabela.codigo}')
    ## Verifica o Cid
    dados = tabela.ds_filtro_cids
    print(dados)
    ## Modalidade
    desfecho = {}
    if tabela.id_desfecho is not None:
        desfecho = {"desfecho": {"id": tabela.id_desfecho}}

    data.append({
        "idIntegracao": f"{tabela.sequenc}",
        "atendimento": {
            "cliente": {
                "id": tabela.id_cidadao
            },
            "profissional": {
                "id": tabela.id_profissional
            },
            "profissionalFaturamento":{
                "id": tabela.id_profissional
            },
            "cbo": {
                "id": tabela.id_cbo
            },
            "unidade": {
                "id": tabela.id_unidade
            },
            "classificacaoCarater": {
                "id": 1029
            },
            "classificacaoSituacao": {
                "id": 1249
            },
            "classificacaoOrigemAtendimento": {
                "id": 5100
            },
            "filaAtendimento": {
                "id": 502148
            },
            **desfecho,
            "dataHoraEntrada": f"{tabela.dataatendimento} 10:00:00",
            "dataHoraInicioAtendimento": f"{tabela.dataatendimento} 10:00:00",
            "dataHoraFimAtendimento": f"{tabela.dataatendimento} 10:00:00"
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
                stmt = text(f"insert into cnv_atendimento(sequenc,protocolo) values({item['idIntegracao']},'{f_token_retorno}')")
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
            stmt = text(f"insert into cnv_atendimento(sequenc,protocolo) values({item['idIntegracao']},'{f_token_retorno}')")
            session.execute(stmt)
        session.commit()
    else:
        print(f'erro execução')