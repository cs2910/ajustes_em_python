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
urlPost = 'https://saude.betha.cloud/api-saude-service-layer/v2/api/atendimentoCidCiaps'
#### Token da Endidade

tokenEntidade = '38096b6b-d3b5-4895-8c0b-92d355c7dee3'

'''Utilizando select direto'''

query = text("select  ad.sequenc,ds_filtro_cids, replace(ds_filtro_ciaps,'|','') cipas,a.id_gerado from cnv_atendimento a "
             "inner join cnv_id_atendom c on "
             "c.sequenc = a.sequenc "
             "inner join cnv_atend_domiciliar ad on"
             " ad.sequenc = a.sequenc "
             "where ad.sequenc not in (1, 501, 16, 503, 9008,538530,538531, 538525, 538509, 538511, 538503, 502, 504, 2 ) and ad.sequenc = 179509 and "
             "(ds_filtro_cids not ilike '%||%' or ds_filtro_ciaps not ilike '%||%') and  "
             "not exists (select sequenc from cnv_idatendimentoscds where sequenc = ad.sequenc and idatend = a.id_gerado) order by ad.sequenc")

resultados = session.execute(query).fetchall()
batch_size = 1
cids = []
data = []
for tabela in resultados:
    print(f'{tabela.sequenc} - {tabela.id_gerado} - {tabela.ds_filtro_cids}')
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
    ciaps = {}
    ciapstabela = 'Null'
    if tabela.cipas is not None and tabela.cipas != '':
        ciaps = {"ciap": {"id": f'{tabela.cipas}'}}
        ciapstabela = tabela.cipas

    if c1 == '' and tabela.cipas is not None:
        data.append({
            "idIntegracao": f"{tabela.sequenc}",
            "atendimentoCidCiap": {
                "atendimento": {"id": tabela.id_gerado},
                "ciap": {"id": f'{tabela.cipas}'}
            }
        })
    if c1 is not None:
        query1 = text(f"select  id_gerado from cnv_cids where codigo = :codigo")
        resultados = session.execute(query1, {"codigo": c1}).fetchall()
        if resultados:
            idCids01 = resultados[0][0]
            print(idCids01)
            data.append({
                "idIntegracao": f"{tabela.sequenc}",
                "atendimentoCidCiap": {
                    "atendimento": {"id": tabela.id_gerado},
                    "cid": {"id": idCids01},
                    **ciaps
                }
            })

            cidprincipal = {'cidPrincipal': {'id': idCids01}}
            print(cidprincipal)
    if c2 is not None:
        query2 = text(f"select  id_gerado from cnv_cids where codigo = :codigo")
        resultados = session.execute(query2, {"codigo": c2}).fetchall()
        if resultados:
            idCids02 = resultados[0][0]
            print(idCids02)
            data.append({
                "idIntegracao": f"{tabela.sequenc}",
                "atendimentoCidCiap": {
                    "atendimento": {"id": tabela.id_gerado},
                    "cid": {"id": idCids02}
                }
            })
    if c3 is not None:
        query3 = text(f"select  id_gerado from cnv_cids where codigo = :codigo")
        resultados = session.execute(query3, {"codigo": c3}).fetchall()
        if resultados:
            idCids03 = resultados[0][0]
            print(idCids03)
            data.append({
                "idIntegracao": f"{tabela.sequenc}",
                "atendimentoCidCiap": {
                    "atendimento": {"id": tabela.id_gerado},
                    "cid": {"id": idCids03}
                }
            })
    print(f'Dados: {idCids01}-{idCids02}-{idCids03}')
    print(len(data))
    if len(data) >= 1 :
        print(data)
        dados = json.dumps(data, ensure_ascii=False)
        print(dados)
        response = requests.post(urlPost,
                                 headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                                 data=json.dumps(data))
        status = response.status_code
        print(response.content)
        ##status = 200
        if status == 200:
    ##        print("entrou")
            recurrence = json.loads(response.content)
            f_token_retorno = recurrence['idLote']
            print(f'protocolo {f_token_retorno}')

            for item in data:
                dict_aninhado_1 = item['atendimentoCidCiap']
                # Verificamos se a chave 'cid' existe
                if 'cid' in dict_aninhado_1:
                    cid = item['atendimentoCidCiap']['cid']['id']
                else:
                    cid = 'Null'
                idatend = item['atendimentoCidCiap']['atendimento']['id']
                idsequenc= item['idIntegracao']

                stmt = text(
                    f"insert into cnv_idatendimentoscds(sequenc,idatend,cds,ciaps,protocolo) values({idsequenc},{idatend},{cid},'{ciapstabela}','{f_token_retorno}')")
                session.execute(stmt)
            session.commit()
        else:
            print(f'erro execução')
        ## Zerar lista
        data = []
