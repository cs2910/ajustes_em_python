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
urlPost = 'https://saude.betha.cloud/api-saude-service-layer/v2/api/atendimentoDomiciliarCondicaoAvaliadas'
#### Token da Endidade

tokenEntidade = 'ba7177f4-8317-4adb-9e35-3edeca0e5306'

'''Utilizando select direto'''

query = text("select st_condic_acamado,st_condic_domiciliado,st_condic_ulceras_feridas,st_condic_acompanham_nutricion,"
             "st_condic_uso_sonda_nasogastri,st_condic_uso_sonda_nasoentera,st_condic_uso_gastrostomia,st_condic_uso_colostomia,"
             "st_condic_uso_cistostomia,st_condic_uso_sond_vesic_demor,st_condic_acomp_pre_operatorio,st_condic_acomp_pos_operatorio,"
             "st_condic_adapt_uso_ortes_prot,st_condic_reabilita_domiciliar,st_condic_cuidd_paliat_oncolog,st_condic_cuidd_paliat_n_oncol,"
             "st_condic_oxigenoterapia_domic,st_condic_uso_traqueostomia,st_condic_uso_aspir_via_aerea,st_condic_suport_ventil_cpap,"
             "st_condic_suport_ventil_bipap,st_condic_dialise_peritonial,st_condic_paracentese,st_condic_medicacao_parenteral, a.codigo, a.id_gerado "
             "from cnv_atend_domiciliar ad "
             "inner join cnv_id_atendom a on a.sequenc = ad.sequenc "
             "where a.id_gerado is not null and protocolocaval is null")

##query = text("SELECT * FROM ajustedtisencao where protocolo is null")
resultados = session.execute(query).fetchall()
batch_size = 22
cids = []
data = []
for tabela in resultados:
    print(f'{tabela.codigo} - {tabela.id_gerado}')
    if tabela.st_condic_acamado == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoDomiciliarCondicaoAvaliada": {
                "atendimentoDomiciliar":{
                    "id":  tabela.id_gerado
                },
                "classificacaoCondicaoAvaliada": {
                    "id": 5054
                },
                "inativo": "false"
            }
        })
    if tabela.st_condic_oxigenoterapia_domic == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoDomiciliarCondicaoAvaliada": {
                "atendimentoDomiciliar":{
                    "id":  tabela.id_gerado
                },
                "classificacaoCondicaoAvaliada": {
                    "id": 5064
                },
                "inativo": "false"
            }
        })
    if tabela.st_condic_domiciliado == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoDomiciliarCondicaoAvaliada": {
                "atendimentoDomiciliar":{
                    "id":  tabela.id_gerado
                },
                "classificacaoCondicaoAvaliada": {
                    "id": 5061
                },
                "inativo": "false"
            }
        })
    if tabela.st_condic_ulceras_feridas == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoDomiciliarCondicaoAvaliada": {
                "atendimentoDomiciliar":{
                    "id":  tabela.id_gerado
                },
                "classificacaoCondicaoAvaliada": {
                    "id": 5069
                },
                "inativo": "false"
            }
        })

    if tabela.st_condic_acompanham_nutricion == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoDomiciliarCondicaoAvaliada": {
                "atendimentoDomiciliar": {
                    "id": tabela.id_gerado
                },
                "classificacaoCondicaoAvaliada": {
                    "id": 5055
                },
                "inativo": "false"
            }
        })
    if tabela.st_condic_uso_sonda_nasogastri == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoDomiciliarCondicaoAvaliada": {
                "atendimentoDomiciliar": {
                    "id": tabela.id_gerado
                },
                "classificacaoCondicaoAvaliada": {
                    "id": 5075
                },
                "inativo": "false"
            }
        })
    if tabela.st_condic_uso_sonda_nasoentera == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoDomiciliarCondicaoAvaliada": {
                "atendimentoDomiciliar": {
                    "id": tabela.id_gerado
                },
                "classificacaoCondicaoAvaliada": {
                    "id": 5074
                },
                "inativo": "false"
            }
        })
    if tabela.st_condic_uso_gastrostomia == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoDomiciliarCondicaoAvaliada": {
                "atendimentoDomiciliar": {
                    "id": tabela.id_gerado
                },
                "classificacaoCondicaoAvaliada": {
                    "id": 5073
                },
                "inativo": "false"
            }
        })
    if tabela.st_condic_uso_colostomia == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoDomiciliarCondicaoAvaliada": {
                "atendimentoDomiciliar": {
                    "id": tabela.id_gerado
                },
                "classificacaoCondicaoAvaliada": {
                    "id": 5072
                },
                "inativo": "false"
            }
        })
    if tabela.st_condic_uso_cistostomia == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoDomiciliarCondicaoAvaliada": {
                "atendimentoDomiciliar": {
                    "id": tabela.id_gerado
                },
                "classificacaoCondicaoAvaliada": {
                    "id": 5071
                },
                "inativo": "false"
            }
        })
    if tabela.st_condic_uso_sond_vesic_demor == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoDomiciliarCondicaoAvaliada": {
                "atendimentoDomiciliar": {
                    "id": tabela.id_gerado
                },
                "classificacaoCondicaoAvaliada": {
                    "id": 5076
                },
                "inativo": "false"
            }
        })
    if tabela.st_condic_acomp_pre_operatorio == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoDomiciliarCondicaoAvaliada": {
                "atendimentoDomiciliar": {
                    "id": tabela.id_gerado
                },
                "classificacaoCondicaoAvaliada": {
                    "id": 5056
                },
                "inativo": "false"
            }
        })
    if tabela.st_condic_acomp_pos_operatorio == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoDomiciliarCondicaoAvaliada": {
                "atendimentoDomiciliar": {
                    "id": tabela.id_gerado
                },
                "classificacaoCondicaoAvaliada": {
                    "id": 5057
                },
                "inativo": "false"
            }
        })
    if tabela.st_condic_adapt_uso_ortes_prot == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoDomiciliarCondicaoAvaliada": {
                "atendimentoDomiciliar": {
                    "id": tabela.id_gerado
                },
                "classificacaoCondicaoAvaliada": {
                    "id": 5058
                },
                "inativo": "false"
            }
        })
    if tabela.st_condic_reabilita_domiciliar == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoDomiciliarCondicaoAvaliada": {
                "atendimentoDomiciliar": {
                    "id": tabela.id_gerado
                },
                "classificacaoCondicaoAvaliada": {
                    "id": 5066
                },
                "inativo": "false"
            }
        })
    if tabela.st_condic_cuidd_paliat_oncolog == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoDomiciliarCondicaoAvaliada": {
                "atendimentoDomiciliar": {
                    "id": tabela.id_gerado
                },
                "classificacaoCondicaoAvaliada": {
                    "id": 5059
                },
                "inativo": "false"
            }
        })
    if tabela.st_condic_cuidd_paliat_n_oncol == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoDomiciliarCondicaoAvaliada": {
                "atendimentoDomiciliar": {
                    "id": tabela.id_gerado
                },
                "classificacaoCondicaoAvaliada": {
                    "id": 5060
                },
                "inativo": "false"
            }
        })
    if tabela.st_condic_uso_traqueostomia == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoDomiciliarCondicaoAvaliada": {
                "atendimentoDomiciliar": {
                    "id": tabela.id_gerado
                },
                "classificacaoCondicaoAvaliada": {
                    "id": 5077
                },
                "inativo": "false"
            }
        })
    if tabela.st_condic_uso_aspir_via_aerea == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoDomiciliarCondicaoAvaliada": {
                "atendimentoDomiciliar": {
                    "id": tabela.id_gerado
                },
                "classificacaoCondicaoAvaliada": {
                    "id": 5070
                },
                "inativo": "false"
            }
        })
    if tabela.st_condic_suport_ventil_cpap == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoDomiciliarCondicaoAvaliada": {
                "atendimentoDomiciliar": {
                    "id": tabela.id_gerado
                },
                "classificacaoCondicaoAvaliada": {
                    "id": 5068
                },
                "inativo": "false"
            }
        })
    if tabela.st_condic_suport_ventil_bipap == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoDomiciliarCondicaoAvaliada": {
                "atendimentoDomiciliar": {
                    "id": tabela.id_gerado
                },
                "classificacaoCondicaoAvaliada": {
                    "id": 5067
                },
                "inativo": "false"
            }
        })
    if tabela.st_condic_dialise_peritonial == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoDomiciliarCondicaoAvaliada": {
                "atendimentoDomiciliar": {
                    "id": tabela.id_gerado
                },
                "classificacaoCondicaoAvaliada": {
                    "id": 5062
                },
                "inativo": "false"
            }
        })
    if tabela.st_condic_paracentese == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoDomiciliarCondicaoAvaliada": {
                "atendimentoDomiciliar": {
                    "id": tabela.id_gerado
                },
                "classificacaoCondicaoAvaliada": {
                    "id": 5065
                },
                "inativo": "false"
            }
        })
    if tabela.st_condic_medicacao_parenteral == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoDomiciliarCondicaoAvaliada": {
                "atendimentoDomiciliar": {
                    "id": tabela.id_gerado
                },
                "classificacaoCondicaoAvaliada": {
                    "id": 5063
                },
                "inativo": "false"
            }
        })
    print(len(data))
    if len(data) > batch_size and batch_size <= 50:
        print(data)
        ##dados = json.dumps(data, ensure_ascii=False, indent=4) ## , indent=4 mostra o json em arvore
        try:
            dados = json.dumps(data, ensure_ascii=False) ## , indent=4 mostra o json em arvore
            print("Conversão para JSON bem-sucedida!")
            print(dados)
        except TypeError as e:
            print(f"Ocorreu um erro: {e}")
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
                stmt = text(f"UPDATE cnv_id_atendom SET protocolocaval = '{f_token_retorno}' WHERE  id_gerado = '{item['idIntegracao']}'")
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
                f"UPDATE cnv_id_atendom SET protocolocaval = '{f_token_retorno}' WHERE  id_gerado = '{item['idIntegracao']}'")
            session.execute(stmt)
        session.commit()
    else:
        print(f'erro execução')