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
urlPost = 'https://saude.betha.cloud/api-saude-service-layer/v2/api/atendimentoEvolucaoCondicoesAvaliadas'
#### Token da Endidade

tokenEntidade = '38096b6b-d3b5-4895-8c0b-92d355c7dee3'

'''Utilizando select direto'''
## Ajustar prontuarios existentes
'''query = text("select ad.sequenc,cia.codigo,ad.dataatendimento,id_cloud as id_gerado, st_condic_acamado,st_condic_domiciliado,st_condic_ulceras_feridas,st_condic_acompanham_nutricion,"
             "st_condic_uso_sonda_nasogastri,st_condic_uso_sonda_nasoentera,st_condic_uso_gastrostomia,st_condic_uso_colostomia,"
             "st_condic_uso_cistostomia,st_condic_uso_sond_vesic_demor,st_condic_acomp_pre_operatorio,st_condic_acomp_pos_operatorio,"
             "st_condic_adapt_uso_ortes_prot,st_condic_reabilita_domiciliar,st_condic_cuidd_paliat_oncolog,st_condic_cuidd_paliat_n_oncol,"
             "st_condic_oxigenoterapia_domic,st_condic_uso_traqueostomia,st_condic_uso_aspir_via_aerea,st_condic_suport_ventil_cpap,"
             "st_condic_suport_ventil_bipap,st_condic_dialise_peritonial,st_condic_paracentese,st_condic_medicacao_parenteral"
             " from cnv_ajustaatendimentos aa"
             " inner join tb_atend a on a.co_seq_atend = pk "
             "inner join tb_atend_prof f on f.co_seq_atend_prof = a.co_atend_prof "
             "inner join cnv_atend_domiciliar ad on ad.localizador = f.co_unico_atend_prof "
             "inner join cnv_id_atendom cia on cia.sequenc = ad.sequenc "
             "where aa.protocolo is null")'''
### Prontuarios novos
query = text("select ad.localizador, ad.sequenc,cia.codigo,ad.dataatendimento,aa.id_gerado, st_condic_acamado,st_condic_domiciliado,st_condic_ulceras_feridas,st_condic_acompanham_nutricion, "
             "st_condic_uso_sonda_nasogastri,st_condic_uso_sonda_nasoentera,st_condic_uso_gastrostomia,st_condic_uso_colostomia,"
             "st_condic_uso_cistostomia,st_condic_uso_sond_vesic_demor,st_condic_acomp_pre_operatorio,st_condic_acomp_pos_operatorio,"
             "st_condic_adapt_uso_ortes_prot,st_condic_reabilita_domiciliar,st_condic_cuidd_paliat_oncolog,st_condic_cuidd_paliat_n_oncol,"
             "st_condic_oxigenoterapia_domic,st_condic_uso_traqueostomia,st_condic_uso_aspir_via_aerea,st_condic_suport_ventil_cpap,"
             "st_condic_suport_ventil_bipap,st_condic_dialise_peritonial,st_condic_paracentese,st_condic_medicacao_parenteral "
             "from cnv_atendimento aa "
             "inner join cnv_atend_domiciliar ad on "
             "  ad.sequenc = aa.sequenc "
             "inner join cnv_id_atendom cia on "
             " cia.sequenc = ad.sequenc  where aa.protocoloavaliacao is null ")



resultados = session.execute(query).fetchall()
batch_size = 0
cids = []
data = []
for tabela in resultados:
    print(f'{tabela.codigo} - {tabela.id_gerado} - {tabela.sequenc}')
    if tabela.st_condic_acamado == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoEvolucaoCondicoesAvaliadas": {
                "atendimento":{
                    "id":  tabela.id_gerado
                },
                "classificacaoEvolucaoCondicaoAvaliada": {
                    "id": 5054
                },
                "checked": "true"
            }
        })
    if tabela.st_condic_oxigenoterapia_domic == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoEvolucaoCondicoesAvaliadas": {
                "atendimento":{
                    "id":  tabela.id_gerado
                },
                "classificacaoEvolucaoCondicaoAvaliada": {
                    "id": 5064
                },
                "checked": "true"
            }
        })
    if tabela.st_condic_domiciliado == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoEvolucaoCondicoesAvaliadas": {
                "atendimento":{
                    "id":  tabela.id_gerado
                },
                "classificacaoEvolucaoCondicaoAvaliada": {
                    "id": 5061
                },
                "checked": "true"
            }
        })
    if tabela.st_condic_ulceras_feridas == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoEvolucaoCondicoesAvaliadas": {
                "atendimento":{
                    "id":  tabela.id_gerado
                },
                "classificacaoEvolucaoCondicaoAvaliada": {
                    "id": 5069
                },
                "checked": "true"
            }
        })

    if tabela.st_condic_acompanham_nutricion == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoEvolucaoCondicoesAvaliadas": {
                "atendimento": {
                    "id": tabela.id_gerado
                },
                "classificacaoEvolucaoCondicaoAvaliada": {
                    "id": 5055
                },
                "checked": "true"
            }
        })
    if tabela.st_condic_uso_sonda_nasogastri == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoEvolucaoCondicoesAvaliadas": {
                "atendimento": {
                    "id": tabela.id_gerado
                },
                "classificacaoEvolucaoCondicaoAvaliada": {
                    "id": 5075
                },
                "checked": "true"
            }
        })
    if tabela.st_condic_uso_sonda_nasoentera == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoEvolucaoCondicoesAvaliadas": {
                "atendimento": {
                    "id": tabela.id_gerado
                },
                "classificacaoEvolucaoCondicaoAvaliada": {
                    "id": 5074
                },
                "checked": "true"
            }
        })
    if tabela.st_condic_uso_gastrostomia == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoEvolucaoCondicoesAvaliadas": {
                "atendimento": {
                    "id": tabela.id_gerado
                },
                "classificacaoEvolucaoCondicaoAvaliada": {
                    "id": 5073
                },
                "checked": "true"
            }
        })
    if tabela.st_condic_uso_colostomia == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoEvolucaoCondicoesAvaliadas": {
                "atendimento": {
                    "id": tabela.id_gerado
                },
                "classificacaoEvolucaoCondicaoAvaliada": {
                    "id": 5072
                },
                "checked": "true"
            }
        })
    if tabela.st_condic_uso_cistostomia == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoEvolucaoCondicoesAvaliadas": {
                "atendimento": {
                    "id": tabela.id_gerado
                },
                "classificacaoEvolucaoCondicaoAvaliada": {
                    "id": 5071
                },
                "checked": "true"
            }
        })
    if tabela.st_condic_uso_sond_vesic_demor == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoEvolucaoCondicoesAvaliadas": {
                "atendimento": {
                    "id": tabela.id_gerado
                },
                "classificacaoEvolucaoCondicaoAvaliada": {
                    "id": 5076
                },
                "checked": "true"
            }
        })
    if tabela.st_condic_acomp_pre_operatorio == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoEvolucaoCondicoesAvaliadas": {
                "atendimento": {
                    "id": tabela.id_gerado
                },
                "classificacaoEvolucaoCondicaoAvaliada": {
                    "id": 5056
                },
                "checked": "true"
            }
        })
    if tabela.st_condic_acomp_pos_operatorio == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoEvolucaoCondicoesAvaliadas": {
                "atendimento": {
                    "id": tabela.id_gerado
                },
                "classificacaoEvolucaoCondicaoAvaliada": {
                    "id": 5057
                },
                "checked": "true"
            }
        })
    if tabela.st_condic_adapt_uso_ortes_prot == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoEvolucaoCondicoesAvaliadas": {
                "atendimento": {
                    "id": tabela.id_gerado
                },
                "classificacaoEvolucaoCondicaoAvaliada": {
                    "id": 5058
                },
                "checked": "true"
            }
        })
    if tabela.st_condic_reabilita_domiciliar == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoEvolucaoCondicoesAvaliadas": {
                "atendimento": {
                    "id": tabela.id_gerado
                },
                "classificacaoEvolucaoCondicaoAvaliada": {
                    "id": 5066
                },
                "checked": "true"
            }
        })
    if tabela.st_condic_cuidd_paliat_oncolog == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoEvolucaoCondicoesAvaliadas": {
                "atendimento": {
                    "id": tabela.id_gerado
                },
                "classificacaoEvolucaoCondicaoAvaliada": {
                    "id": 5059
                },
                "checked": "true"
            }
        })
    if tabela.st_condic_cuidd_paliat_n_oncol == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoEvolucaoCondicoesAvaliadas": {
                "atendimento": {
                    "id": tabela.id_gerado
                },
                "classificacaoEvolucaoCondicaoAvaliada": {
                    "id": 5060
                },
                "checked": "true"
            }
        })
    if tabela.st_condic_uso_traqueostomia == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoEvolucaoCondicoesAvaliadas": {
                "atendimento": {
                    "id": tabela.id_gerado
                },
                "classificacaoEvolucaoCondicaoAvaliada": {
                    "id": 5077
                },
                "checked": "true"
            }
        })
    if tabela.st_condic_uso_aspir_via_aerea == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoEvolucaoCondicoesAvaliadas": {
                "atendimento": {
                    "id": tabela.id_gerado
                },
                "classificacaoEvolucaoCondicaoAvaliada": {
                    "id": 5070
                },
                "checked": "true"
            }
        })
    if tabela.st_condic_suport_ventil_cpap == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoEvolucaoCondicoesAvaliadas": {
                "atendimento": {
                    "id": tabela.id_gerado
                },
                "classificacaoEvolucaoCondicaoAvaliada": {
                    "id": 5068
                },
                "checked": "true"
            }
        })
    if tabela.st_condic_suport_ventil_bipap == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoEvolucaoCondicoesAvaliadas": {
                "atendimento": {
                    "id": tabela.id_gerado
                },
                "classificacaoEvolucaoCondicaoAvaliada": {
                    "id": 5067
                },
                "checked": "true"
            }
        })
    if tabela.st_condic_dialise_peritonial == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoEvolucaoCondicoesAvaliadas": {
                "atendimento": {
                    "id": tabela.id_gerado
                },
                "classificacaoEvolucaoCondicaoAvaliada": {
                    "id": 5062
                },
                "checked": "true"
            }
        })
    if tabela.st_condic_paracentese == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoEvolucaoCondicoesAvaliadas": {
                "atendimento": {
                    "id": tabela.id_gerado
                },
                "classificacaoEvolucaoCondicaoAvaliada": {
                    "id": 5065
                },
                "checked": "true"
            }
        })
    if tabela.st_condic_medicacao_parenteral == 1:
        data.append({
            "idIntegracao": f"{tabela.id_gerado}",
            "atendimentoEvolucaoCondicoesAvaliadas": {
                "atendimento": {
                    "id": tabela.id_gerado
                },
                "classificacaoEvolucaoCondicaoAvaliada": {
                    "id": 5063
                },
                "checked": "true"
            }
        })
    print(len(data))
    if len(data) >= 1 and batch_size <= 25:
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
##            stmt = text(f"UPDATE cnv_ajustaatendimentos SET protocolo = '{f_token_retorno}' WHERE  id_cloud = {tabela.id_gerado}")
            stmt = text(f"UPDATE cnv_atendimento SET protocoloavaliacao = '{f_token_retorno}' WHERE  id_gerado = {tabela.id_gerado}")
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
        ##stmt = text(f"UPDATE cnv_ajustaatendimentos SET protocolo = '{f_token_retorno}' WHERE  id_cloud = {tabela.id_gerado}")
        stmt = text(f"UPDATE cnv_atendimento SET protocoloavaliacao = '{f_token_retorno}' WHERE  id_gerado = {tabela.id_gerado}")
        session.execute(stmt)
        session.commit()
    else:
        print(f'erro execução')