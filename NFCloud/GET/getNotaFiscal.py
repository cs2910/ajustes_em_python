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
urlPost = 'https://saude.betha.cloud/api-saude-service-layer/v2/api/consultalote/'
#### Token da Endidade

tokenEntidade = '38096b6b-d3b5-4895-8c0b-92d355c7dee3'


## Select Atendimentos
query = text("select idnota from bthsc204386 group by idnota")

##query = text("SELECT * FROM ajustedtisencao where protocolo is null")
resultados = session.execute(query).fetchall()
for tabela in resultados:
    print(f'{tabela.idnota}')
    f_token_retorno = tabela.idnota
    response = requests.get(f'https://nota-eletronica.betha.cloud/service-layer/api/notas-fiscais/{f_token_retorno}',
                           headers={'Authorization': f'Bearer {tokenEntidade}'})
    status = response.status_code
    print(response.content)
    print(status)
    if status == 200:
        recurrence = json.loads(response.content)
        f_token_retorno = recurrence['idLote']
        print(f'protocolo {f_token_retorno}')
        try:
           ## json_string = response_data.decode('utf-8')
            data = recurrence
            status_lote = data.get('statusLote')
            retornos = data.get('retorno', [])
            print(f"Status do Lote: {status_lote}")
            print("--- Detalhes do Retorno ---")
            if status_lote == "EXECUTADO" or  status_lote == "EXECUTADO_PARCIALMENTE":
                for item in retornos:
                    id_integracao = item.get('idIntegracao')
                    id_gerado = item.get('idGerado')
                    mensaErro = item.get('mensagem')
                    if mensaErro is None:
                        mensaErro = 'Null'
                    print(mensaErro)
                    status = item.get('status')
                    print(f"ID Integração: {id_integracao}, ID Gerado: {id_gerado}, Status: {status}")
                    if status == "SUCESSO":
                        ##stmt = text(f"insert into cnv_idsavaliativas(sequnc,id_gerado,mensagemerro, tipo) values({id_integracao},{id_gerado},{mensaErro},'AT')")
                        stmt = text(f"insert into cnv_idsavaliativas(sequnc,id_gerado,mensagemerro, tipo) values({id_integracao},{id_gerado},{mensaErro},'ATN')")
                        ##print(stmt)    ##f"UPDATE cnv_id_atendom SET id_gerado = {id_gerado}, mensagemerro = null WHERE  sequenc = {id_integracao}")
                        session.execute(stmt)
                        session.commit()
                        '''response = requests.get(
                            f'https://saude.betha.cloud/api-saude-service-layer/v2/api/atendimentoDomiciliarCondicaoAvaliadas/{id_gerado}',
                            headers={'Authorization': f'Bearer {tokenEntidade}'})
                        status = response.status_code
                        print(response.content)
                        print(status)
                        if status == 200:
                            recurrenceId = json.loads(response.content)
                            antendid = recurrenceId['atendimentoDomiciliar']['id']
                            classId = recurrenceId['classificacaoCondicaoAvaliada']['id']
                            idGerado = recurrenceId['idGerado']
                            print(f'antendid {antendid} - classId {classId}')
                            stmt = text(
                                f"UPDATE cnv_idsavaliativas SET id_atend = {antendid}, class = {classId} WHERE  id_gerado = {idGerado}")
                            session.execute(stmt)
                            session.commit()'''
                    if status == "ERRO":
                        stmt = text(f"insert into cnv_idsavaliativas(sequnc,id_gerado,mensagemerro, tipo) values({id_integracao},{id_gerado},{mensaErro},'ATN')")
                        ##stmt = text(f"insert into cnv_idsavaliativas(sequnc,id_gerado,mensagemerro, tipo) values({id_integracao},{id_gerado},{mensaErro},'AT')")
                        ## f"UPDATE cnv_id_atendom SET mensagemerro = '{mensaErro}' WHERE  sequenc = {id_integracao}")
                        session.execute(stmt)
                        session.commit()
        except json.JSONDecodeError:
            print("Erro: A resposta não é um JSON válido.")
        except KeyError as e:
            print(f"Erro: Chave ausente no JSON - {e}")
    else:
        print(f'erro execução')
