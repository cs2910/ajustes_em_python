from sqlalchemy.orm import sessionmaker
from sqlalchemy import extract, update, text
from tabelatributos import Benparcial, Idimoresp
from conexao import engine, connection
import requests, json

#################################################
#           Conexão com banc de dados           #
#################################################
Session = sessionmaker(bind=engine)
session = Session()

#### Linka
urlPost = 'https://tributos.betha.cloud/service-layer-tributos/api/manutCalcCreditosRec/'
#### Token da Endidade

tokenEntidade = '8b19d51d-684f-4b7f-889a-6e8fcd7ce3fe'

query = text("select n.id,perc_aplicado,id_creditos_tributarios_rec, ano_vigencia, n.id_manutencoes_calculos"
             ",id_manut_calc_referentes from bthsc245892novo n "
             "join bthsc245892manut m on "
             "m.id = n.id_manutencoes_calculos "
             "where situacao in ('D','A') and n.protocolo is null")
resultados = session.execute(query).fetchall()
batch_size = 50
data = []
for tabela in resultados:
##for tabela, imovel in tabelas:
    idTabela = tabela.id
    percent = tabela.perc_aplicado
    tribu = tabela.id_creditos_tributarios_rec
    ano = tabela.ano_vigencia
    print(tribu)
    data.append({
        "idIntegracao": f'{idTabela}',
        "manutCalcCreditosRec": {
            "idCreditosTributariosRec": tribu,
            "idManutencoesCalculos": tabela.id_manutencoes_calculos,
            "idManutCalcReferentes": tabela.id_manut_calc_referentes,
            "selecionada": "SIM",
            "deferida": "SIM",
            "classificacaoRevisaoCalculo": "RETIFICACAO",
            "valorLancado": 0,
            "valorCorrecao": 0,
            "valorJuros": 0,
            "valorMulta": 0,
            "valorBeneficioLancado": 0,
            "valorBeneficioCorrecao": 0,
            "valorBeneficioJuros": 0,
            "valorBeneficioMulta": 0,
            "valorBeneficioLancadoReq": 0,
            "valorBeneficioCorrecaoReq": 0,
            "valorBeneficioJurosReq": 0,
            "valorBeneficioMultaReq": 0,
            "percLancadoReq": percent,
            "percLancado": percent,
            "percCorrecao": 0,
            "percCorrecaoReq": 0,
            "percJuros": 0,
            "percJurosReq": 0,
            "percMulta": 0,
            "percMultaReq": 0,
            "percReqAlterado": 0,
            "percAlterado": 0,
            "anosVigencia": ano
        }
    })

    if len(data) == batch_size:
        print(data)
        try:
            dados = json.dumps(data, ensure_ascii=False)  ## , indent=4 mostra o json em arvore
            print("Conversão para JSON bem-sucedida!")
            print(dados)
        except TypeError as e:
            print(f"Ocorreu um erro: {e}")
        print(dados)

        response = requests.post(urlPost,
                                 headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                                 data=json.dumps(data))
        status = response.status_code
        ##status = 200
        print(response.content)
        print(status)
        if status == 200:
            ##        print("entrou")
            recurrence = json.loads(response.content)
            f_token_retorno = recurrence['idLote']
            print(f'protocolo {f_token_retorno}')
            lista_de_pares = [
                (
                    item['idIntegracao'],
                    item['manutCalcCreditosRec']['idCreditosTributariosRec']
                )
                for item in data
            ]

            # 2. Constrói e executa um ÚNICO comando UPDATE em lote
            # Isso atualiza todos os registros de uma vez, melhorando a velocidade drasticamente
            valores_formatados = [
                f"('{sequenc}', {id_procedimento})"
                for sequenc, id_procedimento in lista_de_pares
            ]
            valores_in_sql = ", ".join(valores_formatados)

            stmt = text(f"""
                            UPDATE bthsc245892novo 
                            SET protocolo = '{f_token_retorno}' 
                            WHERE (id, id_creditos_tributarios_rec) IN ({valores_in_sql})
                        """)

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
    ##status = 200
    print(response.content)
    print(status)
    if status == 200:
        ##        print("entrou")
        ##recurrence = json.loads(response.content)
        f_token_retorno = 10  ##recurrence['idLote']
        print(f'protocolo {f_token_retorno}')
        lista_de_pares = [
            (
                item['idIntegracao'],
                item['manutCalcCreditosRec']['idCreditosTributariosRec']
            )
            for item in data
        ]

        # 2. Constrói e executa um ÚNICO comando UPDATE em lote
        # Isso atualiza todos os registros de uma vez, melhorando a velocidade drasticamente
        valores_formatados = [
            f"('{sequenc}', {id_procedimento})"
            for sequenc, id_procedimento in lista_de_pares
        ]
        valores_in_sql = ", ".join(valores_formatados)

        stmt = text(f"""
                                UPDATE bthsc245892novo 
                                SET protocolo = '{f_token_retorno}' 
                                WHERE (id, id_creditos_tributarios_rec) IN ({valores_in_sql})
                            """)

        session.execute(stmt)
        session.commit()
    else:
        print(f'erro execução')