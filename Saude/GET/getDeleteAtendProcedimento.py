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

'''Utilizando select direto'''

query = text("select protocolo from cnv_delatenddomicprocedimentos where protocolo is not null group by protocolo")

resultados = session.execute(query).fetchall()
for tabela in resultados:
    f_token_retorno = tabela.protocolo
    print(f'Verificando protocolo: {f_token_retorno}')

    status_lote = 'INICIANDO'
    while status_lote not in ["EXECUTADO", "EXECUTADO_PARCIALMENTE", "ERRO"]:
        try:
            response = requests.get(
                f'{urlPost}{f_token_retorno}',
                headers={'Authorization': f'Bearer {tokenEntidade}'}
            )
            response.raise_for_status()  # Lança um HTTPError para respostas de erro (4xx ou 5xx)

            data = response.json()
            status_lote = data.get('statusLote')
            retornos = data.get('retorno', [])

            print(f"Status do Lote: {status_lote}")

            if status_lote in ["EXECUTADO", "EXECUTADO_PARCIALMENTE", "ERRO"]:
                # Listas para guardar os itens para atualização em lote
                atualizacoes_sucesso = []
                atualizacoes_erro = []

                for item in retornos:
                    id_integracao = item.get('idIntegracao')
                    status = item.get('status')

                    if status == "SUCESSO":
                        id_gerado = item.get('idGerado')
                        atualizacoes_sucesso.append((id_gerado, id_integracao))
                    elif status == "ERRO":
                        mensaErro = item.get('mensagem')
                        atualizacoes_erro.append((mensaErro, id_integracao))

                # Realiza uma única atualização em lote para itens de SUCESSO
                if atualizacoes_sucesso:
                    print(f"Executando atualização em lote para {len(atualizacoes_sucesso)} registros de sucesso.")

                    # Cria a string de valores para a tabela temporária
                    valores_str = ", ".join(
                        [f"({id_gerado}, {id_integracao})" for id_gerado, id_integracao in atualizacoes_sucesso])

                    # Constrói o comando UPDATE
                    stmt_sucesso = text(f"""
                        UPDATE cnv_delatenddomicprocedimentos
                        SET id_delete = u.id_gerado::int8
                        FROM (VALUES {valores_str}) AS u(id_gerado, id_integracao)
                        WHERE cnv_delatenddomicprocedimentos.idgerado = u.id_integracao::int8 and cnv_delatenddomicprocedimentos.idprocedimento = u.id_gerado::int8
                    """)
                    ##print(stmt_sucesso)
                    session.execute(stmt_sucesso)

                # Realiza uma única atualização em lote para itens de ERRO
                if atualizacoes_erro:
                    print(f"Executando atualização em lote para {len(atualizacoes_erro)} registros de erro.")

                    # Certifica-se de que as aspas simples na mensagem de erro não quebram a query
                    valores_str = ", ".join(
                        [f"('{mensaErro.replace("'", "''")}', '{id_integracao}')" for mensaErro, id_integracao in
                         atualizacoes_erro])

                    # Constrói o comando UPDATE
                    stmt_erro = text(f"""
                        UPDATE cnv_delatenddomicprocedimentos
                        SET menssagemerrodelete = u.mensaErro
                        FROM (VALUES {valores_str}) AS u(mensaErro, id_integracao)
                        WHERE cnv_delatenddomicprocedimentos.idgerado = u.id_integracao::int8 
                    """)
                    session.execute(stmt_erro)

                # Confirma todas as atualizações de uma só vez para este lote
                session.commit()
                print("Todas as atualizações para este lote foram confirmadas com sucesso.")

        except requests.exceptions.RequestException as e:
            print(f"Erro durante a requisição à API: {e}")
            break
        except json.JSONDecodeError:
            print("Erro: A resposta não é um JSON válido.")
            break
        except KeyError as e:
            print(f"Erro: Chave ausente no JSON - {e}")
            break

# Fecha a sessão depois que todos os protocolos forem processados
session.close()

print("Processo finalizado.")