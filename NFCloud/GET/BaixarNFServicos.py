from sqlalchemy.orm import sessionmaker
from sqlalchemy import extract, update,insert, text
from tabelatributos import Pensend
from conexao import engine, connection
import requests, json
import re
import codecs

#################################################
#           Conexão com banc de dados           #
#################################################
Session = sessionmaker(bind=engine)
session = Session()

#### Linka
urlPost = 'https://nota-eletronica.betha.cloud/service-layer/api/notas-fiscais-servicos/'
#### Token da Endidade
tokenEntidade = '7b07d502-6a5d-4cf1-bb73-73824153d3be'

# Lista para armazenar todos os dados
all_cbo_data = []
batch_size = 3
processed_ids = set()
# Parâmetros de paginação
offset = 12509794
limit = 1000 ##500  # Limite de registros por requisição
has_next = True

# Loop para buscar todos os dados
while has_next:
    print(f"Buscando dados com offset: {offset} e LImit {limit}...")
    params = {'iniciaEm': offset, 'nRegistros': limit}
    response = requests.get(
        urlPost,
        headers={'Authorization': f'Bearer {tokenEntidade}'},
        params=params
    )

    if response.status_code == 200:
        try:
            raw_text = response.text.strip()

            if not raw_text:
                content_list = []
            else:
                # ETAPA 1: Corrigir as quebras de linha (ainda necessário)
                fixed_text = raw_text.replace('\n', '\\n').replace('\r', '')

                # ETAPA 2 (NOVA E SIMPLIFICADA): Remover completamente o campo "discriminacao".
                # Usamos o campo seguinte, "vlServico", como referência para saber onde parar.
                # A expressão r',("discriminacao":.*?,"vlServico")' encontra o trecho problemático.
                # Nós o substituímos por r',"vlServico"', efetivamente cortando o campo "discriminacao".
                # O [,] no início garante que removemos a vírgula anterior corretamente.
                fixed_text = re.sub(r',"discriminacao":.*?,"vlServico":', ',"vlServico":', fixed_text)

                # ETAPA 3: Garantir que a string inteira seja uma lista
                if not fixed_text.startswith('['):
                    fixed_text = f'[{fixed_text}]'

                # Agora o json.loads deve funcionar, pois o campo problemático foi removido.
                data = json.loads(fixed_text)
                content_list = data

        except json.JSONDecodeError as e:
            print(f"Erro ao decodificar JSON mesmo após remover o campo 'discriminacao': {e}")
            # Se ainda falhar, salvamos o arquivo para análise
            with open("problematic_response.json", "w", encoding="utf-8") as f:
                f.write(fixed_text)  # Salva o texto *após* a tentativa de correção
            print("A resposta pré-processada foi salva em 'problematic_response.json' para análise.")
            continue
        content_list = data
        ##print(data)
        if content_list:
            # Lista para acumular os valores a serem inseridos
            valores_para_inserir = []
            for item in content_list:
                idGerado = item['id']
                idNota = item['idNotaFiscal']
                servico = item['idListasServicosEntidades']
                aliquota = item['aliquota']
                vlServico = item['vlServico']
                quantidade = item['quantidade']
                vlTotalServico = item['vlTotalServico']
                vlBaseCalculo = item['vlBaseCalculo']
                vlIss = item['vlIss']

                # Verifica se o ID já foi processado
                if idGerado not in processed_ids:
                    # Formata o par de valores e adiciona à lista
                    # As aspas simples são essenciais para valores de texto
                    valores_para_inserir.append(f"('{idGerado}', '{idNota}', '{servico}','{aliquota}','{vlServico}','{quantidade}','{vlTotalServico}','{vlBaseCalculo}','{vlIss}')")
                    # Adiciona ao conjunto para evitar duplicatas
                    processed_ids.add(idGerado)
                else:
                    print(f"Registro com idGerado {idGerado} já existe. Pulando.")

            # Se a lista de valores não estiver vazia, execute a inserção em massa
            if valores_para_inserir:
                print(f"Inserindo lote de {len(valores_para_inserir)} registros...")

                # Junta todos os pares de valores em uma única string
                valores_em_sql = ", ".join(valores_para_inserir)

                # Constrói o comando INSERT em lote
                stmt = text(f"""
                            INSERT INTO bthsc204386 (idGerado, idNota,servico,aliquota,vlServico,quantidade,vlTotalServico, vlBaseCalculo, vlIss )
                            VALUES {valores_em_sql}
                        """)

                # Executa a instrução e faz o commit
                session.execute(stmt)
                session.commit()
                print("Lote inserido com sucesso!")

        ##has_next = data.get('hasNext', False)
        ##if has_next:
        offset += limit
    else:
        print(f"Erro na requisição: {response.status_code}")
        break

    print("-" * 50)
    print("Listagem e inserção do lote atual concluídas.")
    print("-" * 50)

# Fecha a sessão no final
session.close()

print("Processo de importação finalizado.")