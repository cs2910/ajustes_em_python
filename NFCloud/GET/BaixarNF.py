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
urlPost = 'https://nota-eletronica.betha.cloud/service-layer/api/notas-fiscais/'
#### Token da Endidade
tokenEntidade = '7b07d502-6a5d-4cf1-bb73-73824153d3be'

## Select Atendimentos
##query = text("select idnota from bthsc204386 group by idnota")


# Lista para armazenar todos os dados
all_cbo_data = []
batch_size = 3
processed_ids = set()
# Parâmetros de paginação
offset = 9873562
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
            # 1. Decodificar a resposta para uma string
            raw_text = response.content.decode('utf-8')

            # 2. "Limpar" a string, substituindo o caractere de tabulação por um espaço
            cleaned_text = raw_text.replace('\t', ' ')

            # 3. Tentar decodificar a string já limpa
            data = json.loads(cleaned_text)
            content_list = data

        except json.JSONDecodeError as e:
            # Se ainda der erro, o problema pode ser outro. Salve para análise.
            print(f"Erro ao decodificar JSON: {e}")
            with open('problematic_response.json', 'w', encoding='utf-8') as f:
                f.write(response.content.decode('utf-8', errors='ignore'))
            print("A resposta problemática foi salva em 'problematic_response.json' para análise.")
            break
        content_list = data
        if content_list:
            # Lista para acumular os valores a serem inseridos
            valores_para_inserir = []
            for item in content_list:
                idGerado        = item['id']
                idPessoas       = item['idPessoa']
                nroNota         = item['nroNota']
                dhFatoGerador   = item['dhFatoGerador']
                dhEmissao       = item['dhEmissao']
                situacao        = item['situacao']
                optanteSimples  = item['optanteSimples']
                vlTotalServicos = item['vlTotalServicos']
                vlTotalLiquido  = item['vlTotalLiquido']
                vlTotalBaseCalculo = item['vlTotalBaseCalculo']
                vlTotalIss      = item['vlTotalIss']

                # Verifica se o ID já foi processado
                if idGerado not in processed_ids:
                    # Formata o par de valores e adiciona à lista
                    # As aspas simples são essenciais para valores de texto
                    valores_para_inserir.append(f"('{idGerado}', '{idPessoas}', '{nroNota}','{dhFatoGerador}','{dhEmissao}','{situacao}','{optanteSimples}','{vlTotalServicos}','{vlTotalLiquido}','{vlTotalBaseCalculo}','{vlTotalIss}')")
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
                            INSERT INTO bthsc204386nf (idgerado,idpessoas,nronota,dhfatogerador,dhemissao,situacao,optantesimples,vltotalservicos,vltotalliquido,vlbasecalculo,vltotalIss)
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