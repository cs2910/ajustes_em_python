import psycopg2
import pyodbc
from sqlalchemy import extract, update,insert, text


ip = '192.168.15.6'
port = '9012'
database = 'betha'
server = 'socorro'
username = 'tecbth_delivery'
password = 'AELgAhFDvajZvGfybNPSPt76wbUoDm'

# Conexão com o banco de dados Sybase
strConn = f'Driver=Adaptive Server Anywhere 9.0;ENG={server};UID={username};PWD={password};DBN={database};LINKS=TCPIP(HOST={ip}:{port});'
conexao = pyodbc.connect(strConn)
cursor_sybase = conexao.cursor()

# Conexão com o banco de dados PostgreSQL

conexao_postgres = psycopg2.connect(
    host="localhost",
    port="5432",
    database="pmsocorro",
    user="postgres",
    password="Cp@142536"
)
cursor_postgres = conexao_postgres.cursor()
cursor_dados = conexao_postgres.cursor()
# Seleciona os dados da tabela no PostgreSQL
##query = text(f"SELECT table_name FROM information_schema.columns WHERE  table_schema = 'socorrpm' and table_name LIKE 'ipt%'")

cursor_postgres.execute(f"SELECT table_name FROM information_schema.tables WHERE  table_schema = 'socorrpm' "
                        f"AND table_type = 'BASE TABLE' "
                        f"and (table_name iLIKE 'ipt%' or table_name iLIKE 'itb%' or "
                        f"table_name iLIKE 'div%' or table_name iLIKE 'iss%' or "
                        f"table_name iLIKE 'trb%')  group by table_name;")
tabelas = cursor_postgres.fetchall()

##resultados = cursor_postgres.execute(query).fetchall()
for tabela in tabelas:
    # Obter nomes das colunas
    tabelaCriar = tabela[0]
    print(tabelaCriar)

    ##cursor_postgres.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{tabela}'")
    cursor_postgres.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{tabelaCriar}' AND table_schema = 'socorrpm'")

    colunas = cursor_postgres.fetchall()
    print(colunas)
    tipo_mapeamento = {
        'character varying': 'text',
        'varchar' : 'text',
        'integer': 'BIGINT',
        'numeric': 'DECIMAL',
        'boolean': 'varchar',
        'date': 'DATE',
        'timestamp without time zone': 'DATETIME',
        'jsonb': 'TEXT' # Ajuste conforme necessário para tipos complexos
        }

    ##cursor_sybase.execute(f"DROP TABLE cnv_{tabela}")
    sql_drop_if_exists = f"""
    IF EXISTS (SELECT 1 FROM sysobjects WHERE name = '{tabelaCriar}' AND type = 'U')
    BEGIN
        DROP TABLE {tabelaCriar}
    END
    """

    try:
        # 2. Executa o comando de DROP condicional
        print(f"Verificando e derrubando tabela antiga '{tabelaCriar}' se existir...")
        cursor_sybase.execute(sql_drop_if_exists)

        # 3. Cria a nova tabela
        print(f"Criando tabela '{tabelaCriar}'...")
        colunas_sybase = ", ".join(
            [f"{coluna} {tipo_mapeamento.get(tipo, 'VARCHAR(255)')}" for coluna, tipo in colunas])
        cursor_sybase.execute(f"CREATE TABLE {tabelaCriar} ({colunas_sybase})")


        # Não se esqueça de comitar, se a conexão não estiver em modo autocommit
        # conexao_sybase.commit()

        print(f"Tabela '{tabelaCriar}' criada com sucesso!")

    except Exception as e:
        print(f"Ocorreu um erro durante a operação: {e}")
        # conexao_sybase.rollback()
   ## cursor_sybase.execute(f"drop TABLE {tabelaCriar}")
    '''colunas_sybase = ", ".join([f"{coluna} {tipo_mapeamento.get(tipo, 'VARCHAR(255)')}" for coluna, tipo in colunas])
    cursor_sybase.execute(f"CREATE TABLE {tabelaCriar} ({colunas_sybase})")'''

    # Preparar a query de inserção no Sybase
    valores_placeholder = ", ".join(["?" for _ in colunas])
    sql_insert = f"INSERT INTO {tabelaCriar} VALUES ({valores_placeholder})"

    cursor_dados.execute(f"select * from socorrpm.{tabelaCriar}")
    dados = cursor_dados.fetchall()

    # Inserir os dados no Sybase
    for linha in dados:
        cursor_sybase.execute(sql_insert, linha)

    # Confirmar as mudanças no Sybase
    conexao.commit()

    # Fechar os cursores e as conexões
cursor_postgres.close()
conexao_postgres.close()
cursor_sybase.close()
conexao.close()
