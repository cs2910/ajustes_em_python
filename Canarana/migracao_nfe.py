import psycopg2
import pyodbc
ip = '192.168.15.8'
port = '9015'
database = 'Tributos'
server = 'final'
username = 'tecbth_stafba'
password = 'Xye8bTwgqi7bMo6zXhcyWwqYmyJsKn'

# Conexão com o banco de dados PostgreSQL


conexao_postgres = psycopg2.connect(
    host="localhost",
    port="5432",
    database="canarana22",
    user="postgres",
    password="Cp@142536"
)
cursor_postgres = conexao_postgres.cursor()
tabela = 'nfseletro'

# Conexão com o banco de dados Sybase
strConn = f'Driver=Adaptive Server Anywhere 9.0;ENG={server};UID={username};PWD={password};DBN={database};LINKS=TCPIP(HOST={ip}:{port});'
conexao = pyodbc.connect(strConn)
cursor_sybase = conexao.cursor()

# Seleciona os dados da tabela no PostgreSQL


cursor_postgres.execute(f"select *  from tributario.nfse where nfse_avulsa = 2 ")



dados = cursor_postgres.fetchall()

# Obter nomes das colunas
colunas = [desc[0] for desc in cursor_postgres.description]

# Criar uma tabela semelhante no Sybase (ajuste conforme necessário)
print(colunas)


cursor_postgres.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'nfse' AND table_schema = 'tributario'")
colunas = cursor_postgres.fetchall()
print(colunas)
tipo_mapeamento = {
    'character varying': 'text',
    'varchar' : 'text',
    'integer': 'INT',
    'numeric': 'DECIMAL',
    'boolean': 'varchar',
    'date': 'DATE',
    'timestamp without time zone': 'DATETIME',
    'jsonb': 'TEXT' # Ajuste conforme necessário para tipos complexos
    }

##cursor_sybase.execute(f"DROP TABLE cnv_{tabela}")


colunas_sybase = ", ".join([f"{coluna} {tipo_mapeamento.get(tipo, 'VARCHAR(255)')}" for coluna, tipo in colunas])
cursor_sybase.execute(f"CREATE TABLE cnvf_{tabela} ({colunas_sybase})")

# Preparar a query de inserção no Sybase
valores_placeholder = ", ".join(["?" for _ in colunas])
sql_insert = f"INSERT INTO cnvf_{tabela} VALUES ({valores_placeholder})"

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
