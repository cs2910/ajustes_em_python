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
    database="migracao",
    user="postgres",
    password="Cp@142536"
)
cursor_postgres = conexao_postgres.cursor()
tabela = 'imomigra'

# Conexão com o banco de dados Sybase
strConn = f'Driver=Adaptive Server Anywhere 9.0;ENG={server};UID={username};PWD={password};DBN={database};LINKS=TCPIP(HOST={ip}:{port});'
conexao = pyodbc.connect(strConn)
cursor_sybase = conexao.cursor()

# Seleciona os dados da tabela no PostgreSQL

cursor_postgres.execute(f"select imovel_id, imovel_id_conversao, imovel_matricula from tributario.imovel")



dados = cursor_postgres.fetchall()
cursor_sybase.execute(f"CREATE TABLE cnv_{tabela} ("
                      f"imo_id integer, "
                      f"imoconv_id integer,"
                      f"imovel_matricula integer)")

# Preparar a query de inserção no Sybase
sql_insert = f"INSERT INTO cnv_{tabela}(imo_id,imoconv_id,imovel_matricula) VALUES (?, ?, ?)"

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
