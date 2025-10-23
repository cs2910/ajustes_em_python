from sqlalchemy import Column, Integer, String, DateTime, Text, ForeignKey,JSON, TEXT, DOUBLE
from sqlalchemy.orm import declarative_base
from conexao import engine


Base = declarative_base()

class Article(Base):
    __tablename__ = 'testar'
    id = Column(Integer(), primary_key=True)
    nome = Column(String(100), nullable=False, unique=True)

class Cargo(Base):
    __tablename__ = 'cargo'
    id = Column(Integer(), primary_key=True)
    cmjason = Column(JSON)
    protocolo = Column(String(100))
    idcloud = Column(Integer)

class Tcargo(Base):
    __tablename__ = 'tcargo'
    id = Column(Integer(), primary_key=True)
    cbo = Column(Integer)
    protocolo = Column(String(100))
    idcloud   = Column(Integer)
    retorno   = Column(JSON)

class Tnova(Base):
    __tablename__ = 'claudio'
    id = Column(Integer(), primary_key=True)
    cbo = Column(Integer)
    protocolo = Column(String(100))
    idcloud   = Column(Integer)


class Formulas(Base):
    __tablename__ = 'formulas'
    id = Column(Integer(), primary_key=True)
    formula = Column(Text)
    protocolo = Column(String(100))
    idcloud   = Column(Integer)

class Horario(Base):
    __tablename__ = 'horaraio'
    id = Column(Integer(), primary_key=True)
    cmjason = Column(JSON)
    protocolo = Column(String(100))
    idcloud   = Column(Integer)
    ajuste    = Column(Text)


class Matricula(Base):
    __tablename__ = 'matricula'
    id = Column(Integer(), primary_key=True)
    cmjason = Column(JSON)
    protocolo = Column(String(100))
    idcloud   = Column(Integer)
    ajuste    = Column(Text)

class Histmatr(Base):
    __tablename__ = 'histmatr'
    id = Column(Integer(), primary_key=True)
    matricula = Column(Integer)
    protocolo = Column(String(100))
    idcloud   = Column(Integer)
    ajuste    = Column(Text)
    campo     = Column(String(200))

class Hmatricula(Base):
    __tablename__ = 'hmatricula'
    id = Column(Integer(), primary_key=True)
    matricula = Column(Integer)
    protocolo = Column(String(100))
    idcloud   = Column(Integer)
    ajuste    = Column(Text)
    campo     = Column(String(200))

class Ajustaadd(Base):
    __tablename__ = 'ajustaadd'
    id = Column(Integer(), primary_key=True)
    matricula = Column(Integer)
    novo      = Column(Integer)
    protocolo = Column(String(100))
    idcloud   = Column(Integer)

class Siope(Base):
    __tablename__ = 'sipoe'
    id = Column(Integer(), primary_key=True)
    matricula = Column(Integer)
    profms  =  Column(String(1))
    trabped =  Column(String(1))
    trabtecn =  Column(String(1))
    trabnoto =  Column(String(1))
    profgradu =  Column(String(1))
    prestpsic =  Column(String(1))
    prestsocial =  Column(String(1))

class Ocorrencia(Base):
    __tablename__ = 'ocorrsefip'
    id = Column(Integer(), primary_key=True)
    matricula = Column(Integer)
    ocorrencia = Column(String(1))
    matr = Column(String(10))

class Provimento(Base):
    __tablename__ = 'provim'
    id = Column(Integer(), primary_key=True)
    matricula = Column(Integer)
    ocorrencia = Column(String(1))
    matr = Column(String(10))

class Procedimento(Base):
    __tablename__ = 'proced'
    id = Column(Integer(), primary_key=True)
    matricula = Column(Integer)
    idcloud = Column(Integer)
    competencia = Column(String(7))
    situacao = Column(String(50))

class Receitaproce(Base):
    __tablename__ = 'receitaproced'
    id = Column(Integer(), primary_key=True)
    matricula = Column(Integer)
    idcloud = Column(Integer)

class Baixapatri(Base):
    __tablename__ = 'baixapatri'
    id = Column(Integer(), primary_key=True)
    idbaixa = Column(Integer)
    databaixa = Column(DateTime)
    placa = Column(Integer)
    protocolo = Column(String(100))
    idcloud = Column(Integer)

class Situacao(Base):
    __tablename__ = 'situacao'
    id = Column(Integer(), primary_key=True)
    func = Column(Integer)
    matric = Column(Integer)
    sit = Column(Integer)
    tipo = Column(String(20))

class Dependente(Base):
    __tablename__ = 'dependentes'
    id = Column(Integer(), primary_key=True)
    matric = Column(Integer)
    nome = Column(String(120))

class Conta(Base):
    __tablename__ = 'conta'
    id = Column(Integer(), primary_key=True)
    idpessoa = Column(Integer)
    idconta  = Column(Integer)
    numconta = Column(String(50))
    protocolo = Column(String(50))


class Histpess(Base):
    __tablename__ = 'histpess'
    id = Column(Integer(), primary_key=True)
    origem = Column(Integer)
    ids = Column(Integer)
    nome = Column(String(150))
    cpf = Column(String(14))
    datanascimento = Column(DateTime)
    sexo = Column(String(50))
    estadocivil = Column(String(50))
    nacionalidade = Column(String(50))
    numeropispasep = Column(String(50))
    grauescolaridade = Column(String(50))
    protocolo = Column(String(50))

class Eventos(Base):
    __tablename__ = 'eventos'
    id = Column(Integer(), primary_key=True)
    codigo = Column(Integer)
    descr = Column(String(150))

class Pessoashist(Base):
    __tablename__ = 'pessoashist'
    id = Column(Integer(), primary_key=True)
    pes = Column(Integer)

class Histpessoas(Base):
    __tablename__ = 'histpessoas'
    id = Column(Integer(), primary_key=True)
    dtalter = Column(String(10))
    origem =  Column(String(50))
    ids =  Column(String(50))
    nome =  Column(String(250))
    cpf =  Column(String(50))
    dtnasc =  Column(String(50))
    sexo =  Column(String(50))
    estadocivil =  Column(String(50))
    nacionalidade =  Column(String(50))
    pis =  Column(String(50))
    raca =  Column(String(50))
    naturalidade =  Column(String(50))
    rg =  Column(String(50))
    orgemissor =  Column(String(50))
    uf =  Column(String(50))
    titulo =  Column(String(50))
    zona =  Column(String(50))
    secao =  Column(String(50))
    ctps =  Column(String(50))
    seriectps =  Column(String(50))
    ufctps =  Column(String(50))
    emissaoctps =  Column(String(50))
    grauinstrucao =  Column(String(50))
    sitgrauinstrucao =  Column(String(50))
    protocolo = Column(String(50))
    executar = Column(String(1))

class Idmatriz(Base):
    __tablename__ = 'idmatriz'
    id = Column(Integer(), primary_key=True)
    nome = Column(String(150))
    descricao = Column(String(150))

class Idmatrizpiata(Base):
    __tablename__ = 'idmatrizpiata'
    id = Column(Integer(), primary_key=True)
    nome = Column(String(150))
    descricao = Column(String(150))

class Paquisicao(Base):
    __tablename__ = 'paquisicao'
    idprincipal = Column(Integer)
    datacadastro = Column(DateTime)
    id = Column(Integer(), primary_key=True)
    dataini = Column(DateTime)
    datafin = Column(DateTime)
    ordem = Column(Integer)
    novaini = Column(DateTime)
    novafin = Column(DateTime)
    protocolo = Column(String(50))

class Campoadd(Base):
    __tablename__ = 'campoadd'
    id = Column(Integer(), primary_key=True)
    matric = Column(String(15))
    protocolo = Column(String(50))


class Bateponto(Base):
    __tablename__ = 'bateponto'
    id = Column(Integer(), primary_key=True)
    matric = Column(String(15))

class Eventoadd(Base):
    __tablename__ = 'eventoadd'
    id = Column(Integer(), primary_key=True)
    codigo = Column(String(15))
    protocolo = Column(String(50))

class Matrievento(Base):
    __tablename__ = 'matrievento'
    id = Column(Integer(), primary_key=True)
    nome = Column(String(150))
    protocolo = Column(String(50))



def init_db():
    Base.metadata.create_all(bind=engine)

if __name__ == '__main__':
    init_db()