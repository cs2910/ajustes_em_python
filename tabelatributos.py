from sqlalchemy import Column, Integer, String, DateTime, Text, ForeignKey,JSON, TEXT, Double
from sqlalchemy.orm import declarative_base
from conexao import engine

Base = declarative_base()

class Baixapatri(Base):
    __tablename__ = 'baixapatri'
    id = Column(Integer(), primary_key=True)
    idbaixa = Column(Integer)
    databaixa = Column(DateTime)
    placa = Column(Integer)
    protocolo = Column(String(100))
    idcloud = Column(Integer)

class Benparcial(Base):
    __tablename__ = 'benparcial'
    id = Column(Integer(), primary_key=True)
    codtipo = Column(Integer)
    descricao =	Column(String(100))
    percdesconto = Column(Double)
    desconto = Column(Integer)
    idusuarios = Column(String(50))
    idsistema = Column(String(50))
    dataalteracao = Column(DateTime)
    i_isencoes = Column(Integer)
    coddesconto = Column(Integer)
    tipocadastro = Column(Integer)
    codcadastro = Column(Integer)
    numprocesso = Column(String(50))
    dtcadprocesso = Column(DateTime)
    dtinicial = Column(DateTime)
    dtfinal = Column(DateTime)
    codtipodesc = Column(Integer)
    codimpdesc = Column(Integer)
    status = Column(String(50))
    dtcaddesc = Column(DateTime)
    dtcancdesc = Column(DateTime)
    codmotcanc = Column(String(50))
    idusuario = Column(String(50))
    idsistaudit = Column(String(50))
    dtalteracao = Column(DateTime)
    idimovel = Column(Integer)
    idbeneficio = Column(Integer)
    idresp = Column(Integer)
    protocolo = Column(String(100))
    idcloud   = Column(Integer)
    idcloudref = Column(Integer)
    protrocref = Column(String(100))
    idcloudcred = Column(Integer)
    protroccred = Column(String(100))
    idcloudcredrec = Column(Integer)
    protroccredrec = Column(String(100))


class Idimoresp(Base):
    __tablename__ = 'imovelresp'
    id = Column(Integer(), primary_key=True)
    idimovel = Column(Integer)
    codimovel = Column(Integer)
    idresp    = Column(Integer)

class Protbeneficio(Base):
    __tablename__ = 'protbeneficio'
    id = Column(Integer(), primary_key=True)
    protocolo = Column(String)

class Pensend(Base):
    __tablename__ = 'pensend'
    id = Column(String(), primary_key=True)
    idcontrub = Column(String(20))
    idlogra = Column(String(20))
    abrev = Column(String(5))
    nomelogra = Column(String(150))
    idbairro = Column(String(20))
    nmbairro = Column(String(150))
    ceps = Column(String(20))
    nmmunic = Column(String(50))
    novocep = Column(String(10))



def init_db():
    Base.metadata.create_all(bind=engine)

if __name__ == '__main__':
    init_db()