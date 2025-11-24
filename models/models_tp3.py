from sqlalchemy import Column, String, DateTime, Numeric, Boolean, Date, ForeignKey, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import relationship
from constantes.constantes_tp3 import CLIENTES_CABECALHO, FUNCIONARIOS_CABECALHO, MONTAGEM_CABECALHO
import uuid



Base = declarative_base()


# ==========================
# CLIENTES
# ==========================
class Clientes_models(Base):
    __tablename__ = "clientes"
    __table_args__ = {"schema": "projeto_de_bloco_tp3"}

    # config da base genérica
    COLUNAS_PADRAO = CLIENTES_CABECALHO
    CAMPO_ID = "id_cliente"

    id_cliente = Column(
        UUID(as_uuid=False),
        primary_key=True,
        default=uuid.uuid4
    )

    nome = Column(String(35), nullable=False)
    cpf = Column(String(14), unique=True, nullable=False)
    email = Column(String(55), unique=True, nullable=False)
    telefone = Column(String(15), nullable=False)
    data_cadastro = Column(
        DateTime,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP")
    )

    # RELATIONSHIP -> cliente pode ter várias montagens
    montagens = relationship(
        "Montagem_models",
        back_populates="cliente",
        cascade="all, delete-orphan"
    )


# ==========================
# FUNCIONÁRIOS
# ==========================
class Funcionarios_models(Base):
    __tablename__ = "funcionarios"
    __table_args__ = {"schema": "projeto_de_bloco_tp3"}

    COLUNAS_PADRAO = FUNCIONARIOS_CABECALHO
    CAMPO_ID = "id_funcionario"

    id_funcionario = Column(
        UUID(as_uuid=False),
        primary_key=True,
        default=uuid.uuid4
    )

    nome = Column(String(35), nullable=False)
    cpf = Column(String(14), unique=True, nullable=False)
    cargo = Column(String(35), nullable=False)
    departamento = Column(String(35), nullable=False)
    salario = Column(Numeric(10, 2), nullable=False)
    data_contratacao = Column(
        DateTime,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP")
    )
    cargo_de_confianca = Column(Boolean, nullable=False)

    # RELATIONSHIP -> funcionário participa de várias montagens
    montagens = relationship(
        "Montagem_models",
        secondary="projeto_de_bloco_tp3.montagem_funcionario",
        back_populates="funcionarios"
    )


# ==========================
# MONTAGEM
# ==========================
class Montagem_models(Base):
    __tablename__ = "montagem"
    __table_args__ = {"schema": "projeto_de_bloco_tp3"}

    COLUNAS_PADRAO = MONTAGEM_CABECALHO
    CAMPO_ID = "id_montagem"

    id_montagem = Column(
        UUID(as_uuid=False),
        primary_key=True,
        default=uuid.uuid4
    )

    xid_cliente = Column(
        UUID(as_uuid=False),
        ForeignKey("projeto_de_bloco_tp3.clientes.id_cliente"),
        nullable=True
    )

    placa_mae = Column(String(50), nullable=False)
    fonte = Column(String(50), nullable=False)
    armazenamento1 = Column(String(50), nullable=False)
    armazenamento2 = Column(String(50))
    processador = Column(String(50), nullable=False)
    memoria_ram = Column(String(50), nullable=False)
    placa_video = Column(String(50), nullable=False)
    data_compra = Column(Date, nullable=False)
    total = Column(Numeric(10, 2), nullable=False)
    rgb = Column(Boolean, nullable=False)
    loja_preferencia = Column(String(50), nullable=False)
    water_cooler = Column(Boolean, nullable=False)

    # RELATIONSHIP -> montagem pertence a 1 cliente
    cliente = relationship(
        "Clientes_models",
        back_populates="montagens"
    )

    # RELATIONSHIP -> montagem tem vários funcionários (N:N)
    funcionarios = relationship(
        "Funcionarios_models",
        secondary="projeto_de_bloco_tp3.montagem_funcionario",
        back_populates="montagens"
    )


# ==========================
# TABELA DE JUNÇÃO
# ==========================
class MontagemFuncionario_models(Base):
    __tablename__ = "montagem_funcionario"
    __table_args__ = {"schema": "projeto_de_bloco_tp3"}

    # aqui não definimos COLUNAS_PADRAO / CAMPO_ID
    # porque normalmente você não vai usar para_lista/para_dict nela

    xid_montagem = Column(
        UUID(as_uuid=False),
        ForeignKey("projeto_de_bloco_tp3.montagem.id_montagem"),
        primary_key=True
    )

    xid_funcionario = Column(
        UUID(as_uuid=False),
        ForeignKey("projeto_de_bloco_tp3.funcionarios.id_funcionario"),
        primary_key=True
    )
