from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pathlib
import configparser

DIR = pathlib.Path(__file__).parent.resolve() # Pegar o diretório atual do arquivo
CONFIG_PATH = DIR / "banco.env"


def ler_arquivo_config():
    params = configparser.ConfigParser()
    params.read(CONFIG_PATH)
    return params

def conectar():
    session = None
    try:
        params = ler_arquivo_config()
        engine = create_engine(
            "postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}?sslmode=require".format(
            host=params.get("DB", "host"),
            username=params.get("DB", "username"),
            password=params.get("DB", "password"),
            database=params.get("DB", "database"),
            port=params.getint("DB", "port")
        ))
        session = sessionmaker(bind=engine)()
        
    except Exception as ex:
        print(f"Erro ao conectar ao banco de dados: {ex}")
    return session

def desconectar(session):
    if session:
        session.close()


