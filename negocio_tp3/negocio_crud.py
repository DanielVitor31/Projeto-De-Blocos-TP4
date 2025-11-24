from constantes.constantes_tp3 import FUNCIONARIOS_CABECALHO
from .negocio_crud_db import *
from utils.negocio_tp3 import converter_dicionario, converter_lista



def left_join_dicionario(limite):
    dados = left_join()
    converter_dicionario(dados, limite)
    return 

def right_join_dicionario(limite):
    dados = right_join()
    converter_dicionario(dados, limite)
    return

def inner_join_dicionario(limite):
    dados = inner_join()
    converter_dicionario(dados, limite)
    return

def left_join_lista(limite):
    dados = left_join()
    converter_lista(dados, "lista", limite)
    return

def right_join_lista(limite):
    dados = right_join()
    converter_lista(dados, "lista", limite)
    return

def inner_join_lista(limite):
    dados = inner_join()
    converter_lista(dados, "lista", limite)
    return

