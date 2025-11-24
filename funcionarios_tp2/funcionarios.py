from models.models_tp2 import Funcionarios_models
import pandas as pd
from constantes.constantes_tp2 import FUNCIONARIOS_CABECALHO
from .funcionarios_crud_db import *

"""
Sinceramente n curti muito o pandas, porém como n usei ele para manipular os dados, porém como foi muito ensinado em aula,
optei por usar ele apenas para exibir a tabela de forma mais organizada e tb n ter sido algo em "vão".
"""

def _exibir_tabela(dados, colunas, titulo: str):
    print(f"\n--- {titulo} ---")
    if not dados:
        print("Nenhum registro encontrado.")
        return

    df = pd.DataFrame(dados, columns=colunas)
    print(df.to_string(index=False))
    
    
def funcionarios_exibir():
    dados = funcionarios_exibir_db("lista")
    _exibir_tabela(dados, FUNCIONARIOS_CABECALHO, "Funcionários")


def exercicio1_exibir():
    dados = exercicio1_db("lista")
    _exibir_tabela(dados, FUNCIONARIOS_CABECALHO, "Exercício 1")


def exercicio2_exibir():
    dados = exercicio2_db("lista")
    _exibir_tabela(dados, ["Nome", "Salário"], "Exercício 2")


def exercicio3_exibir():
    dados = exercicio3_db("lista")
    _exibir_tabela(dados, ["Nome", "Data de Contratação"], "Exercício 3")
    
def exercicio4_exibir():
    dados = exercicio4_db("lista")
    _exibir_tabela(dados, ["Departamento", "Média Salarial"], "Exercício 4")


def exercicio5_exibir():
    dados = exercicio5_db("lista")
    _exibir_tabela(dados, ["Nome", "Cargo"], "Exercício 5")


def exercicio6_exibir():
    dados = exercicio6_db("lista")
    _exibir_tabela(dados, ["Nome", "Cargo de confiança"], "Exercício 6")


def exercicio7_exibir():
    dados = exercicio7_db("lista")
    _exibir_tabela(dados, ["Nome", "Departamento"], "Exercício 7")


def exercicio8_exibir():
    dados = exercicio8_db("lista")
    _exibir_tabela(dados, ["Nome", "Salário"], "Exercício 8")


def exercicio9_exibir():
    dados = exercicio9_db("lista")
    _exibir_tabela(dados, ["Nome", "ID"], "Exercício 9")


def exercicio10_exibir():
    dados = exercicio10_db("lista")
    _exibir_tabela(dados, ["Nome"], "Exercício 10")


def exercicio11_exibir():
    dados = exercicio11_db("lista")
    _exibir_tabela(dados, ["Nome"], "Exercício 11")
    
def exercicio12_exibir():
    dados = exercicio12_db("lista")
    _exibir_tabela(dados, ["Nome", "Anos de Experiência"], "Exercício 12")


def exercicio13_exibir():
    dados = exercicio13_db("lista")
    _exibir_tabela(dados, ["Nome", "Departamento"], "Exercício 13")


def exercicio14_exibir():
    dados = exercicio14_db("lista")
    _exibir_tabela(dados, ["Nome", "Cargo"], "Exercício 14")

def exercicio15_exibir():
    dados = exercicio15_db("lista")
    _exibir_tabela(dados, ["Departamento", "Quantidade"], "Exercício 15")