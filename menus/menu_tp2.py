from utils.menu_utils import menu_validar_entrada
from constantes.constantes import MENU_SAIR
from constantes.constantes_tp2 import MENU_OPCOES_NUM_TP2
from funcionarios_tp2.funcionarios_crud_db import funcionarios_criar_resetar
from funcionarios_tp2.funcionarios import *
from textwrap import dedent


def menu_interface():
    """Exibe a interface do menu."""

    print(dedent("""
        Obs: Na frente do enunciado vai ter o número do exercício correspondente. Pode ser que o enunciado esteja diferente para ser compatível com o banco de dados criado.
        
        ========= MENU TP2 =========
        [1] Resetar e Criar Tabela de Funcionários (tudo automatico com dados de exemplo do professor)
        [2] Exibir Funcionário
        [3] Selecione todos os funcionários que trabalham no departamento Técnico (Exercico 1).
        [4] Selecione os nomes dos funcionários que possuem um salário maior que 5.000,00 (Exercício 2).
        [5] Selecione o nome e a data de contratação dos funcionários que foram contratados após 01/01/2022. (Exercício 3).
        [6] Selecione o departamento e o salário médio de cada departamento. (Exercício 4).
        [7] Selecione o nome e o cargo dos funcionários que possuem "da Silva" no nome. (Exercício 5).
        [8] Selecione todos os funcionários que têm cargos de confiança. (Exercício 6).
        [9] Selecione o nome e o departamento dos Montador Júnior. (Exercício 7).
        [10] Selecione o nome dos funcionários e seus salários ordenados de forma decrescente pelo salário. (Exercício 8).
        [11] Selecione o nome e o ID dos funcionários que foram contratados no ano de 2023. (Exercício 9).
        [12] Selecione o nome dos funcionários que trabalham no departamento Administração e possuem um salário menor ou igual a 3.000,00. (Exercício 10).
        [13] Selecione o nome dos funcionários que são Coordenador Administrativo ou Diretor. (Exercício 11).
        [14] Selecione o nome dos funcionários e os anos de experiência (considerando que estamos em 2025). (Exercício 12).
        [15] Selecione o nome e o departamento dos funcionários, ordenados pelo nome em ordem alfabética. (Exercício 13).
        [16] Selecione o nome e o cargo dos funcionários cujo nome começa com 'João'. (Exercício 14).
        [17] Selecione a quantidade de funcionários em cada departamento. (Exercício 15).
        [0] Voltar para o Menu Principal 
        ============================
    """))


def menu_opcao_validar():
    """Valida a opção escolhida no menu."""

    while (True):
        menu_interface()
        opcao_escolhida = input("Escolha uma opção do menu: ")
        print()

        opcao_verificada = menu_validar_entrada(opcao_escolhida, MENU_OPCOES_NUM_TP2)

        if (opcao_verificada is None):
            print(f'Você escolheu uma opção "{opcao_escolhida}" não valido')
            print()

        return opcao_verificada


def menu_exibir_tp2():
    """
    Exibe o menu principal e encaminha para sua função.
    """
    
    while (True):
        opcao_validade = menu_opcao_validar()
        # N entendi muito bem pq o "MENU_SAIR" n funciona no match direito, então fiz separado
        if (opcao_validade == MENU_SAIR):
            print("Você voltou para o Menu Principal.")
            return

        # Não vou criar um "default" pq a própria função "menu_opcao_validar" já faz esse tratamento.
        match (opcao_validade):
            case 1:
                funcionarios_criar_resetar()
            case 2:
                funcionarios_exibir()
            case 3:
                exercicio1_exibir()
            case 4:
                exercicio2_exibir()
            case 5:
                exercicio3_exibir()
            case 6:
                exercicio4_exibir()
            case 7:
                exercicio5_exibir()
            case 8:
                exercicio6_exibir()
            case 9:
                exercicio7_exibir()
            case 10:
                exercicio8_exibir()
            case 11:
                exercicio9_exibir()
            case 12:
                exercicio10_exibir()
            case 13:
                exercicio11_exibir()
            case 14:
                exercicio12_exibir()
            case 15:
                exercicio13_exibir()
            case 16:
                exercicio14_exibir()
            case 17:
                exercicio15_exibir()