from utils.menu_utils import menu_validar_entrada
from constantes.constantes import MENU_SAIR
from constantes.constantes_tp3 import MENU_OPCOES_NUM_TP3, DADOS_REDUZIDO
from negocio_tp3.negocio_crud import *
from funcionarios_tp2.funcionarios import *
from json_tp4.json_crud_db import exibirdados
from textwrap import dedent


def menu_interface():
    """Exibe a interface do menu."""

    print(dedent("""
        
        ========= MENU TP3 =========
        [1] Left Join (dicionario).
        [2] Right Join (dicionario).
        [3] Inner Join (dicionario).
        [4] Left Join (lista).
        [5] Right Join (lista).
        [6] Inner Join (lista).
        [0] Voltar para o Menu Principal 
        ============================
    """))


def menu_opcao_validar():
    """Valida a opção escolhida no menu."""

    while (True):
        menu_interface()
        opcao_escolhida = input("Escolha uma opção do menu: ")
        print()

        opcao_verificada = menu_validar_entrada(opcao_escolhida, MENU_OPCOES_NUM_TP3)

        if (opcao_verificada is None):
            print(f'Você escolheu uma opção "{opcao_escolhida}" não valido')
            print()

        return opcao_verificada


def menu_exibir_tp4():
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
                exibirdados()
            case 2:
                right_join_dicionario(DADOS_REDUZIDO)
            case 3:
                inner_join_dicionario(DADOS_REDUZIDO)
            case 4:
                left_join_lista(DADOS_REDUZIDO)
            case 5:
                right_join_lista(DADOS_REDUZIDO)
            case 6:
                inner_join_lista(DADOS_REDUZIDO)