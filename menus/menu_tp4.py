from utils.menu_utils import menu_validar_entrada
from constantes.constantes import MENU_SAIR
from constantes.constantes_tp4 import MENU_OPCOES_NUM_TP4
from json_tp4.json_crud_db import *
from textwrap import dedent


def menu_interface():
    """Exibe a interface do menu."""

    print(dedent("""
        
        ========= MENU TP3 =========
        [1] Inserir e atualizar clientes.
        [2] Deletar clientes.
        [0] Voltar para o Menu Principal 
        ============================
    """))


def menu_opcao_validar():
    """Valida a opção escolhida no menu."""

    while (True):
        menu_interface()
        opcao_escolhida = input("Escolha uma opção do menu: ")
        print()

        opcao_verificada = menu_validar_entrada(opcao_escolhida, MENU_OPCOES_NUM_TP4)

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
                inserir_atualizar()
            case 2:
                deletar_clientes()
            