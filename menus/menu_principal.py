from utils.menu_utils import menu_validar_entrada
from constantes.constantes import MENU_SAIR, MENU_OPCOES_NUM
from .menu_tp1 import menu_exibir_tp1
from .menu_tp2 import menu_exibir_tp2
from .menu_tp3 import menu_exibir_tp3
from .menu_tp4 import menu_exibir_tp4
from textwrap import dedent


def menu_interface():
    """Exibe a interface do menu principal."""

    print(dedent("""
        ========= MENU PRINCIPAL =========
        [1] TP1 (Tarefas)
        [2] TP2 (Funcionários)
        [3] TP3 ( Negocio Completo)
        [0] Encerrar Programa
        ==================================
    """))


def menu_opcao_validar():
    """Valida a opção escolhida no menu principal."""

    while (True):
        menu_interface()
        opcao_escolhida = input("Escolha uma opção do menu: ")
        print()

        opcao_verificada = menu_validar_entrada(opcao_escolhida, MENU_OPCOES_NUM)

        if (opcao_verificada is None):
            print(f'Você escolheu uma opção "{opcao_escolhida}" não valido')
            print()

        return opcao_verificada


def menu_exibir():

    while (True):
        opcao_validade = menu_opcao_validar()
        # N entendi muito bem pq o "MENU_SAIR" n funciona no match direito, então fiz separado
        if (opcao_validade == MENU_SAIR):
            print("Você optou por encerrar o programa.")
            return

        # Não vou criar um "default" pq a própria função "menu_opcao_validar" já faz esse tratamento.
        match (opcao_validade):
            case 1:
                menu_exibir_tp1()
            case 2:
                menu_exibir_tp2()
            case 3:
                menu_exibir_tp3()
            case 4:
                menu_exibir_tp4()