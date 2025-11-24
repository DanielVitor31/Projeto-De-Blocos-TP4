def menu_validar_entrada(opcao_escolhida, opcao_limite):
    """Valida a entrada do usuário para o menu principal."""

    if (opcao_escolhida.isdigit()):
        opcao_escolhida = int(opcao_escolhida)
    else:
        return None

    if (opcao_escolhida >= 0 and opcao_escolhida <= opcao_limite):
        return opcao_escolhida
    else:
        return None
