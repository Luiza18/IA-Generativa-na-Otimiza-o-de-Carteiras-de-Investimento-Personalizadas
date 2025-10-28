from classes.ColetorDados import ColetorDados
from classes.Resumo import Resumo
from classes.Carteira import Carteira

#coletor = ColetorDados()
#coletor.coletar_precos()

#resumo = Resumo("https://www.youtube.com/watch?v=RgzvK2XCBz0")
#resumo.resumir_diretorio()

carteira = Carteira()
lista = carteira.filtrar_ativos()

print(f"LISTA: {lista}")
#carteira.filtrar()