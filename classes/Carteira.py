import pandas as pd
from config import DIRETORIO_RESUMO, DB_CONNECTION_STRING
from classes.PostgreSQL import PostgresSQL
from pypika import Table, Query, Case, Field
from pypika.terms import CustomFunction
import requests

class Carteira:
    def __init__(self) -> None:
        self.__resumo_path = f"{DIRETORIO_RESUMO}\\Resumo total.txt"
        self.__postgre = PostgresSQL(DB_CONNECTION_STRING)
        self.__conteudo_resumo = self.__ler_resumo()

    def __ler_resumo(self) -> str:
        conteudo = None
        with open(self.__resumo_path, 'r', encoding='utf-8') as f:
            conteudo = f.read()
        return conteudo

    def __query_segmentos(self):
        ativos = Table("ATIVOS")

        query = (
            Query.from_(ativos)
            .select(
            ativos.TICKER,
            Case()
            .when((ativos.CATEGORIA == 2) | (ativos.CATEGORIA == 4), ativos.RESUMO)
            .else_(ativos.SEGMENTO)
            .as_("NOME")
            )
        ).as_("sub")
        return query

    def __segmentos(self) -> list:
        query = self.__query_segmentos()
        df = self.__postgre.query(query.get_sql())
        lista = df["NOME"].dropna().tolist()
        return lista

    def __segmentos_recomendados(self) -> list:

        segmentos = self.__segmentos()

        if not segmentos:
            return []

        prompt = (
            f"Analise cuidadosamente o texto a seguir:\n\n"
            f"{self.__conteudo_resumo}\n\n"
            f"Abaixo está a lista de segmentos disponíveis:\n{segmentos}\n\n"
            "Com base no conteúdo do texto, identifique apenas os segmentos da lista acima que são mencionados"
            "Retorne SOMENTE os nomes dos segmentos encontrados, exatamente como aparecem na lista, "
            "separados por ponto e vírgula (;), sem adicionar explicações ou texto adicional."
        )

        try:
            resposta = requests.post(
                'http://localhost:11434/api/generate',json={
                "model": "qwen3:0.6b",
                "prompt": prompt, 
                "stream": False
                }
            )
            resposta.raise_for_status()
        except requests.RequestException as e:
            print(f"❌ Erro na API: {e}")
            return []

        dados = resposta.json()['response']
        segmentos_recomendados = dados.split(';')
        return segmentos_recomendados

    def filtrar_ativos(self):

        segmentos_recomendados = self.__segmentos_recomendados()
        segmentos_recomendados = [f"%{item.strip('%')}%" for item in segmentos_recomendados]

        subquery = self.__query_segmentos()

        array_str = "ARRAY[" + ",".join(f"'{s}'" for s in segmentos_recomendados) + "]"
        ANY = CustomFunction("ANY", [array_str])

        query = (
            Query.from_(subquery)
            .select(subquery.TICKER)
            .where(
                Field("NOME").ilike(ANY)
            )
        )

        df = self.__postgre.query(query.get_sql())
        ativos = df["TICKER"].dropna().tolist()
        return ativos
     
