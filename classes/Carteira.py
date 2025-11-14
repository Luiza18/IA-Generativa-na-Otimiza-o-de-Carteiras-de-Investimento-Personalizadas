import json
import logging
import unicodedata
from difflib import get_close_matches
import numpy as np
import pandas as pd
import requests
from pypika import Query, Table
from classes.PostgreSQL import PostgresSQL
from config import DB_CONNECTION_STRING, RESUMO_GERAL

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%d-%m-%Y %H:%M:%S')


class Carteira:
    def __init__(self):
        self.__postgre = PostgresSQL(DB_CONNECTION_STRING)
        self.__resumo_texto = self.__ler_resumo()
        self.__PROMPT_TEMPLATE = (
        "Você é um modelo de classificação SEMÂNTICA. Sua tarefa é identificar quais segmentos da lista oficial "
        "são REALMENTE relevantes para o contexto do texto analisado.\n\n"
        "REGRAS OBRIGATÓRIAS:\n"
        "1. Selecione no máximo 5 a 10 segmentos.\n"
        "2. Escolha SOMENTE os segmentos MAIS IMPORTANTES e diretamente relacionados ao tema central.\n"
        "3. Ignore completamente categorias que sejam apenas tangencialmente relacionadas.\n"
        "4. NÃO liste a maioria dos segmentos.\n"
        "5. NÃO invente novos nomes, só pode usar exatamente o que está na lista oficial.\n"
        "6. A saída deve ser: segmento1; segmento2; segmento3...\n\n"
        "<lista-oficial>\n{lista_segmentos}\n</lista-oficial>\n\n"
        "<texto-analise>\n{conteudo_resumo}\n</texto-analise>\n\n"
        "SEGMENTOS MAIS RELEVANTES:"
    )

    def __ler_resumo(self) -> str:
        try:
            with open(RESUMO_GERAL, "r", encoding="utf-8") as f:
                return f.read()
        except FileNotFoundError:
            logging.error(f"❌ Arquivo resumo não encontrado em {RESUMO_GERAL}")
            return ""

    def __query_segmentos(self):
        ativos = Table("ATIVOS")
        return Query.from_(ativos).select(ativos.SEGMENTO).distinct()

    def __buscar_segmentos_postgre(self) -> list:
        df = self.__postgre.query(self.__query_segmentos().get_sql())

        if df is None or df.empty:
            logging.error("❌ Nenhum segmento foi retornado do banco")
            return []

        logging.info("✅ Segmentos coletados do __postgre")
        return df["SEGMENTO"].dropna().tolist()

    def __normalizar(self,s: str) -> str:
        s = str(s).strip().lower()
        return "".join(
            c
            for c in unicodedata.normalize("NFD", s)
            if unicodedata.category(c) != "Mn"
        )

    def __segmentos_relevantes_llm(self) -> list:
        segmentos___postgre = self.__buscar_segmentos_postgre()

        if not segmentos___postgre or not self.__resumo_texto:
            return []

        SEGMENTOS_RF = {
            "Renda Fixa", "Tesouro Direto", "Títulos Públicos",
            "C__postgre", "CRI", "CRA", "LCI", "LCA", "Debêntures",
            "IMAB-5",
            "SELIC", "CDI", "IPCA"  
        }

        mapa_normalizado = {
            self.__normalizar(s): s for s in segmentos___postgre
        }
        segmentos_norm = list(mapa_normalizado.keys())

        prompt = self.__PROMPT_TEMPLATE.format(
            lista_segmentos="\n".join(sorted(set(segmentos___postgre))),
            conteudo_resumo=self.__resumo_texto
        )

        logging.info("🔄 Consultando LLM para segmentação...")

        try:
            resp = requests.post(
                "http://localhost:11434/api/generate",
                json={
                    "model": "llama3.2:latest",
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.0},
                }
            )
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Erro de conexão com o Ollama: {e}")
            return []

        if resp.status_code != 200:
            logging.error(f"❌ Falha API LLM: {resp.status_code}")
            return []

        resposta = resp.json().get("response", "")
        resposta = resposta.replace("\n", ";").replace("*", "").replace(".", "")

        # Tokeniza resposta
        tokens = [s.strip() for s in resposta.split(";") if s.strip()]

        segmentos_finais = []

        # Validação dos segmentos sugeridos pela LLM
        for seg in tokens:
            if len(seg) > 50:
                continue

            seg_norm = self.__normalizar(seg)
            match = get_close_matches(seg_norm, segmentos_norm, n=1, cutoff=0.9)

            if match:
                oficial = mapa_normalizado[match[0]]
                if oficial not in segmentos_finais:
                    segmentos_finais.append(oficial)
                    logging.info(f"👍 Segmento válido pela LLM: {oficial}")

        for seg in SEGMENTOS_RF:
            if seg in segmentos___postgre and seg not in segmentos_finais:
                segmentos_finais.append(seg)
                logging.info(f"⚠️ Forçando inclusão de segmento essencial: {seg}")

        logging.info(f"🎯 Segmentos relevantes finais: {segmentos_finais}")
        return segmentos_finais

    def __listar_ativos_por_segmento(self) -> list:
        segmentos = self.__segmentos_relevantes_llm()

        if not segmentos:
            logging.warning("⚠️ Nenhum segmento relevante — retornando lista vazia")
            return []

        ativos_tb = Table("ATIVOS")
        query = (
            Query.from_(ativos_tb)
            .select(ativos_tb.TICKER)
            .where(ativos_tb.SEGMENTO.isin(segmentos))
            .distinct()
        )

        df = self.__postgre.query(query.get_sql())
        if df is None or df.empty:
            logging.warning("⚠️ Nenhum ativo encontrado no filtro de segmentos")
            return []

        # Expansão caso tenha poucos ativos
        if len(df) < 10:
            logging.warning("⚠️ Poucos ativos — expandindo seleção...")
            extra_query = (
                Query.from_(ativos_tb)
                .select(ativos_tb.TICKER)
                .limit(30)
            )
            extra = self.__postgre.query(extra_query.get_sql())
            if extra is not None:
                df = pd.concat([df, extra]).drop_duplicates()

        logging.info(f"🔎 Total de ativos usados: {len(df)}")
        return df["TICKER"].tolist()


    def __obter_historico_precos(self) -> pd.DataFrame:
        tickers = self.__listar_ativos_por_segmento()

        if not tickers:
            return pd.DataFrame()

        precos_tb = Table("PRECOS")
        query = (
            Query.from_(precos_tb)
            .select(precos_tb.DATA, precos_tb.TICKER, precos_tb.PRECO)
            .where(precos_tb.TICKER.isin(tickers))
            .orderby(precos_tb.DATA)
        )

        df = self.__postgre.query(query.get_sql())
        if df is None or df.empty:
            return pd.DataFrame()

        df["DATA"] = pd.to_datetime(df["DATA"])
        df = df.drop_duplicates(subset=["DATA", "TICKER"])

        df = df.pivot(index="DATA", columns="TICKER", values="PRECO")
        df = df.ffill().bfill().dropna(axis=1, how="all")

        logging.info(f"📈 Histórico carregado: {df.shape}")
        return df

    def __limpar_pesos(self, carteiras: dict) -> dict:
        carteiras_filtradas = {}

        for perfil, dados in carteiras.items():
            pesos = dados.get("pesos", {})

            pesos_filtrados = {
                ativo: peso
                for ativo, peso in pesos.items()
                if peso > 0
            }

            carteiras_filtradas[perfil] = {
                "pesos": pesos_filtrados,
                "performance": dados.get("performance", {})
            }

        return carteiras_filtradas


    def calcular_carteiras(self):
        df = self.__obter_historico_precos()

        if df.empty:
            logging.error("❌ Sem dados suficientes para calcular carteiras")
            return {}

        retornos = df.pct_change().dropna()
        retorno_medio = retornos.mean() * 252
        volatilidade = retornos.std() * np.sqrt(252)
        cov = retornos.cov() * 252
        
        ativos_ordenados = volatilidade.sort_values().index.tolist()
        grupos_split = np.array_split(ativos_ordenados, 3)

        safe = grupos_split[0].tolist()
        medio = grupos_split[1].tolist()
        risco = grupos_split[2].tolist()

        logging.info(f"Grupos de risco: Safe({len(safe)}), Medio({len(medio)}), Risco({len(risco)})")

        vol_min = float(volatilidade.min())
        vol_max = float(volatilidade.max())
        dispersao = (vol_max - vol_min) / max(vol_max, 1e-9)
        dispersao = max(0.2, min(dispersao, 0.8))

        pesos_perfil = {
            "conservador": [
                max(0, 0.50 + dispersao * 0.4),
                max(0, 0.35 - dispersao * 0.2),
                max(0, 0.15 - dispersao * 0.2),
            ],
            "moderado": [0.33, 0.34, 0.33],
            "arrojado": [
                max(0, 0.20 - dispersao * 0.1),
                max(0, 0.30 - dispersao * 0.2),
                max(0, 0.50 + dispersao * 0.3),
            ],
        }

        for perfil in pesos_perfil:
            total = sum(pesos_perfil[perfil])
            pesos_perfil[perfil] = [p / total for p in pesos_perfil[perfil]]

        grupos = [safe, medio, risco]
        carteiras = {}

        for perfil, pesos in pesos_perfil.items():
            carteira = {}

            for grupo, peso_grupo in zip(grupos, pesos):
                if not grupo:
                    continue

                peso_individual = peso_grupo / len(grupo)

                for ativo in grupo:
                    carteira[ativo] = peso_individual

            w = np.array([carteira.get(col, 0) for col in df.columns])

            retorno_port = float(np.sum(w * retorno_medio))
            vol_port = float(np.sqrt(w.T @ cov.values @ w))
            sharpe = retorno_port / vol_port if vol_port > 0 else 0

            carteiras[perfil] = {
                "pesos": {t: float(p) for t, p in carteira.items()},
                "performance": {
                    "retorno_esperado": retorno_port,
                    "volatilidade_anual": vol_port,
                    "sharpe_ratio": sharpe,
                },
            }

        carteiras_limpa = self.__limpar_pesos(carteiras)

        with open("carteiras_otimizadas.json", "w", encoding="utf-8") as f:
            json.dump(carteiras_limpa, f, ensure_ascii=False, indent=4)

