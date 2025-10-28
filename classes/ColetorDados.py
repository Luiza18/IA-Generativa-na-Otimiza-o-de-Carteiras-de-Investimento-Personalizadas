import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import requests
from datetime import datetime, date, timedelta
import yfinance as yf
from pypfopt.efficient_frontier import EfficientFrontier
from pypfopt import risk_models
from pypfopt import expected_returns
from pypfopt.expected_returns import mean_historical_return
from pypfopt.risk_models import CovarianceShrinkage
from classes.PostgreSQL import PostgresSQL
from config import DB_CONNECTION_STRING, FOLDER


class ColetorDados:
    def __init__(self):
        self.__options = webdriver.ChromeOptions()
        self.__options.add_argument("--start-maximized")
        self.__options.add_argument("--headless")
        self.__options.add_argument("--disable-gpu")
        self.__options.add_argument("--window-size=1920,1080")
        self.__postgre = PostgresSQL(DB_CONNECTION_STRING)
  
    def __iniciar_driver(self):
        return webdriver.Chrome(options=self.__options)

    def __processar_tabelas(self, html):
        soup = BeautifulSoup(html, "html.parser")
        tables = soup.find_all("table")
        
        if not tables:
            return

        df_total = pd.DataFrame()
        for i, table in enumerate(tables):
            try:
                df = pd.read_html(str(table))[0]
                df_total = pd.concat([df_total, df], ignore_index=True)
            except Exception as e:
                print(f"Erro ao processar tabela {i+1}: {e}")
        
        return df_total

    def __coletar_acoes(self):
        contador = 1
        paginas = 1000
        df_geral = pd.DataFrame()

        while contador <= paginas:
            driver = self.__iniciar_driver()
            driver.get(f"https://investidor10.com.br/acoes/?page={contador}")

            try:
                if contador == 1:
                    link_paginas = WebDriverWait(driver, 10).until(
                        EC.presence_of_all_elements_located(
                            (By.CSS_SELECTOR, "ul.pagination-list li.page-item a.page-link")
                        )
                    )
                    numeros_paginas = [int(link.text.strip()) for link in link_paginas if link.text.strip().isdigit()]
                    paginas = max(numeros_paginas) if numeros_paginas else 1

                # Interações com elementos da página
                driver.find_element(By.XPATH, '//*[@id="page-ranking"]/section[1]/div/div[1]/div/div[3]').click()
                WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, '//*[@id="swal2-content"]/div[1]/div[2]/div[6]/div[13]/label/span')
                    )
                ).click()
                WebDriverWait(driver, 30).until(
                    EC.element_to_be_clickable((By.XPATH, '//*[@id="swal2-content"]/div[2]/span[2]'))
                ).click()

                html = driver.page_source
                df_pagina = self.__processar_tabelas(html)

                if not df_pagina.empty:
                    df_geral = pd.concat([df_geral, df_pagina], ignore_index=True)
                contador += 1

            except Exception as e:
                try:
                    driver.find_element(By.XPATH,'//*[@id="pop_up_ads"]/div/button').click()
                except:
                    ...
                contador += 1
            finally:
                driver.quit()

        if not df_geral.empty:
            df_geral['CATEGORIA'] = 1
            df_geral['TICKER'] = df_geral['Ativos'].apply(lambda x: str(x).split()[1])
            df_geral = df_geral.rename(columns={'Segmento': 'SEGMENTO'})
            df_geral = df_geral[['TICKER', 'SEGMENTO', 'CATEGORIA']]
            return df_geral

    def __coletar_fiis(self):
        driver = self.__iniciar_driver()
        try:
            driver.get("https://fundamentus.com.br/fii_resultado.php")
            html = driver.page_source
        finally:
            driver.quit()
        
        df = self.__processar_tabelas(html)
        df['CATEGORIA'] = 3
        df = df.rename(columns={'Papel': 'TICKER'})
        df = df.dropna(subset=['Segmento'])
        df = df.rename(columns={'Segmento': 'SEGMENTO'})
        df = df[['TICKER', 'CATEGORIA', 'SEGMENTO']]
        return df

    def __coletar_etf(self):
        driver = self.__iniciar_driver()
        try:
            driver.get("https://www.etfsbrasil.com.br/")
            botao = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/main/div[1]/div/div[3]/button"))
            )
            botao.click()
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            html = driver.page_source
        finally:
            driver.quit()
        
        df = self.__processar_tabelas(html)
        df = df.rename(columns={'Categoria': 'CATEGORIA', 'Ticker': 'TICKER', 'Resumo': 'RESUMO', 'Segmento': 'SEGMENTO'})
        df['CATEGORIA'] = 2
        df = df[['TICKER', 'RESUMO', 'SEGMENTO', 'CATEGORIA']]
        return df


    def coletar_renda_fixa_preco(self):
        ano_atual = datetime.today().year
        ano_inicial = ano_atual - 1

        data_inicial = date(ano_inicial,1,1).strftime("%d/%m/%Y")
        data_final = datetime.today().strftime("%d/%m/%Y")

        dicionario = {
        "SELIC" : 1178,
        "CDI": 4389,
        "IPCA" : 433
       }

        dfs = []

        for name, code in dicionario.items():
            url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados?formato=json&dataInicial={data_inicial}&dataFinal={data_final}"
            data =  requests.get(url).json()
            df = pd.DataFrame(data)
            df['TICKER'] = name
            dfs.append(df)

        df_fix = pd.concat(dfs,ignore_index=True)
        df_fix = df_fix.rename(columns={'data':'DATA', 'valor': 'PRECO'})
        df_fix['DATA'] = pd.to_datetime(df_fix['DATA']).dt.strftime('%Y-%m-%d')
        return df_fix

    def coletar_ativos(self):
        
        acao = self.__coletar_acoes()
        etf = self.__coletar_etf()
        fii = self.__coletar_fiis()

        df = pd.concat([acao, fii, etf], ignore_index=True)
        return df

    def coletar_precos(self):
        df = self.__postgre.read('ATIVOS')
        tickers = df['TICKER'].to_list()

        tickers_yahoo = [t + ".SA" for t in tickers]
        inicio = datetime.today() - timedelta(days= 90)
        fim = datetime.today()

        dfs = []
        for original, yahoo_ticker in zip(tickers, tickers_yahoo):
            try:
                dados = yf.download(yahoo_ticker, start=inicio, end=fim, interval="1mo", auto_adjust=True)

                if dados is not None:
                    if not dados.empty: 
                    
                        dados = dados.reset_index()

                        if isinstance(dados.columns, pd.MultiIndex):
                            dados.columns = dados.columns.get_level_values(0)
                        
                        dados = dados[["Date", "Close"]].copy()

                        dados['TICKER'] = original
                        dfs.append(dados)

            except Exception as e:
                print(f"❌ Erro em {original}: {e}")

        
        df_final = pd.concat(dfs, ignore_index=True)
        df_final.rename(columns={"Date": "DATA", "Close": "PRECO"}, inplace=True)
        df_final['DATA'] = pd.to_datetime(df_final['DATA']).dt.strftime('%Y-%m-%d')
        df_final['PRECO'] = df_final['PRECO'].astype(float)
        df_final = df_final.drop_duplicates(subset=['DATA', 'PRECO'], keep='first')
        self.__postgre.sincronizar('PRECOS',df_final,['TICKER', 'DATA'])

    def calculo_markowitz(self,risk_free_rate=0.02):
        # Ler dados
        df = self.__postgre.read('PRECOS')

        # Remove duplicatas
        df = df.drop_duplicates(subset=["Date", "Tickers"], keep="last")

        # Pivot
        df_wide = df.pivot(index='Date', columns='Tickers', values='Close')

        # Permite até 20% de falhas
        limite_falhas = int(df_wide.shape[0] * 0.2)
        df_wide = df_wide.dropna(axis=1, thresh=df_wide.shape[0] - limite_falhas)

        # Remove linhas totalmente vazias
        df_wide = df_wide.dropna(axis=0, how='all')

        # 🔍 Diagnóstico
        print(f"✅ Tickers após limpeza: {df_wide.shape[1]}")
        if df_wide.empty:
            print("❌ Nenhum dado válido para cálculo de Markowitz.")
            return None

        df_wide = df_wide.loc[:, df_wide.var() > 0]
        # Substitui valores NaN restantes por interpolação
        df_wide = df_wide.interpolate(method='linear').bfill().ffill()
        

        retornos_esperados = expected_returns.mean_historical_return(df_wide)
        cov_matrix = risk_models.CovarianceShrinkage(df_wide).ledoit_wolf()

        # Usa as variáveis já calculadas
        ef = EfficientFrontier(retornos_esperados, cov_matrix)
        
        # Tentar calcular max_sharpe; remover tickers problemáticos se falhar
        while True:
            try:
                pesos = ef.max_sharpe()
                break
            except Exception as e:
                print(f"⚠️ Solver falhou: {e}")
                # Remove o ticker com maior variância condicional
                var_cond = cov_matrix.values.diagonal()
                if len(var_cond) <= 1:
                    print("❌ Não foi possível calcular o Sharpe, muito poucos ativos.")
                    return None
                idx_remover = var_cond.argmax()
                ticker_remover = cov_matrix.columns[idx_remover]
                print(f"❌ Removendo ticker problemático: {ticker_remover}")
                df_wide = df_wide.drop(columns=[ticker_remover])
                retornos_esperados = mean_historical_return(df_wide)
                cov_matrix = CovarianceShrinkage(df_wide).ledoit_wolf()
                ef = EfficientFrontier(retornos_esperados, cov_matrix)


        pesos = ef.clean_weights()
        retorno, risco, sharpe = ef.portfolio_performance(verbose=True, risk_free_rate=risk_free_rate)

        # Salvar resultados
        df_pesos = pd.DataFrame(list(pesos.items()), columns=['Ticker', 'Peso'])
        df_metricas = pd.DataFrame({'Retorno': [retorno], 'Risco': [risco], 'Sharpe': [sharpe]})

        return df_pesos
