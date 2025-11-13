import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
from datetime import datetime, date, timedelta
import yfinance as yf
from classes.PostgreSQL import PostgresSQL
from config import DB_CONNECTION_STRING
from io import StringIO
import logging

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%d-%m-%Y %H:%M:%S')



class ColetorDados:
    def __init__(self):
        self.__options = webdriver.ChromeOptions()
        self.__options.add_argument("--start-maximized")
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
                df = pd.read_html(StringIO(str(table)))[0]
                df_total = pd.concat([df_total, df], ignore_index=True)
            except Exception as e:
                print(f"Erro ao processar tabela {i+1}: {e}")
        
        return df_total

    def __investidor10(self, categoria:int):
        contador = 1
        paginas = 1000
        df_geral = pd.DataFrame()

        if categoria == 1:
            tipo = "acoes"
            segmento = '//*[@id="swal2-content"]/div[1]/div[2]/div[6]/div[12]/label/span'
            setor = '//*[@id="swal2-content"]/div[1]/div[2]/div[6]/div[13]/label/span'

        elif categoria == 3:
            tipo = "fiis"
            segmento = '//*[@id="swal2-content"]/div[1]/div[2]/div[2]/div[5]/label/span'
            setor = '//*[@id="swal2-content"]/div[1]/div[2]/div[2]/div[6]/label/span'

        else:
            logging.error('❌ Categoria não encontrada!')
            return pd.DataFrame()

        while contador <= paginas:
            driver = self.__iniciar_driver()
            driver.get(f"https://investidor10.com.br/{tipo}/?page={contador}")

            try:
                if contador == 1:
                    link_paginas = WebDriverWait(driver, 10).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "ul.pagination-list li.page-item a.page-link"))
                    )

                    numeros_paginas = [int(link.text.strip()) for link in link_paginas if link.text.strip().isdigit()]
                    logging.info(f'❕ O site possui {len(numeros_paginas)} paginas')
                    paginas = max(numeros_paginas) if numeros_paginas else 1

                # Interações com elementos da página
                driver.find_element(By.XPATH, '//*[@id="page-ranking"]/section[1]/div/div[1]/div/div[3]').click()

                WebDriverWait(driver, 8).until(EC.element_to_be_clickable((By.XPATH, segmento))).click()
                WebDriverWait(driver, 8).until(EC.element_to_be_clickable((By.XPATH, setor))).click()

                WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="swal2-content"]/div[2]/span[2]'))).click()

                html = driver.page_source
                df_pagina = self.__processar_tabelas(html)

                if not df_pagina.empty:
                    df_geral = pd.concat([df_geral, df_pagina], ignore_index=True)
                contador += 1

            except Exception as e:
                try:
                    driver.find_element(By.XPATH,'//*[@id="pop_up_ads"]/div/button').click()
                except:
                    logging.error(e)
                contador += 1
            finally:
                driver.quit()

        return df_geral

    def __coletar_acoes(self):
        logging.info("🔄️ Inicianco coleta de ações")

        df_geral = self.__investidor10(1)

        if not df_geral.empty:
            df_geral['CATEGORIA'] = 1
            df_geral['TICKER'] = df_geral['Ativos'].apply(lambda x: str(x).split()[1])
            df_geral = df_geral.rename(columns={'Segmento': 'SEGMENTO', 'Setor': 'RESUMO'})
            df_geral = df_geral[['TICKER', 'SEGMENTO', 'CATEGORIA', 'RESUMO']]
            print(df_geral)

            logging.info("✅ Coleta de ações finalizadas!")

        return df_geral

    def __coletar_fiis(self):
        logging.info("🔄️ Inicianco coleta de FIIs")

        df_geral = self.__investidor10(3)

        if not df_geral.empty:
            df_geral['CATEGORIA'] = 3
            df_geral['TICKER'] = df_geral['Ativos'].apply(lambda x: str(x).split()[1])
            df_geral = df_geral.rename(columns={'Tipo de Fundo': 'RESUMO', 'Segmento': 'SEGMENTO'})
            df_geral = df_geral[['TICKER', 'SEGMENTO', 'CATEGORIA', 'RESUMO']]

            logging.info("✅ Coleta de FIIs finalizadas!")

            
        return df_geral

    def __coletar_etf(self):
        logging.info("🔄️ Inicianco coleta de ETFs")

        driver = self.__iniciar_driver()
        try:
            driver.get("https://www.etfsbrasil.com.br")
            botao_cookies = WebDriverWait(driver, 30).until(
                EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div[2]/div/button"))
            )
            botao = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/main/div[1]/div/div[3]/button"))
            )
            botao_cookies.click()
            botao.click()
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            html = driver.page_source
        finally:
            driver.quit()
        
        df = self.__processar_tabelas(html)
        df = df.rename(columns={'Categoria': 'SEGMENTO', 'Ticker': 'TICKER', 'Resumo': 'RESUMO'})
        df['CATEGORIA'] = 2
        df = df[['TICKER', 'RESUMO', 'SEGMENTO', 'CATEGORIA']]

        logging.info("✅ Coleta de ETFs finalizadas!")
        return df

    def __coletar_preco_renda_fixa(self):
        logging.info("🔄️ Inicianco coleta preços renda fixa")
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
        
        df_fix['DATA'] = pd.to_datetime(df_fix['DATA'], dayfirst=True).dt.strftime('%Y-%m-%d')
        self.__postgre.sincronizar('PRECOS', df_fix, ['TICKER', 'DATA'])
        logging.info("✅ de preços renda fixa concluída")

    def coletar_ativos(self):
        acao = self.__coletar_acoes()
        etf = self.__coletar_etf()
        fii = self.__coletar_fiis()

        df = pd.concat([acao, fii, etf], ignore_index=True)
        self.__postgre.sincronizar('ATIVOS', df, ['TICKER'])
        logging.info("✅ Dados atualizados!")

    def __coletar_precos_renda_variavel(self):
        logging.info("🔄️ Inicianco coleta de preços de renda variável")
        df = self.__postgre.read('ATIVOS')
        tickers = df['TICKER'].to_list()

        tickers_yahoo = [t + ".SA" for t in tickers]
        inicio = datetime.today() - timedelta(days=90)
        fim = datetime.today()

        dfs = []
        logging.info(f"🔄️ Iniciando download de preços para {len(tickers_yahoo)} tickers...")
        
        for original, yahoo_ticker in zip(tickers, tickers_yahoo):
            try:
                dados = yf.download(yahoo_ticker, start=inicio, end=fim, interval="1mo", auto_adjust=True)

                if dados is not None and not dados.empty:
                    dados = dados.reset_index()

                    if isinstance(dados.columns, pd.MultiIndex):
                        dados.columns = dados.columns.get_level_values(0)
                    
                    dados = dados[["Date", "Close"]].copy()
                    dados['TICKER'] = original
                    dfs.append(dados)
            except Exception as e:
                print(f"❌ Erro em {original}: {e}")

        
        if not dfs:
            print("Nenhum dado de preço foi coletado. Encerrando.")
            return

        df_final = pd.concat(dfs, ignore_index=True)
        df_final.rename(columns={"Date": "DATA", "Close": "PRECO"}, inplace=True)
        df_final['DATA'] = pd.to_datetime(df_final['DATA']).dt.strftime('%Y-%m-%d')
        df_final['DATA'] = pd.to_datetime(df_final['DATA']).dt.date.astype(str)
        df_final['PRECO'] = df_final['PRECO'].astype(float)
        df_final = df_final.drop_duplicates(subset=['TICKER', 'DATA'], keep='first') 

        logging.info(f"✅ Download concluído. {len(df_final)} registros de preços para sincronizar.")


        self.__postgre.sincronizar('PRECOS', df_final, ['TICKER', 'DATA'])
        logging.info("✅ Sincronização de preços renda variável concluída")


    def coletar_precos(self):
        self.__coletar_precos_renda_variavel()
        self.__coletar_preco_renda_fixa()
