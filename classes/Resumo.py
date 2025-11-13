import requests
from classes.Video import Video
from config import DIRETORIO_RESUMO, RESUMO_GERAL
import os
import logging

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%d-%m-%Y %H:%M:%S')

class Resumo():

    def __gerador_resumo(self, transcricao, arquivo_resumo):
        resposta = requests.post(
            'http://localhost:11434/api/generate',
            json={
                "model" : "llama3.2:latest",
                "prompt" :f"Resuma o seguinte conteúdo de maneira clara e objetiva. Foque nos principais insights sobre o cenário econômico e nos setores de investimento mencionados no texto.\n\n{transcricao}",
                "system" : "Você é um analista de investimentos sênior. Seu objetivo é extrair a essência de um texto para uma análise de portfólio posterior. Seja conciso e direto ao ponto.",
                "stream" : False
            }
        )

        if resposta.status_code == 200:
          resumo = resposta.json()['response']

          with open(arquivo_resumo, 'w', encoding="utf-8") as file:
            file.write(resumo)

        else:
            logging.error(f"❌ Erro na resposta da API: {resposta.status_code} \nCorpo da resposda: {resposta.text}")

    def resumir_arquivo(self, url: str):
        video = Video(url)
        video.transcrever()
    
        with open(video.get_transcricao_audio, 'r', encoding="utf-8") as arquivo:
            transcricao = arquivo.read()

        arquivo_resumo = DIRETORIO_RESUMO + '\\' + video.get_id_video + ".txt"
        print(f"Endereço arquivi: {arquivo_resumo}")

        logging.info('🔄️ Gerando Resumo')
        self.__gerador_resumo(transcricao,arquivo_resumo)
        logging.info('✅ Resumo gerado')

    def resumir_diretorio(self):
        conteudo = ""
        
        for nome_arquivo in os.listdir(DIRETORIO_RESUMO):
           caminho_arquivo = os.path.join(DIRETORIO_RESUMO, nome_arquivo)

           with open(caminho_arquivo, 'r', encoding="utf-8") as arquivo:
            conteudo += arquivo.read() + '\n'

        self.__gerador_resumo(conteudo, RESUMO_GERAL)
        
        