import requests
from classes.Video import Video
from config import DIRETORIO_RESUMO
import os

class Resumo():
    def __init__(self, url):
        self.__video = Video(url)

    def __gerador_resumo(self, transcricao, arquivo_resumo):
        resposta = requests.post(
            'http://localhost:11434/api/generate',
            json={
                "model" : "qwen2.5",
                "prompt" : f"Resuma o seguinte conteúdo de maneira clara e objetiva (sem análises extras):\n\n{transcricao}",
                "system" : "Você é um analista de investimentos sênior, e deseja encontrar melhores investimentos com o cenário atual",
                "stream" : False
            }
        )

        if resposta.status_code == 200:
          resumo = resposta.json()['response']

          with open(arquivo_resumo, 'w', encoding="utf-8") as file:
            file.write(resumo)

    def resumir_arquivo(self):
        self.__video.transcrever()
    
        with open(self.__video.get_transcricao_audio, 'r', encoding="utf-8") as arquivo:
            transcricao = arquivo.read()

        arquivo_resumo = DIRETORIO_RESUMO + '\\' + self.__video.get_id_video + ".txt"

        self.__gerador_resumo(transcricao,arquivo_resumo)

    def resumir_diretorio(self):

        arquivo_resumo =DIRETORIO_RESUMO + "\\Resumo total.txt"
        conteudo = ""
        
        for nome_arquivo in os.listdir(DIRETORIO_RESUMO):
           caminho_arquivo = os.path.join(DIRETORIO_RESUMO, nome_arquivo)

           with open(caminho_arquivo, 'r', encoding="utf-8") as arquivo:
            conteudo += arquivo.read() + '\n'

        self.__gerador_resumo(conteudo, arquivo_resumo)
        