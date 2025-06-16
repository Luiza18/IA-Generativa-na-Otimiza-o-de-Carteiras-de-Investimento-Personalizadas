import yt_dlp
import os
import whisper
import re
import requests

class Video:
    def __init__(self):
        self.__audio_path = self.__trasncricao_audio = None

    def __extraia_video_id(self, url):
        # Expressão regular para extrair o código do vídeo da URL
        match = re.search(r"v=([a-zA-Z0-9_-]+)", url)
        if match:
            return match.group(1)
        else:
            raise ValueError("Não foi possível extrair o ID do vídeo da URL fornecida.") 

    def baixar_video(self, url, pasta_destino="audios"):
        os.makedirs(pasta_destino, exist_ok=True)

        video_id = self.__extraia_video_id(url)

        opcoes = {
            "outtmpl": os.path.join(pasta_destino, f"{video_id}.%(ext)s"),  # Nome do vídeo como nome do arquivo
            "format": "bestaudio/best",
            "postprocessors": [
                {
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "mp3",
                    "preferredquality": "192",
                }
            ],
        }

        # Use extract_info para obter informações do vídeo
        with yt_dlp.YoutubeDL(opcoes) as ydl:
            ydl.extract_info(url, download=True)  # Obtém informações do vídeo e baixa o áudio


        audio = video_id + ".mp3"
        txt = video_id + ".txt"

        self.__audio_path = os.path.join(os.getcwd(), pasta_destino, audio)
        self.__trasncricao_audio = os.path.join(os.getcwd(), "doc", txt)


    def transcrever(self):
        if not self.__audio_path or not os.path.exists(self.__audio_path):
            print(f"Erro: Arquivo de áudio não encontrado! ({self.__audio_path})")
            return

        modelo = whisper.load_model("base")

        resposta = modelo.transcribe(self.__audio_path)
        trasncricao =  resposta["text"]

        #arquivo = str(self.__trasncricao_audio)

        with open(self.__trasncricao_audio, 'w', encoding="utf-8") as arquivo:
            arquivo.write(trasncricao)


    def resumir(self):
        
        #with open(self.__trasncricao_audio, 'r', encoding="utf-8") as arquivo:
        with open("C:\\Users\\Luiza\\Documents\\TG\\doc\\KArD5_L1amQ.txt", 'r', encoding="utf-8") as arquivo:
            transcricao = arquivo.read()

        resposta = requests.post(
            'http://localhost:11434/api/generate',
            json={
                "model" : "qwen2.5",
                "prompt" : f"Resuma o seguinte conteúdo de maneira clara e objetiva (sem análises extras):\n\n{transcricao}",
                "stream" : False
            }
        )

        #print(f'Resposta : {resposta.content}')

        if resposta.status_code == 200:
            resumo = resposta.json()['response']

            with open("C:\\Users\\Luiza\\Documents\\TG\\doc\\resumo.txt", 'w', encoding="utf-8") as file:
                file.write(resumo)


    def run(self, url):
        self.baixar_video(url)
        self.transcrever()
        self.resumir()

