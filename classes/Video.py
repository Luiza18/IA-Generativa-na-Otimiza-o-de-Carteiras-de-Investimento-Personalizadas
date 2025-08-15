import yt_dlp
import os
import whisper
import re
from config import DIRETORIO_TRANSCRICAO, DIRETORIO_AUDIO

class Video:
    def __init__(self, url):
        self.__url = url
        self.__id_video = self.__extraia_video_id()
        self.__audio_path = self.__transcricao_audio = None

    def __extraia_video_id(self):
        match = re.search(r"v=([a-zA-Z0-9_-]+)", self.__url)
        if match:
            return match.group(1)
        else:
            raise ValueError("Não foi possível extrair o ID do vídeo da URL fornecida.")

    @property
    def get_id_video(self):
        return self.__id_video

    @property
    def get_transcricao_audio(self):
        return self.__transcricao_audio

    def __baixar_video(self):

        configuracoes = {
            "outtmpl": os.path.join(DIRETORIO_AUDIO, f"{self.__id_video}.%(ext)s"), 
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
        with yt_dlp.YoutubeDL(configuracoes) as ydl:
            ydl.extract_info(self.__url, download=True)  # Obtém informações do vídeo e baixa o áudio

        self.__audio_path = DIRETORIO_AUDIO + "\\" + self.__id_video + ".mp3"
        self.__transcricao_audio = DIRETORIO_TRANSCRICAO + '\\' +  self.__id_video + ".txt"

    def transcrever(self):
        self.__baixar_video()
        
        if not self.__audio_path or not os.path.exists(self.__audio_path):
            return

        modelo = whisper.load_model("base")

        resposta = modelo.transcribe(self.__audio_path)
        transcricao =  resposta["text"]

        with open(self.__transcricao_audio, 'w', encoding="utf-8") as arquivo:
            arquivo.write(transcricao)
