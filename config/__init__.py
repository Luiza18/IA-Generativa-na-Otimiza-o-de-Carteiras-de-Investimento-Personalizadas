import os
from dotenv import load_dotenv

load_dotenv()

diretorio_config = os.path.dirname(os.path.abspath(__file__))

DIRETORIO_PROJETO = os.path.dirname(diretorio_config)
DIRETORIO_TRANSCRICAO = DIRETORIO_PROJETO + "\\transcricao"
DIRETORIO_RESUMO = DIRETORIO_PROJETO + "\\resumos"
DIRETORIO_AUDIO = DIRETORIO_PROJETO + "\\audios"
ARQUIVO_BAIXADOS = DIRETORIO_PROJETO + "\\doc\\videos_baixados.txt"
FOLDER = DIRETORIO_PROJETO + "\\Dados"
RESUMO_GERAL = DIRETORIO_RESUMO + "\\Resumo Geral.txt"

DB_CONNECTION_STRING  = os.getenv('DB_CONNECTION_STRING')
