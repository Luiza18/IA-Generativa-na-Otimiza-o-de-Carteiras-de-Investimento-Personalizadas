from classes.Video import Video
from classes.Resumo import Resumo

url = "https://www.youtube.com/watch?v=lq1wkuwA2VE"

#video = Video(url)
#video.trasncrever()

conteudo = Resumo(url)
conteudo.resumir_diretorio()