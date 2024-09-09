from googleapiclient.discovery import build
from datetime import datetime
import os

#Importamos credenciales y testeamos con un canal. El método build nos permite conectarnos a la api de Youtube

API_KEY = os.getenv('YOUTUBE_API_KEY')
CHANNEL_ID = 'UC7mJ2EDXFomeDIRFu5FtEbA'
youtube = build('youtube', 'v3', developerKey=API_KEY)

# Establezco la fecha de inicio. Esto debería mirar los últimos N días 
published_after = datetime(2024, 9, 7).isoformat("T") + "Z"

# Le pegamos a la api con un metodo requests, sólo buscando videos y a partir de la fecha que establecimos. 
request = youtube.search().list(
    part='snippet',
    channelId=CHANNEL_ID,
    publishedAfter=published_after,
    order='date',  
    type='video'
    # ,maxResults=10  #
)

response = request.execute()

# Printeamos la response
for item in response['items']:
    title = item['snippet']['title']
    published_at = item['snippet']['publishedAt']
    print(f"Título: {title}, Publicado en: {published_at}")