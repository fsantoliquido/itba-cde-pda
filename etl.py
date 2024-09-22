from utils import initialize_youtube_api, get_videos_from_channel, transform_video_data, group_videos_by_date, CHANNEL_IDS
from datetime import datetime
import pandas as pd

"""
    Tiene tres etapas:
    iterar por cada channel y traerme la información sobre qué videos se publicaron
    iterar sobre los videos y traernos las statistics y las metemos en un dataframe
    subimos el dataframe a redshift
    
"""
# Inicializamos la API
youtube = initialize_youtube_api()

# Establezco la fecha de inicio (últimos N días)
published_after = datetime(2024, 9, 15).isoformat("T") + "Z"

all_videos = []

# Para cada channel de youtube itero y me traigo los videos
for channel_id in CHANNEL_IDS:
    videos = get_videos_from_channel(youtube, channel_id, published_after)
    transformed_videos = transform_video_data(videos)
    all_videos.extend(transformed_videos)

# Agrupo por fecha
grouped_videos = group_videos_by_date(all_videos)

# Convertir los datos agrupados en una lista de diccionarios para DataFrame
video_data = []
for date, videos in grouped_videos.items():
    for video in videos:
        video_data.append({
            'date': date,
            'title': video['title'],
            'published_at': video['published_at']
        })

# Guardamos los datos en un DataFrame
df = pd.DataFrame(video_data)

# Guardar en un archivo CSV
df.to_csv('youtube_videos.csv', index=False)

# Mostrar los primeros registros del DataFrame para verificar
print(df.head())