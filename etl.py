from utils import initialize_youtube_api, get_videos_from_channel, get_video_statistics, get_channel_info, group_videos_by_date, CHANNEL_IDS
from datetime import datetime
import pandas as pd

"""
    Tiene tres etapas:
    1. Iterar por cada channel y traer la información sobre qué videos se publicaron
    2. Iterar sobre los videos y traernos las estadísticas y las metemos en un DataFrame
    3. Subir los DataFrames (videos y suscriptores) a Redshift o guardarlos en CSV
    
    WARNING: Este código es para correr de manera diaria.
    Una vulnerabilidad que tiene es que la tabla con cantidad de suscriptores si se corre desde una fecha en particular, no va a obtener los subs desde esa fecha, si no de manera diaria
    
"""

# Inicializamos la API
youtube = initialize_youtube_api()

# Establezco la fecha de inicio (últimos N días)
published_after = datetime(2024, 9, 15).isoformat("T") + "Z"

all_videos = []
all_video_stats = []
df_subscribers_list = []

# Se itera para cadaa channel de YouTube y sacamos estadísticas: cantindad de subs a la fecha de consulta, sus videos y para cada video métricas de views, comentarios y likes
for channel_id in CHANNEL_IDS:
    # Obtenemos la información del canal (nombre, id, suscriptores)
    channel_info = get_channel_info(youtube, channel_id)
    
    # Obtener la fecha de consulta actual
    consulta_fecha = datetime.now().strftime('%Y-%m-%d')
    
    # Crear un df con la info de suscriptores del canal
    df_subscribers_list.append({
        'channel_name': channel_info['channel_name'],
        'channel_id': channel_info['channel_id'],
        'consulta_fecha': consulta_fecha,
        'subscriber_count': channel_info['subscriber_count']
    })

    videos = get_videos_from_channel(youtube, channel_id, published_after)
    
    # Filtramos videos que contengan alguno de los campos que necesitamos para obtener el video ID. 
    # Hacemos esto por si cambia el nombre de la key con la que se guarda el video_id. 
    video_ids = []
    for video in videos:
        
        if 'video_id' in video:
            video_ids.append(video['video_id'])
        elif 'id' in video and isinstance(video['id'], dict) and 'videoId' in video['id']:
            video_ids.append(video['id']['videoId'])
        else:
            # Si no se encuentra el video_id, imprimimos el video para depuración
            print(f"Video sin 'video_id' o 'id' válido detectado: {video}")
    
    # Las stats de los videos
    video_stats = get_video_statistics(youtube, video_ids)
    
    # Agregamos los videos y las estadísticas a las listas
    all_videos.extend(videos)
    all_video_stats.extend(video_stats)

# Agrupamos los videos por fecha
grouped_videos = group_videos_by_date(all_videos)

# Crear el DataFrame de videos con la información combinada
combined_data = []
for date, videos in grouped_videos.items():
    for video, stats in zip(videos, all_video_stats):
        # Por si pincha video id
        video_id = video.get('video_id') or (video['id']['videoId'] if isinstance(video['id'], dict) and 'videoId' in video['id'] else 'N/A')
        
        combined_data.append({
                        'channel_name': channel_info['channel_name'],
            'channel_id': channel_info['channel_id'],
            'title': video['title'],
            'published_at': video['published_at'],
            'video_id': video['video_id'],
            'video_type': video['video_type'],
            'view_count': stats['view_count'],
            'like_count': stats['like_count'],
            'comment_count': stats['comment_count'],
            'duration_seconds': stats['duration'],
            'is_short': stats['is_short']
            
        })

# Pasamos a un df los videos
df_videos = pd.DataFrame(combined_data)
df_videos['likes_per_view'] = df_videos['like_count'] / df_videos['view_count']
df_videos['comments_per_view'] = df_videos['comment_count'] / df_videos['view_count']

# Pasamos a un df los logs de suscriptores
df_subscribers = pd.DataFrame(df_subscribers_list)

# Guardar los DataFrames en archivos CSV
df_videos.to_csv('youtube_videos.csv', index=False)
df_subscribers.to_csv('youtube_subscribers_history.csv', mode='a', index=False, header=False)
