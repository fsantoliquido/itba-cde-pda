# utils.py

from googleapiclient.discovery import build
from datetime import datetime
import os
import time
from googleapiclient.errors import HttpError
import isodate


# Lista de canales a monitorear
CHANNEL_IDS = [
    'UC7mJ2EDXFomeDIRFu5FtEbA', 
    'UCvCTWHCbBC0b9UIeLeNs8ug',
    'UCWSfXECGo1qK_H7SXRaUSMg',
    'UCTHaNTsP7hsVgBxARZTuajw',
    'UC4mdhKZXjrKoq5aVG6juHEg' 
]


# Convertir duracion que da Youtube ISO 8601 a un objeto de tiempo
def convert_duration_to_seconds(duration_iso):

    duration = isodate.parse_duration(duration_iso)
    return int(duration.total_seconds())


# Inicio la api de Youtube con mis credenciales
def initialize_youtube_api():
    API_KEY = os.getenv('YOUTUBE_API_KEY')
    youtube = build('youtube', 'v3', developerKey=API_KEY)
    return youtube


# Funcion para buscar videos de un canal (ajustada para manejar errores)

def get_videos_from_channel(youtube, channel_id, published_after, max_requests=10, sleep_time=1):
    videos = []
    request_count = 0
    
    try:
        request = youtube.search().list(
            part='snippet',
            channelId=channel_id,
            publishedAfter=published_after,
            order='date',
            type='video'
            #,maxResults=10  # Puedes ajustar según tus necesidades
        )
        response = request.execute()

        # De la respuesta sacamos titulo, momento de publicacion, id del video y tipo de video (live, upcoming -vivo programado- o none/tradicional)
        for item in response['items']:
            video_data = {
                'title': item['snippet']['title'],
                'published_at': item['snippet']['publishedAt'],
                'video_id': item['id']['videoId'],
                'video_type': item['snippet'].get('liveBroadcastContent', 'none')  # live, upcoming o none
            }
            videos.append(video_data)
        
        request_count += 1
        
        # Si hemos alcanzado el límite de solicitudes, hacemos un sleep
        if request_count % max_requests == 0:
            print(f"Realizadas {request_count} solicitudes. Esperando {sleep_time} segundos para continuar...")
            time.sleep(sleep_time)

    except HttpError as e:
        print(f"Error al obtener videos del canal {channel_id}: {e}")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")

    return videos


#Busca la información del canal de Youtube que le pases
def get_channel_info(youtube, channel_id):
    try:
        request = youtube.channels().list(
            part='snippet,statistics',
            id=channel_id
        )
        response = request.execute()

        # la respuesta tiene una key "items" contiene la data del channel y la cantidad de suscriptores 
        channel_info = response['items'][0]
        channel_data = {
            'channel_name': channel_info['snippet']['title'],
            'channel_id': channel_info['id'],
            'subscriber_count': channel_info['statistics'].get('subscriberCount', 0)
        }

        return channel_data

    except HttpError as e:
        print(f"Error al obtener información del canal {channel_id}: {e}")
        return None
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")
        return None


#Una funcion para pasarle el id de un video y obtener las estadísticas
def get_video_statistics(youtube, video_ids, max_requests=10, sleep_time=2):
    video_stats = []
    request_count = 0
    
    # Pasamos los IDs de los videos como una lista separada por comas
    try:
        stats_request = youtube.videos().list(
            part='statistics,contentDetails',
            id=','.join(video_ids)  
        )
        stats_response = stats_request.execute()

        # Procesamos las estadísticas
        for stats in stats_response['items']:
            # Acá uso la función para convertir a tiempo desde el formato ISO
            duration_iso = stats['contentDetails']['duration']
            duration_seconds = convert_duration_to_seconds(duration_iso)
            
            # Verificamos si el video es un YouTube Short (menos de 60 segundos)
            is_short = duration_seconds < 60
            
            stats_data = {
                'video_id': stats['id'],
                'view_count': stats['statistics'].get('viewCount', 0),
                'like_count': stats['statistics'].get('likeCount', 0),
                'comment_count': stats['statistics'].get('commentCount', 0),
                'duration': duration_seconds,
                'is_short': is_short
            }
            video_stats.append(stats_data)
        
        request_count += 1

        # Si hemos alcanzado el límite de solicitudes, hacemos un sleep
        if request_count % max_requests == 0:
            print(f"Realizadas {request_count} solicitudes. Esperando {sleep_time} segundos para continuar...")
            time.sleep(sleep_time)

    except HttpError as e:
        print(f"Error al obtener estadísticas de videos: {e}")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")

    return video_stats


# Función para sacar del json el datos que necesito
def transform_video_data(videos):
    transformed_data = []
    for video in videos:
        title = video['snippet']['title']
        published_at = video['snippet']['publishedAt']
        transformed_data.append({
            'title': title,
            'published_at': published_at
        })
    return transformed_data

# Función para agrupar datos por dia 
def group_videos_by_date(videos):
    grouped_data = {}
    for video in videos:
        date = video['published_at'].split("T")[0]
        if date not in grouped_data:
            grouped_data[date] = []
        grouped_data[date].append(video)
    return grouped_data