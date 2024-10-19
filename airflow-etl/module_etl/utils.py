from googleapiclient.discovery import build
from datetime import datetime
from sqlalchemy import create_engine
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


#Tres funciones para subir a redshift: una conecta, la otra uploadea en raw y la otra hace el upsert
def connect_to_redshift():

    DATABASE_TYPE = 'redshift+psycopg2'
    DBAPI = 'psycopg2'
    ENDPOINT = os.getenv('REDSHIFT_ENDPOINT')
    USER = os.getenv('REDSHIFT_USER')        
    PASSWORD = os.getenv('REDSHIFT_PASSWORD')
    PORT = 5439
    DATABASE = os.getenv('REDSHIFT_DATABASE')
    engine = create_engine(f"{DATABASE_TYPE}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
    return engine

def upload_to_redshift(engine, df, destintation_table, schema):
    df.to_sql(destintation_table, engine, schema, index=False, if_exists='replace')


def run_sql_queries(engine):
    # Obtener la ruta absoluta del archivo queries.sql
    base_dir = os.path.dirname(os.path.abspath(__file__))  # Obtiene la ruta absoluta de module_etl
    queries_dir = os.path.join(base_dir, 'queries')  # Se une para llegar a la carpeta de queries
    
    sql_files = ['videos_upsert.sql', 'subscribers_upsert.sql']

    for sql_file in sql_files:
            queries_file_path = os.path.join(queries_dir, sql_file)

            if not os.path.exists(queries_file_path):
                raise FileNotFoundError(f"No se encontró el archivo {sql_file} en la ruta: {queries_file_path}")
            
            with open(queries_file_path, 'r') as file:
                sql_queries = file.read()
            queries = sql_queries.split(';')
            
            # Ejecuto las queries del archivo
            with engine.connect() as connection:
                for query in queries:
                    query = query.strip()
                    if query:  # Si la consulta no está vacía
                        connection.execute(query)
                        print(f"Ejecutada la consulta del archivo {sql_file}:\n{query}\n")


# Convierto duracion que da Youtube ISO 8601 a un objeto de tiempo
def convert_duration_to_seconds(duration_iso):

    duration = isodate.parse_duration(duration_iso)
    return int(duration.total_seconds())

# Inicio la api de Youtube con mis credenciales
def initialize_youtube_api():
    API_KEY = os.getenv('YOUTUBE_API_KEY')
    youtube = build('youtube', 'v3', developerKey=API_KEY)
    return youtube


# Funcion para buscar videos de un canal, con retrys

def get_videos_from_channel(youtube, channel_id, published_after, max_requests=10, sleep_time=1, max_retries=3):
    videos = []
    request_count = 0
    
    for attempt in range(max_retries):
        try:
            request = youtube.search().list(
                part='snippet',
                channelId=channel_id,
                publishedAfter=published_after,
                order='date',
                type='video'
            )
            response = request.execute()

            # De la respuesta sacamos titulo, momento de publicacion, id del video y tipo de video (live, upcoming o none)
            for item in response['items']:
                video_data = {
                    'title': item['snippet']['title'],
                    'published_at': item['snippet']['publishedAt'],
                    'video_id': item['id']['videoId'],
                    'video_type': item['snippet'].get('liveBroadcastContent', 'none')  # live, upcoming o none
                }
                videos.append(video_data)
            
            request_count += 1
            
            # Si se alcanza el límite de requests, metemos un sleep comentado
            if request_count % max_requests == 0:
                print(f"Realizadas {request_count} solicitudes. Esperando {sleep_time} segundos para continuar...")
                time.sleep(sleep_time)
            
            return videos

        except HttpError as e:
            print(f"Error al obtener videos del canal {channel_id}: {e}. Intento {attempt+1} de {max_retries}")
        except Exception as e:
            print(f"Ocurrió un error inesperado: {e}. Intento {attempt+1} de {max_retries}")
        
        # Meto otro sleep para retry
        time.sleep(sleep_time)
        
    raise Exception(f"No se pudo obtener los videos del canal {channel_id} después de {max_retries} intentos.")

#Busca la información del canal de Youtube que le pases
def get_channel_info(youtube, channel_id, max_retries=3, sleep_time=1):
    for attempt in range(max_retries):
        try:
            request = youtube.channels().list(
                part='snippet,statistics',
                id=channel_id
            )
            response = request.execute()

            # la respuesta tiene una key "items" contiene la data del canal y la cantidad de suscriptores
            channel_info = response['items'][0]
            channel_data = {
                'channel_name': channel_info['snippet']['title'],
                'channel_id': channel_info['id'],
                'subscriber_count': channel_info['statistics'].get('subscriberCount', 0)
            }

            return channel_data

        except HttpError as e:
            print(f"Error al obtener información del canal {channel_id}: {e}. Intento {attempt+1} de {max_retries}")
        except Exception as e:
            print(f"Ocurrió un error inesperado: {e}. Intento {attempt+1} de {max_retries}")
        
        # Esperamos antes de intentar nuevamente
        time.sleep(sleep_time)

    # Tiro el error para que pinche si no puedo obtener la información del canal
    raise Exception(f"No se pudo obtener la información del canal {channel_id} después de {max_retries} intentos.")

def get_video_statistics(youtube, video_ids, max_requests=10, sleep_time=2, max_retries=3):
    video_stats = []
    request_count = 0
    
    for attempt in range(max_retries):
        try:
            # Pasamos los IDs de los videos como una lista
            stats_request = youtube.videos().list(
                part='statistics,contentDetails',
                id=','.join(video_ids)
            )
            stats_response = stats_request.execute()

            # Procesamos las estadísticas
            for stats in stats_response['items']:
                duration_iso = stats['contentDetails'].get('duration', None)  # Ajuste aquí
                if duration_iso is None:
                    print(f"El video {stats['id']} no tiene duración, omitimos el dato.")
                    continue 

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

            # Si se llega el límite de solicitudes, hacemos un sleep
            if request_count % max_requests == 0:
                print(f"Realizadas {request_count} solicitudes. Esperando {sleep_time} segundos para continuar...")
                time.sleep(sleep_time)

            return video_stats

        except HttpError as e:
            print(f"Error al obtener estadísticas de videos: {e}. Intento {attempt+1} de {max_retries}")
            # Esperamos para intentar nuevamente
            time.sleep(sleep_time)
        
        #Tiro error en los errores no salvables
        except KeyError as e:
            print(f"Ocurrió un error de clave: {e}. Intento {attempt+1} de {max_retries}")
        except Exception as e:
            print(f"Ocurrió un error inesperado: {e}. Intento {attempt+1} de {max_retries}")
        
        

    # Tiro el error para que pinche si no puedo obtener las estadísticas del canal
    raise Exception(f"No se pudieron obtener las estadísticas de videos después de {max_retries} intentos.")



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