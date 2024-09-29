import pytest
from unittest.mock import patch
from module_etl.etl import get_videos_from_channel, get_channel_info, get_video_statistics
from module_etl.utils import initialize_youtube_api

@patch('module_etl.utils.build') #Uso patch para que cuando lea module_etl.utils.build en la función real lo reemplace por lo mockeado
def test_get_videos_from_channel_success(mock_build):
    
    """ En esta función se intenta simular la respuesta de la api de youtube que busca los videos de un canal 
        Tiene tres etapas:
    1. Mockear la llamada a la API de YouTube con mock_youtube
    2. Llamar a la función `get_videos_from_channel` para obtener los videos del canal, pasándole el objeto mock
    3. Validar que la función devuelve los videos correctamente, verificando cantidad de datos y que los campos title y video_id coinciden con los valores de la respuesta simulada.
    """
    
    mock_build = mock_build.return_value
    mock_youtube = mock_build.youtube.return_value
    mock_youtube.search().list().execute.return_value = {
        'items': [
            {
                'snippet': {
                    'title': 'Video 1', 
                    'publishedAt': '2024-09-28T12:00:00Z',
                    'liveBroadcastContent': 'none'
                },
                'id': {'videoId': 'video_id_1'},
            },
            {
                'snippet': {
                    'title': 'Video 2', 
                    'publishedAt': '2024-09-28T12:00:00Z',
                    'liveBroadcastContent': 'live'
                },
                'id': {'videoId': 'video_id_2'},
            }
        ]
    } 

    channel_id = 'OLGA12345'
    published_after = '2024-09-24T12:00:00Z'
    
    videos = get_videos_from_channel(mock_youtube, channel_id, published_after)
    print('hasta acá')
    assert len(videos) == 2
    assert videos[0]['title'] == 'Video 1'
    assert videos[0]['video_id'] == 'video_id_1'


@patch('module_etl.utils.build')
def test_get_channel_info_success(mock_build):
    
    """   En esta funcción intento simular la request para obtener la información del canal.
    Tiene tres etapas:
    1. Mockear la llamada a la API para devolver una respuesta simulada que contenga el nombre del canal y el número de suscriptores.
    2. Llamar a la función `get_channel_info` pasándole el mock a la API.
    3. Validar que la función devuelve la información del canal correctamente, verificando el channel_name y cantidad de subscribers"""

    # Simular la respuesta de la API de YouTube
    mock_youtube = mock_build.return_value
    mock_youtube.channels().list().execute.return_value = {
        'items': [
            {
                'snippet': {'title': 'OLGA TEST'},
                'id': 'OLGA1234',
                'statistics': {'subscriberCount': '10000'}
            }
        ]
    }
    
    channel_id = 'OLGA1234'
    channel_info = get_channel_info(mock_youtube, channel_id)
    
    assert channel_info['channel_name'] == 'OLGA TEST'
    assert channel_info['subscriber_count'] == '10000'


@patch('module_etl.utils.build') 
def test_get_video_statistics_success(mock_build):
    """ En esta funcción intento simular la request para obtener las estadísticas del canal.
        Tiene tres etapas:
        1. Mockear la llamada a la API para devolver una respuesta con estadísticasdel video.
        2. Llamar a la función get_video_statistics pasándole el mock y una lista de IDs de videos.
        3. Validar que la función devuelve las estadísticas del video correctamente, verificando view_count, like_count, duration y si es Short o no """
    # Simular la respuesta de la API de YouTube
    mock_youtube = mock_build.return_value
    mock_youtube.videos().list().execute.return_value = {
        'items': [
            {
                'id': 'video_id_1',
                'statistics': {'viewCount': '1000', 'likeCount': '100', 'commentCount': '10'},
                'contentDetails': {'duration': 'PT3M'}
            }
        ]
    }

    video_ids = ['video_id_1']
    
    stats = get_video_statistics(mock_youtube, video_ids)
    
    assert len(stats) == 1
    assert stats[0]['view_count'] == '1000'
    assert stats[0]['like_count'] == '100'
    assert stats[0]['duration'] == 180  # Acá ajusto a segudnos porque dentro de get_video_statistics se cambia del formato ISO a segundos