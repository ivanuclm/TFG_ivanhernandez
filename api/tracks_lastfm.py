import pickle
import time
import spotipy
import auth
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.exceptions import SpotifyException
import requests
import pandas as pd
import pylast
import concurrent.futures

# def fetch_track_data(row):
#     """
#     Obtener el número de oyentes y reproducciones globales de una canción en Last.fm.

#     Args:
#         row (pandas.Series): Fila de un DataFrame de pandas con las columnas 'artist' y 'title'.
    
#     Returns:
#         listeners (int): Número de oyentes globales de la canción en Last.fm.
#         playcounts (int): Número de reproducciones globales de la canción en Last.fm.
#     """
#     network = auth.get_lastfm()
#     while True:
#         try:
#             # fetch the track info
#             track = network.get_track(row.artist, row.title)
#             listeners = track.get_listener_count()
#             playcounts = track.get_playcount()
#             print(f"Processed {row.artist} - {row.title}")
#             return listeners, playcounts
#         except pylast.WSError as e:
#             if 'Rate Limit Exceeded' in str(e):
#                 print("Rate limit exceeded, esperamos 10 segundos antes de reintentar...")
#                 time.sleep(10)
#                 network = auth.get_lastfm()
#             else:
#                 print(f"Error obteniendo {row.artist} - {row.title}: {e}")
#                 return None, None
#         except Exception as e:
#             print(f"Error obteniendo {row.artist} - {row.title}: {e}")
#             return None, None

def fetch_track_data(row):
    """
    Obtener el número de oyentes y reproducciones globales de una canción en Last.fm.

    Args:
        row (pandas.Series): Fila de un DataFrame de pandas con las columnas 'artist' y 'title'.
    
    Returns:
        listeners (int): Número de oyentes globales de la canción en Last.fm.
        playcounts (int): Número de reproducciones globales de la canción en Last.fm.
    """
    attempt = 1
    max_attempts = 5  # Maximum number of attempts before giving up
    while attempt <= max_attempts:
        try:
            network = auth.get_lastfm()  # Recreate the Last.fm network object on each attempt
            # fetch the track info
            track = network.get_track(row.artist, row.title)
            listeners = track.get_listener_count()
            playcounts = track.get_playcount()
            print(f"Processed {row.artist} - {row.title}")
            return listeners, playcounts
        except pylast.WSError as e:
            if 'Rate Limit Exceeded' in str(e):
                wait_time = 5 * attempt  # Increase wait time with each attempt
                print(f"Rate limit exceeded, esperamos {wait_time} segundos antes de reintentar...")
                time.sleep(wait_time)
                attempt += 1
            else:
                print(f"Error obteniendo {row.artist} - {row.title}: {e}")
                return None, None
        except Exception as e:
            print(f"Error obteniendo {row.artist} - {row.title}: {e}")
            return None, None
    print("Max attempts reached. Giving up.")
    return None, None

def get_tracks_lastfm(df):
    """
    Llamadas concurrentes para obtener los datos de Last.fm para cada canción en un DataFrame de pandas.

    Args:
        df (pandas.DataFrame): DataFrame de pandas con las columnas 'artist' y 'title'.

    Returns:
        results (list): Lista de tuplas con los datos de Last.fm para cada canción.
    """
    # create a ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # use the executor to map the fetch_track_data function to each row in the dataframe
        results = list(executor.map(fetch_track_data, df.itertuples(index=False)))
    return results

if __name__=='__main__':
    # Set up Spotipy client
    network = auth.get_lastfm()

    # load your data
    df = pd.read_csv('output/dataset_spotify.csv')

    # create a ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # use the executor to map the fetch_track_data function to each row in the dataframe
        results = list(executor.map(fetch_track_data, df.itertuples(index=False)))

    # add the data to your dataframe
    df['lastfm_listeners'], df['lastfm_playcounts'] = zip(*results)

    popularity = df.pop('popularity')
    df['popularity'] = popularity
    # save the updated dataframe
    df.to_csv('output/dataset_spotify_lastfm.csv', index=False)