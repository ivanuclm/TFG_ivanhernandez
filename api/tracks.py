
import numpy as np
import pandas as pd
import time
import pickle # Guardar y cargar datos en formato binario

import requests

# No usamos hilos concurrentes para evitar los límites de la API de Spotify.
# from concurrent.futures import ThreadPoolExecutor

# Importa el módulo auth.py que contiene las credenciales de la API de Spotify
import api.auth as auth 
# Excepciones de la API de Spotify (Too Many Requests por exceso de llamadas)
from spotipy.exceptions import SpotifyException


def get_tracks_from_artists(artist_ids, require_sp=True):
    """
    Extrae todas las canciones de los artistas incluidos en la lista de artist_ids.
    Para ello, busca la discografía de cada artista y extrae las canciones de cada álbum.
    Nota: Este método puede tardar mucho tiempo en ejecutarse, dependiendo del número de artistas y de cuántas canciones hayan lanzado. Para nuestro listado con los 2500 artistas más populares, se tardó aproximadamente 24 horas en extraer todas las canciones.    

    Args:
        artist_ids (list): Lista de IDs de artistas en Spotify. Ejemplo: ['12Chz98pHFMPJEknJQMWvI', '06HL4z0CvFAxyc27GXpf02'] (Son los IDs de Muse y Taylor Swift, respectivamente.)

    Returns:
        track_info_list (list): Lista de diccionarios con la información de las canciones extraídas. Cada diccionario tiene las siguientes claves:
            - 'id': ID de la canción en Spotify.
            - 'name': Nombre de la canción.
            - 'artist': Nombre del artista.
            - 'artist_id': ID del artista en Spotify.
            - 'album': Nombre del álbum.
            - 'album_type': Tipo del álbum (album, single, etc.)
            - 'album_total_tracks': Número total de canciones en el álbum.
            - 'disc_number': Número del disco (del álbum) en el que se encuentra la canción (normalmente 1).
            - 'track_number': Número de la canción en el álbum (Primera canción, segunda canción, etc.)
            - 'release_date': Fecha de lanzamiento del álbum (en formato YYYY-MM-DD).
            - 'duration_ms': Duración de la canción en milisegundos.
            - 'explicit': True si la canción es explícita (contiene lenguaje malsonante), False en caso contrario.

    """
    if require_sp:
        sp = auth.get_spotipy()

    print(f"----- Extraemos las canciones de {len(artist_ids)} artistas -----")
    track_info_list = []
    counter_artists = 0
    for artist_id in artist_ids:
        counter_artists += 1
        try:
            albums = sp.artist_albums(artist_id, album_type='album', limit=50)
            for album in albums['items']:
                retry = 3
                while retry > 0:
                    try:
                        tracks = sp.album_tracks(album['id'], limit=50)
                        retry = 0
                        for track in tracks['items']:
                            track_info = {
                                'id': track['id'],
                                'name': track['name'],
                                'artist': track['artists'][0]['name'],
                                'artist_id': track['artists'][0]['id'],
                                'album': album['name'],
                                'album_id': album['id'],
                                # 'album_type': album['album_type'], # con album_type='album' no tiene sentido
                                'album_total_tracks': album['total_tracks'],
                                'disc_number': track['disc_number'],
                                'track_number': track['track_number'],
                                'release_date': album['release_date'],
                                'duration_ms': track['duration_ms'],
                                'explicit': track['explicit'],
                            }
                            track_info_list.append(track_info)

                        # Guardamos el progreso en un archivo por si se interrumpe la ejecución
                        with open('output/tmp/tracks_WIP.pkl', 'wb') as f:
                            pickle.dump(track_info_list, f)
                        
                        print(f"\033[K\t{len(track_info_list)} tracks extraídas. Quedan {len(artist_ids) - counter_artists} artistas por extraer. Último artista procesado: {artist_id}.", end='\r')
                        time.sleep(1)  # To avoid rate limits
                    # time.sleep(2)  # To avoid rate limits
                    except requests.exceptions.HTTPError as e:
                        if e.response.status_code == 504:
                            print("\n504 Gateway Timeout error encountered. Reintentando en 5 seconds...\n")
                            time.sleep(5)
                            retry -= 1
                            continue
                        else:
                            raise e
        except SpotifyException as e:
            if e.http_status == 429:
                retry_after = int(e.headers['Retry-After'])
                hours, remainder = divmod(retry_after, 3600)
                minutes, seconds = divmod(remainder, 60)
                print(f"Rate limit exceeded, esperamos {hours} horas, {minutes} minutos, y {seconds} segundos")
                time.sleep(retry_after)
                continue
            else:
                raise e
        except Exception as e:
            print(f"Error procesando artista {artist_id}: {e}")
        print("\n")
        print(f"\033[K\t\tArtista procesado: {(artist_id)}")
        print("\n")
    return track_info_list


if __name__=='__main__':
    
    sp = auth.get_spotipy()

    with open('input/top_artist_ids.pkl', 'rb') as f:
        artist_ids_full = pickle.load(f)

    # artist_ids = artist_ids_full[:10] # Para pruebas
    artist_ids = artist_ids_full # Para ejecución completa

    track_list = get_tracks_from_artists(artist_ids, require_sp=False)
    with open('output/raw/tracks.pkl', 'wb') as f:
        pickle.dump(track_list, f)
    
    
    

                                             