
import numpy as np
import pandas as pd
import time
import pickle # Guardar y cargar datos en formato binario

import artists_ids_extractor
import api.tracks as tracks_extractor
import api.tracks_audio_features as tracks_audio_features_extractor
import api.tracks_popularity as tracks_popularity_extractor
import api.artists_features as artists_features_extractor
import api.tracks_lastfm as tracks_lastfm_extractor

# No usamos hilos concurrentes para evitar los límites de la API de Spotify.
# from concurrent.futures import ThreadPoolExecutor

# Importa el módulo auth.py que contiene las credenciales de la API de Spotify
import api.auth as auth 
# Excepciones de la API de Spotify (Too Many Requests por exceso de llamadas)
from spotipy.exceptions import SpotifyException

def extraer_artist_ids(path, head=False, tail=False):
    # Si path es un enlace a kworb.net, extraemos los artistas de ahí
    if path.startswith("http"):
        return artists_ids_extractor.extraer_artist_ids_online(path)
    
    # Si es un archivo .html, extraemos los artistas de ahí
    if path.endswith(".html"):
        return artists_ids_extractor.extraer_artist_ids_html(path, head, tail)
    
    # if path is "input/top_artist_ids.pkl", load the artist ids from the file
    if path.endswith(".pkl"):
        with open(path, 'rb') as f:
            return pickle.load(f)

    artist_ids_full = artists_ids_extractor.get_artist_ids_excel(path)
    top_artist_ids = artist_ids_full

    if(head):
        top_artist_ids = artist_ids_full[:head]

    if(tail):
        top_artist_ids = artist_ids_full[-tail:]
    print(f"{len(top_artist_ids)} artistas extraídos del excel de kworb.net")

    #remove all top_artist_ids that contain "#VALOR!" or "#VALUE!"
    top_artist_ids = [artist_id for artist_id in top_artist_ids if artist_id != "#VALOR!" and artist_id != "#VALUE!"]
    
    with open('input/top_artist_ids.pkl', 'wb') as f:
        pickle.dump(top_artist_ids, f)
    
    print(f"{len(top_artist_ids)} artistas de entrada tras limpiar.")    

    return top_artist_ids

def extraer_tracks_de_artistas(top_artist_ids):
    # Extraemos las canciones de los artistas
    tracks = tracks_extractor.get_tracks_from_artists(top_artist_ids)
    with open('output/raw/tracks.pkl', 'wb') as f:
        pickle.dump(tracks, f)

    print(f"{len(tracks)} canciones extraídas")
    return tracks

def extraer_popularidad_de_tracks(track_ids):
    # Extraemos la popularidad de las canciones
    track_popularity = tracks_popularity_extractor.get_track_popularity(track_ids)
    print(f"{len(track_popularity)} canciones con popularidad extraída")

    track_popularity_no_dupes = tracks_popularity_extractor.remove_duplicates(track_popularity)
    print(f"{len(track_popularity_no_dupes)} canciones tras eliminar duplicados escogiendo la más popular")

    with open('output/raw/track_popularity.pkl', 'wb') as f:
        pickle.dump(track_popularity_no_dupes, f)
    
    df_track_popularity = pd.DataFrame(track_popularity_no_dupes)
    df_track_popularity.to_csv('output/raw/track_popularity.csv', index=False)
    
    return track_popularity_no_dupes

def extraer_audio_features_de_tracks(track_ids):
    # Extraemos las features de audio de las canciones
    audio_features = tracks_audio_features_extractor.get_track_features(track_ids)
    with open('output/raw/track_features.pkl', 'wb') as f:
        pickle.dump(audio_features, f)
    print(f"{len(audio_features)} canciones con features de audio extraídas")

    df_audio_features = pd.DataFrame(audio_features)
    df_audio_features.to_csv('output/raw/track_features.csv', index=False)

    return audio_features

def extraer_artist_features(artist_ids):
    # Extraemos los géneros de los artistas
    artist_genres = artists_features_extractor.get_artist_features(artist_ids)
    with open('output/raw/artist_genres.pkl', 'wb') as f:
        pickle.dump(artist_genres, f)
    print(f"{len(artist_genres)} artistas con géneros extraídos")

    df_artist_genres = pd.DataFrame(artist_genres)
    df_artist_genres.to_csv('output/raw/artist_genres.csv', index=False)

    return artist_genres

def extraer_lastfm_de_tracks(tracks_df):
    # Extraemos las features de audio de las canciones
    lastfm = tracks_lastfm_extractor.get_tracks_lastfm(tracks_df)
    with open('output/raw/track_lastfm.pkl', 'wb') as f:
        pickle.dump(lastfm, f)
    print(f"{len(lastfm)} canciones con features de audio extraídas")

    df_lastfm = pd.DataFrame(lastfm)
    df_lastfm.to_csv('output/raw/track_lastfm.csv', index=False)
    return lastfm

def load_and_concatenate_data(filenames):
    df = pd.DataFrame()
    for filename in filenames:
        with open(filename, 'rb') as f:
            data = pickle.load(f)
            df = pd.concat([df, pd.DataFrame(data)], ignore_index=True)
    return df

def load_and_concatenate_list(filenames):
    data = []
    for filename in filenames:
        with open(filename, 'rb') as f:
            data += pickle.load(f)
    return data


if __name__=='__main__':
    # top_artist_ids = extraer_artist_ids("input/TOP_ARTISTAS_07052024.xlsm")
    top_artist_ids = extraer_artist_ids("input/top_artist_ids.pkl")

    print("---Artist IDs extraídos del excel de kworb.net---")

    # tracks = extraer_tracks_de_artistas(top_artist_ids)
    tracks = load_and_concatenate_list(['output/tmp/tracks_WIP_batch1.pkl', 'output/tmp/tracks_WIP_batch2.pkl', 'output/tmp/tracks_WIP_batch3.pkl', 'output/tmp/tracks_WIP_batch4.pkl', 'output/tmp/tracks_WIP_batch5.pkl', 'output/tmp/tracks_WIP_batch6.pkl', 'output/tmp/tracks_WIP_batch7.pkl', 'output/tmp/tracks_WIP_batch8.pkl'])
    print(f"Hay {len(tracks)} canciones extraídas")

    track_ids = [track['id'] for track in tracks]
    print(f"Hay {len(track_ids)} track_ids")
    track_ids = list(set(track_ids))
    print(f"De los cuales {len(track_ids)} track_ids son únicos")

    # Convert track_ids to a set for faster lookup
    track_ids_set = set(track_ids)

    # Use the set for membership checks in the list comprehension
    tracks_clean = [track for track in tracks if track['id'] in track_ids_set]    
    
    print(f"Guardando {len(tracks_clean)} canciones únicas")
    with open('output/raw/tracks.pkl', 'wb') as f:
        pickle.dump(tracks_clean, f)
    
    tracks_df = pd.DataFrame(tracks_clean)
    tracks_df.to_csv('output/raw/tracks.csv', index=False)

    # track_popularity = extraer_popularidad_de_tracks(track_ids)
    with open('output/raw/track_popularity.pkl', 'rb') as f:
        track_popularity = pickle.load(f)
    
    print(f"Hay {len(track_popularity)} canciones con popularidad extraída")

    track_ids = [track['id'] for track in track_popularity]
    
    track_audio_features = extraer_audio_features_de_tracks(track_ids)
    print(f"Hay {len(track_audio_features)} canciones con features de audio extraídas")

    artist_features = extraer_artist_features(top_artist_ids)
    print(f"Hay {len(artist_features)} artistas con géneros y seguidores extraídos")

    print("---Datos crudos extraídos de Spotify---")

    print("---Integración de datos de Spotify en un único DataFrame ---")
    
    # POR SI TENEMOS MUCHOS FILES (de interrupciones en la extracción de datos, tracks1.pkl, tracks2.pkl, tracks3.pkl, etc.)
    track_info_files = ['output/raw/tracks.pkl']
    df_track_info = load_and_concatenate_data(track_info_files)
    print("Cargamos las canciones")

    track_features_files = ['output/raw/track_features.pkl']
    df_track_features = load_and_concatenate_data(track_features_files)
    print("Cargamos las features de audio")

    track_popularity_files = ['output/raw/track_popularity.pkl']
    df_track_popularity = load_and_concatenate_data(track_popularity_files)
    print("Cargamos la popularidad de las canciones")

    artist_genres_file = ['output/raw/artist_genres.pkl']
    df_artist_genres = load_and_concatenate_data(artist_genres_file)
    print("Cargamos los géneros y seguidores de los artistas")  

    # Merge the dataframes keeping only the tracks that have all the information (no missing values)
    df = df_track_info.merge(df_track_features, on='id', how='inner')
    print("Tracks + features")
    df = df.merge(df_track_popularity, on='id', how='inner')
    print("Tracks + features + popularity")
    df = df.merge(df_artist_genres, left_on='artist_id', right_on='id', how='inner')
    print("Tracks + features + popularity + artist_genres")

    # Eliminamos columnas duplicadas
    df = df.drop(columns=['id_y','name_y','song_name','artist_name'])

    # Renombramos columnas
    df = df.rename(columns={'id_x': 'id', 'name_x': 'title', 'genres': 'artist_genres', 'followers': 'artist_followers'})

    # Ponemos la popularity al final
    popularity = df.pop('popularity')
    df['popularity'] = popularity

    # Save the dataframe to a file
    df.to_csv('output/dataset_spotify.csv', index=False)
    print("---Dataset guardado en output/dataset_spotify.csv---")

    # Extraemos datos de lastfm de las canciones
    lastfm = extraer_lastfm_de_tracks(df)

    # Añadimos los datos de lastfm al dataframe de Spotify
    df['lastfm_listeners'], df['lastfm_playcounts'] = zip(*lastfm)

    # Volvemos a poner la popularity al final
    popularity = df.pop('popularity')
    df['popularity'] = popularity

    # Guardamos el dataframe con los datos de lastfm
    df.to_csv('output/dataset_spotify_lastfm.csv', index=False)
    print("---Dataset con Last.fm guardado en output/dataset_spotify_lastfm.csv---")

    
    
    
    

                                             