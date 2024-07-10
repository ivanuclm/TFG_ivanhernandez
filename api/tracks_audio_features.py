import pickle
import time
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import api.auth as auth
from spotipy.exceptions import SpotifyException
def get_track_features(track_ids, batch_size=100, require_sp=True):
    """
    Obtiene las audio_features de las canciones de Spotify.

    Args:
        track_ids (list): Lista de IDs de canciones en Spotify.
        batch_size (int): Tamaño del lote para procesar las canciones.

    Returns:
        track_features_list (list): Lista de diccionarios con las audio_features de las canciones extraídas. Cada diccionario tiene las siguientes claves:
            - 'id': ID de la canción en Spotify.
            - 'danceability': La medida en que una canción es adecuada para bailar.
            - 'energy': La energía de la canción.
            - 'key': La nota tónica de la canción.
            - 'loudness': El volumen general de la canción en decibelios (dB).
            - 'mode': El modo de la canción (mayor o menor).
            - 'speechiness': La probabilidad de que la canción contenga palabras habladas.
            - 'acousticness': La probabilidad de que la canción sea acústica.
            - 'instrumentalness': La probabilidad de que la canción no contenga voz.
            - 'liveness': La probabilidad de que la canción se haya grabado en vivo.
            - 'valence': La positividad de la canción.
            - 'tempo': El tempo de la canción en BPM.
            - 'time_signature': El compás en el que está la obra (4/4, 3/4...).
    """
    if require_sp:
        sp = auth.get_spotipy()
    
    print(f"----- Extraemos las características musicales (audio features)  de {len(track_ids)} canciones (en lotes de {batch_size}) -----")

    # track_features_list = []
    with open('output/tmp/track_features_WIP_1.pkl', 'rb') as f:
        track_features_list = pickle.load(f)
    
    with open('output/tmp/track_features_WIP_2.pkl', 'rb') as f:
        track_features_list_2 = pickle.load(f)

    with open('output/tmp/track_features_WIP_3.pkl', 'rb') as f:
        track_features_list_3 = pickle.load(f)

    track_features_list = track_features_list + track_features_list_2 + track_features_list_3
    # check for dupes on id
    track_features_list = [dict(t) for t in {tuple(d.items()) for d in track_features_list}]
    track_ids = list(set(track_ids) - set([track['id'] for track in track_features_list]))
    
    for i in range(0, len(track_ids), batch_size):
        batch = track_ids[i:i+batch_size]
        try:
            features = sp.audio_features(batch)
            features_index = 0
            for feature in features:
                features_index += 1
                if feature:
                    track_features_list.append({
                        'id': feature['id'],
                        'danceability': feature['danceability'],
                        'energy': feature['energy'],
                        'key': feature['key'],
                        'loudness': feature['loudness'],
                        'mode': feature['mode'],
                        'speechiness': feature['speechiness'],
                        'acousticness': feature['acousticness'],
                        'instrumentalness': feature['instrumentalness'],
                        'liveness': feature['liveness'],
                        'valence': feature['valence'],
                        'tempo': feature['tempo'],
                        # 'duration_ms': feature['duration_ms'], # Ya lo tenemos en track_info_list
                        'time_signature': feature['time_signature']
                    })
                else:
                    print(f"\nNo se ha encontrado las features para la {features_index}a. canción del batch {i//batch_size}\n")
            time.sleep(1)  # To avoid rate limits
        except SpotifyException as e:
            if e.http_status == 429:
                if 'Retry-After' in e.headers:
                    retry_after = int(e.headers['Retry-After'])
                    hours, remainder = divmod(retry_after, 3600)
                    minutes, seconds = divmod(remainder, 60)
                    print(f"\n---Rate limit exceeded, esperamos {hours} horas, {minutes} minutos, y {seconds} segundos---\n")
                else:
                    retry_after = 60  # Default wait time
                    print(f"\n---ERROR, reintentamos en {retry_after} segundos---\n")
                
                time.sleep(retry_after)
                continue
            else:
                print(f"\nError procesando batch {i//batch_size}: {e}\n")
        except Exception as e:
            print(f"\nError processing batch {i//batch_size}: {e}\n")

        print(f"\033[K\tAudio features extraídas para {len(track_features_list)}/{len(track_ids)} canciones.", end='\r')


        with open('output/tmp/track_features_WIP.pkl', 'wb') as f:
            pickle.dump(track_features_list, f)

    print(f"\n---SUCCESS: Extracción audio features de {len(track_features_list)} canciones---\n")

    return track_features_list

if __name__=='__main__':
    # Set up Spotipy client
    sp = auth.get_spotipy()

    # Load the track_info_list from a file
    with open('output/raw/tracks.pkl', 'rb') as f:
        track_info_list = pickle.load(f)

    # Extract the unique track IDs from the track_info_list
    track_ids = list(set([track['id'] for track in track_info_list if isinstance(track['id'], str)]))

    # Get audio features for all tracks
    track_features = get_track_features(track_ids, require_sp=False)

    # Save the track_features to a file
    with open('output/raw/track_features.pkl', 'wb') as f:
        pickle.dump(track_features, f)
    
    
