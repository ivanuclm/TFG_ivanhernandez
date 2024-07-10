import pickle
import time
import api.auth as auth
from spotipy.exceptions import SpotifyException

def get_artist_features(artist_ids, batch_size=50, require_sp=True):
    """
    Obtener los géneros de los artistas de Spotify y su número de seguidores.

    Args:
        artist_ids (list): Lista de IDs de artistas en Spotify. Ejemplo: ['12Chz98pHFMPJEknJQMWvI', '06HL4z0CvFAxyc27GXpf02'] (Son los IDs de Muse y Taylor Swift, respectivamente. 
        Puedes buscarlos así: https://open.spotify.com/artist/12Chz98pHFMPJEknJQMWvI )
        batch_size (int): Tamaño de los lotes de artistas a procesar. Por defecto, 50.
        require_sp (bool): Si es True, se crea una nueva instancia de Spotipy. Por defecto, True.

    Returns:
        artist_genres_list (list): Lista de diccionarios con la información de los artistas extraída. Cada diccionario tiene las siguientes claves:
            - 'id': ID del artista en Spotify.
            - 'name': Nombre del artista.
            - 'genres': Lista de géneros del artista.
            - 'followers': Número de seguidores del artista.
    """
        
    if require_sp:
        sp = auth.get_spotipy()
    
    print(f"----- Extraemos los géneros y el número de seguidores de {len(artist_ids)} artistas -----")

    artist_genres_list = []
    for i in range(0, len(artist_ids), batch_size):
        batch = artist_ids[i:i+batch_size]
        batch = [str(id) for id in batch]
        try:
            artists = sp.artists(batch)
            if artists is not None and 'artists' in artists:
                for artist in artists['artists']:
                    if artist is not None:
                        artist_genres_list.append({
                            'id': artist['id'],
                            'name': artist['name'],
                            'genres': artist['genres'],
                            'followers': artist['followers']['total'],
                        })
            else:
                print(f"Error: sp.artists returned None for batch {i//batch_size}")
        except SpotifyException as e:
            if e.http_status == 429:
                print(f"Rate limit exceeded, sleeping for {e.headers['Retry-After']} seconds")
                time.sleep(int(e.headers['Retry-After']))
                continue
            else:
                print(f"Error processing batch {i//batch_size}: {e}")
        except Exception as e:
            print(f"Error processing batch {i//batch_size}: {e}")
        
        print(f"\033[K\tGéneros y seguidores extraídos para {len(artist_genres_list)}/{len(artist_ids)} artistas.", end='\r')

        with open('output/tmp/artist_genres_WIP.pkl', 'wb') as f:
            pickle.dump(artist_genres_list, f)

    print(f"\n---SUCCESS: Procesados {len(artist_genres_list)} artistas---\n")

    return artist_genres_list


if __name__=='__main__':
    # Set up Spotipy client
    sp = auth.get_spotipy()

    # Load the track_info_list from a file
    # TODO: Change this to the artists from the tracks that we have, not all the 2500 artists.
    with open('input/top_artist_ids.pkl', 'rb') as f:
        artist_ids = pickle.load(f)
    
    # Get artist genres
    artist_genres = get_artist_features(artist_ids, require_sp=False)

    # Save the artist_genres to a file
    with open('output/raw/artist_genres.pkl', 'wb') as f:
        pickle.dump(artist_genres, f)

    
    
