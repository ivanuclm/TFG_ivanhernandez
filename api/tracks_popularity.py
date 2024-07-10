import pickle
import time
import spotipy
import api.auth as auth
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.exceptions import SpotifyException
import requests

def get_track_popularity(track_ids, batch_size=50, require_sp=True):
    """
    Obtener la popularidad actual de las canciones en Spotify.

    Args:
        track_ids (list): Lista de IDs de canciones en Spotify. Ejemplo: ['5zA8vzDGqPl2AzZkEYQGKh', '2WfaOiMkCvy7F5fcp2zZ8L'] (Son los IDs de "Uptown Girl" de Billy Joel y "Take on Me" de a-ha, respectivamente. Puedes buscarlos así: https://open.spotify.com/track/5zA8vzDGqPl2AzZkEYQGKh ) 
    """
    track_popularity_list = []
    # # track_popularity_list loads track_popularity_dupes_WIP.pkl and reads track_ids from the 205550 tracks, as those are already extracted
    # with open('output/tmp/track_popularity_dupes_WIP_1.pkl', 'rb') as f:
    #     track_popularity_list = pickle.load(f)

    # track_ids = list(set(track_ids) - set([track['id'] for track in track_popularity_list]))
    
    if require_sp:
        sp = auth.get_spotipy()
    print(f"\nExtrayendo la popularidad de {len(track_ids)} tracks\n")

    

    for i in range(0, len(track_ids), batch_size):
        batch = track_ids[i:i+batch_size]
        try:
            tracks = sp.tracks(batch)
            for track in tracks['tracks']:
                track_popularity_list.append({
                    'id': track['id'],
                    'name': track['name'],
                    'artist_id': track['artists'][0]['id'],
                    'artist': track['artists'][0]['name'],
                    'album_id': track['album']['id'],
                    'album': track['album']['name'],
                    'popularity': track['popularity'],
                    'preview_url': track['preview_url']  # Get the track's preview URL
                })
            time.sleep(1)  # To avoid rate limits
        except SpotifyException as e:
            if e.http_status == 429:
                retry_after = int(e.headers['Retry-After'])
                hours, remainder = divmod(retry_after, 3600)
                minutes, seconds = divmod(remainder, 60)
                print(f"\n---Rate limit exceeded, esperamos {hours} horas, {minutes} minutos, y {seconds} segundos---\n")
                time.sleep(retry_after)
                continue
            else:
                print(f"\nError procesando batch {i//batch_size}: {e}\n")
        except Exception as e:
            print(f"\nError procesando batch {i//batch_size}: {e}\n")
        
        print(f"\033[K\tPopularidad extraída para {len(track_popularity_list)}/{len(track_ids)} canciones.", end='\r')

        with open('output/tmp/track_popularity_dupes_WIP.pkl', 'wb') as f:
            pickle.dump(track_popularity_list, f)

    print(f"\n---SUCCESS: Popularidad extraída para {len(track_popularity_list)} tracks---\n")

    return track_popularity_list


def remove_duplicates(track_popularity):
    # Remove duplicates on artist_name and song_name, keeping the most popular song
    track_best_popularity = {}
    for track in track_popularity:
        #on artist_name and song_name
        key = (track['artist_name'], track['song_name'])
        if key not in track_best_popularity:
            track_best_popularity[key] = track
        else:
            if track['popularity'] > track_best_popularity[key]['popularity']:
                track_best_popularity[key] = track
     
    return list(track_best_popularity.values())

if __name__=='__main__':
    # Set up Spotipy client
    sp = auth.get_spotipy()

    # # # Load the track_info_list from a file
    with open('output/tmp/tracks_from_artists_batch6_WIP.pkl', 'rb') as f:
        track_info_list = pickle.load(f)
    
    with open('Data/tracks_from_artists_batch7_WIP.pkl', 'rb') as f:
        track_info = pickle.load(f)
        track_info_list = track_info_list + track_info
    
    with open('Data/tracks_from_artists_batch8.pkl', 'rb') as f:
        track_info = pickle.load(f)
        track_info_list = track_info_list + track_info

    # # # Extract the unique track IDs from the track_info_list
    track_ids = list(set([track['id'] for track in track_info_list if isinstance(track['id'], str)]))

    # # # Get track popularity and other data for all tracks
    track_popularity = get_track_popularity(track_ids, batch_size=50, require_sp=False)

    # with open('Data/track_popularity_dupes.pkl', 'wb') as f:
    #     pickle.dump(track_popularity, f)

    # with open('Data/track_popularity_dupes.pkl', 'rb') as f:
    #     track_popularity = pickle.load(f)
    
    # Remove duplicates on artist_name and song_name, keeping the most popular song
    track_best_popularity = remove_duplicates(track_popularity)

    # Save the track_best_popularity to a file
    with open('Data/track_popularity.pkl', 'wb') as f:
        pickle.dump(track_best_popularity, f)