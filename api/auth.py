import requests
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pylast


def get_spotipy():
    """
    Devuelve una instancia del cliente Spotipy.

    Returns:
        spotipy.Spotify: Una instancia del cliente Spotipy.
    """

    CLIENT_ID = '99d12a50c17249ef8824489e594ae609'
    CLIENT_SECRET = '7613df2e33a74126bf5daa698c67e358'

    # CLIENT_ID = 'dcb2ed7f6ff64c5692e3ebdafd1c93e4'
    # CLIENT_SECRET = 'a051ab216b3f47c08770b96be7b73525'

    # CLIENT_ID = '5079248b197c473a8da943cb6c634f08'
    # CLIENT_SECRET = '0bf61fa9564c4a93963d5c979b4d0417'

    client_credentials_manager = SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    # Disable automatic retries
    sp._session = requests.Session()
    sp._session.mount('https://', requests.adapters.HTTPAdapter(max_retries=0))   

    return sp


def get_lastfm():
    """
    Devuelve una instancia de la red de Last.fm.

    Returns:
        pylast.LastFMNetwork: Una instancia de la red de Last.fm.
    """
    # API_KEY = '8d58da1f92cf6bfc3b52e2e8b63c1a79'
    # API_SECRET = '2a2b3c5c3880fff8ebe357ce51dcce3c'

    # username = "respetador"
    # password_hash = pylast.md5("OriginOfSymmetry2001*")
    # password_hash = "6baa0d634ca149ac1da41cfb5008d104"


    API_KEY = '91ed68c0cddd90645a3ce5ee0f361f8e'
    API_SECRET = '82d1ad1c170c9ce8a78060d17867cf0f'

    username = "andreasaez"
    password_hash = pylast.md5("RvCfqH.a884DY.F")

    network = pylast.LastFMNetwork(api_key=API_KEY, api_secret=API_SECRET, username=username, password_hash=password_hash)
    
    return network
