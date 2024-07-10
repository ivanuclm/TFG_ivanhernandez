import streamlit as st
import pandas as pd
import numpy as np
import pickle
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from PIL import Image
import api.auth as auth
from train_model import CustomTransformer
import ast

# @st.cache(allow_output_mutation=True)
def load_pipelines():
    # xgb = pickle.load(open('models/xgb_model.pkl', 'rb'))
    xgb_pipeline = pickle.load(open('model/xgb_model.pkl', 'rb'))
    xgb_pipeline_basic = pickle.load(open('model/xgb_model_basic.pkl', 'rb'))
    return xgb_pipeline, xgb_pipeline_basic


def main():
    sp = auth.get_spotipy()
    # network = auth.get_lastfm()
    # load pipeline object and model
    # preprocessing, model = load_pipeline_and_model()
    xgb_pipeline, xgb_pipeline_basic = load_pipelines()

    prediction = False

    # side bar and title



    predictors = ['artist_followers', 'lastfm_listeners', 'lastfm_playcounts', 'track_number', 'album_total_tracks', 'disc_number', 'duration_s', 'months_elapsed', 'explicit', 'acousticness', 'danceability', 'energy', 'instrumentalness', 'liveness', 'loudness', 'speechiness', 'valence', 'key', 'mode', 'key_mode', 'tempo', 'time_signature', 'pop_or_rap', 'rock_or_metal', 'latin']

    search_query = st.sidebar.text_input("#### Busca una canción por su nombre...", value='...')


    if search_query:
        # Search for songs on Spotify
        results = sp.search(q=search_query, limit=5, type='track')

        for idx, track in enumerate(results['tracks']['items']):
            # Display the song and its actual popularity
            # st.sidebar.markdown(f"""<iframe style="border-radius:12px" src="https://open.spotify.com/embed/track/{track['id']}" width="100%" height="302" frameBorder="0" allowfullscreen="" allow="autoplay; clipboard-write; encrypted-media; fullscreen; picture-in-picture" loading="lazy"></iframe>""", unsafe_allow_html=True)
            st.sidebar.markdown(f"""<iframe style="border-radius:12px" src="https://open.spotify.com/embed/track/{track['id']}" width="100%" height="152" frameBorder="0" allowfullscreen="" allow="autoplay; clipboard-write; encrypted-media; fullscreen; picture-in-picture" loading="lazy"></iframe>""", unsafe_allow_html=True)
            # st.write(f"**{track['name']}** by {track['artists'][0]['name']}")
            st.sidebar.info(f"Popularidad real: {track['popularity']}")

            # Add a button for predicting the song's popularity
            if st.sidebar.button(f"Predecir popularidad para ***{track['name']}***", key=f"button_{track['id']}"):                 # Set the input fields to the values from the song
                # Get artist's information
                id = track['id']
                title = track['name']
                artist = track['artists'][0]['name']
                artist_id = track['artists'][0]['id']
                album = track['album']['name']
                album_total_tracks = track['album']['total_tracks']
                disc_number = track['disc_number']
                track_number = track['track_number']
                explicit = track['explicit']
                release_date = track['album']['release_date']
                duration_ms = track['duration_ms']

                # Get audio features
                audio_features = sp.audio_features(track['id'])[0]
                acousticness = audio_features['acousticness']
                danceability = audio_features['danceability']
                energy = audio_features['energy']
                instrumentalness = audio_features['instrumentalness']
                liveness = audio_features['liveness']
                loudness = audio_features['loudness']
                speechiness = audio_features['speechiness']
                valence = audio_features['valence']
                key = audio_features['key']
                mode = audio_features['mode']
                tempo = audio_features['tempo']
                time_signature = audio_features['time_signature']

                artist_info = sp.artist(track['artists'][0]['id'])
                artist_genres = artist_info['genres'] # assuming the first genre is the main one
                artist_followers = artist_info['followers']['total']

                # Get Last.fm information
                network = auth.get_lastfm()
                lastfm_track_info = network.get_track(track['artists'][0]['name'], track['name'])
                lastfm_listeners = lastfm_track_info.get_listener_count()
                lastfm_playcounts = lastfm_track_info.get_playcount()

                predictors = ['id', 'title', 'artist', 'artist_id', 'album', 'album_total_tracks', 'disc_number', 'track_number', 'release_date', 'duration_ms', 'explicit', 'acousticness', 'danceability', 'energy', 'instrumentalness', 'liveness', 'loudness', 'key', 'mode', 'tempo', 'time_signature',  'speechiness', 'valence', 'artist_genres', 'artist_followers', 'lastfm_listeners', 'lastfm_playcounts']
            
                input_features_search = pd.DataFrame(columns=predictors)
                input_features_search.loc[0] = [id, title, artist, artist_id, album, album_total_tracks, disc_number, track_number, release_date, duration_ms, explicit, acousticness, danceability, energy, instrumentalness, liveness, loudness, key, mode, tempo, time_signature, speechiness, valence, artist_genres, artist_followers, lastfm_listeners, lastfm_playcounts]
            
                # Predict the song's popularity
                # input_features_processed_search = preprocessing.transform(input_features_search)
                prediction = xgb_pipeline.predict(input_features_search)[0]
                # st.progress(prediction/100)
                # st.success(f'Predicción de popularidad: {prediction:.2f}')
                # Print all the features
            
            st.sidebar.write('---')

    st.header('Predicción de popularidad de canciones')
    st.write('Busca una canción en el cuadro de búsqueda de la barra lateral y presiona Enter para ver las opciones. Luego, presiona el botón para predecir la popularidad de la canción escogida.')
    st.write('---')

    if prediction:
        st.header('Resultados')
        st.markdown(f"""<iframe style="border-radius:12px" src="https://open.spotify.com/embed/track/{id}" width="100%" height="352" frameBorder="0" allowfullscreen="" allow="autoplay; clipboard-write; encrypted-media; fullscreen; picture-in-picture" loading="lazy"></iframe>""", unsafe_allow_html=True)
        st.success(f'Predicción de popularidad: {prediction:.2f}')
        st.progress(prediction/100)

        # st.write('---')

        col1, col2 = st.columns(2)

        with col1:          
            st.write('#### Información de la canción')
            st.write(f"**{title}** - {artist}")
            
            col11, col12 = st.columns(2)

            
            st.write(f"**Álbum:** {album}")
            st.write(f"**Duración:** {duration_ms/60000:.0f} minutos  y {duration_ms%60000/1000:.0f} segundos")
            st.write(f"**Lanzamiento:** {release_date}")
            st.write(f"**Géneros del artista:** {artist_genres}")
            st.write(f"**Seguidores del artista:** {artist_followers}")
            st.write(f"**Escuchas en Last.fm:** {lastfm_listeners}")
            st.write(f"**Reproducciones en Last.fm:** {lastfm_playcounts}")

        with col2:
            st.write('#### Características de la canción')
            st.write(f"**Acousticness:** {acousticness:.2f}")
            st.write(f"**Danceability:** {danceability:.2f}")
            st.write(f"**Energy:** {energy:.2f}")
            st.write(f"**Instrumentalness:** {instrumentalness:.2f}")
            st.write(f"**Liveness:** {liveness:.2f}")
            st.write(f"**Loudness:** {loudness:.2f}")
            st.write(f"**Speechiness:** {speechiness:.2f}")
            st.write(f"**Valence:** {valence:.2f}")
            st.write(f"**Key:** {key}")
            st.write(f"**Mode:** {mode}")
            st.write(f"**Tempo:** {tempo:.2f}")
            st.write(f"**Compás:** {time_signature}")
    else:
        st.write('Aún no se ha realizado ninguna predicción.')

        
    # main_genres = ['Pop', 'Rock', 'Hip Hop', 'Indie', 'Country', 'Metal', 'Classical', 'Jazz', 'R&B', 'Reggae', 'Latin', 'Electronic', 'OST']

    # duration_s = st.sidebar.slider('Duración (s)', 0, 600, 1)
    # months_elapsed = st.sidebar.slider('Meses desde el lanzamiento de la canción', 0, 48, 1)

    # explicit = st.sidebar.selectbox('Contiene lenguaje malsonante', ('Sí', 'No'))
    # acousticness = st.sidebar.slider('Acousticness', 0.0, 1.0, 0.01)
    # danceability = st.sidebar.slider('Danceability', 0.0, 1.0, 0.01)
    # energy = st.sidebar.slider('Energy', 0.0, 1.0, 0.01)
    # instrumentalness = st.sidebar.slider('Instrumentalness', 0.0, 1.0, 0.01)
    # liveness = st.sidebar.slider('Liveness', 0.0, 1.0, 0.01)
    # loudness = st.sidebar.slider('Loudness', -60.0, 4.0, 0.1)
    # speechiness = st.sidebar.slider('Speechiness', 0.0, 1.0, 0.01)
    # key = st.sidebar.selectbox('Tono', ('Do', 'Do#', 'Re', 'Re#', 'Mi', 'Fa', 'Fa#', 'Sol', 'Sol#', 'La', 'La#', 'Si'))
    # mode = st.sidebar.selectbox('Modo', ('Mayor', 'menor'))
    # valence = st.sidebar.slider('Valence', 0.0, 1.0, 0.01)
    # tempo = st.sidebar.slider('Tempo', 0.0, 250.0, 0.01)
    # time_signature = st.sidebar.selectbox('Compás', ('3/4', '4/4', '5/4'))

    # artist_followers = st.sidebar.slider("Seguidores del artista", 0, 1000000000, 1000)
    # playcounts_per_listener = st.sidebar.slider("Escuchas en Last.fm por oyente", 0, 1500, 1) 
    # genre = st.sidebar.selectbox('Género del artista', ('Pop', 'Rock', 'Hip Hop', 'Indie', 'Country', 'Metal', 'Classical', 'Jazz', 'R&B', 'Reggae', 'Latin', 'Electronic', 'OST', 'Otros'))

    

    # # assign value to is_pop_or_rap feature
    # if genre in ['Pop', 'Hip Hop/Rap']:
    #     pop_or_rap = 1
    # else:
    #     pop_or_rap = 0

    # if genre in ['Rock', 'Metal']:
    #     rock_or_metal = 1
    # else:
    #     rock_or_metal = 0

    # if genre in ['Latin']:
    #     latin = 1
    # else:
    #     latin = 0

    # if(explicit == 'Sí'):
    #     explicit = 1
    # else:
    #     explicit = 0
    
    # key_dict = {'Do': 0, 'Do#': 1, 'Re': 2, 'Re#': 3, 'Mi': 4, 'Fa': 5, 'Fa#': 6, 'Sol': 7, 'Sol#': 8, 'La': 9, 'La#': 10, 'Si': 11}
    # key_value = key_dict[key]
    # mode_dict = {'Mayor': 1, 'menor': 0}
    # mode_value = mode_dict[mode]

    # #key_mode_value positive if major, negative if minor. this needs key to change from 0-11 to 1-12, and then add the mode value
    # key_value_for_keymode = key_value + 1
    # # feng['key_mode'] + (feng['mode'] == 0) * -2 * feng['key_mode']
    # key_mode_value = key_value_for_keymode + (mode_value == 0) * -2 * key_value_for_keymode
    


    # # create input matrix with user response
    # input_features = pd.DataFrame(columns=predictors)

    # input_features.loc[0] = [artist_followers, lastfm_listeners, lastfm_playcounts, track_number, album_total_tracks, disc_number, duration_s, months_elapsed, explicit, acousticness, danceability, energy, instrumentalness, liveness, loudness, speechiness, valence, key_value, mode_value, key_mode_value, tempo, time_signature, pop_or_rap, rock_or_metal, latin]

    # # create button that generates prediction
    # if st.sidebar.button('Predicción COMPLETA'):
    #     input_features_processed = preprocessing.transform(input_features)
    #     prediction = model.predict(input_features_processed)[0]
    #     st.sidebar.success(f'Predicción de popularidad para los valores escogidos: ***{prediction:.2f}***')

    # if st.sidebar.button('Predicción BÁSICA (sin seguidores)'):
    #     input_features_processed = preprocessing.transform(input_features)
    #     prediction = model.predict(input_features_processed)[0]
    #     st.sidebar.success(f'Predicción de popularidad para los valores escogidos: ***{prediction:.2f}***')

# Add a search bar for finding songs on Spotify

if __name__ == '__main__':
    main()