import streamlit as st
import pandas as pd
import numpy as np
import pickle
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
import api.auth as auth
from train_model import CustomTransformer
import ast
import json

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
    prediction_basic = False
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
                prediction_basic = xgb_pipeline_basic.predict(input_features_search)[0]
                # st.progress(prediction/100)
                # st.success(f'Predicción de popularidad: {prediction:.2f}')
                # Print all the features
            
            st.sidebar.write('---')

    st.header('Predicción de popularidad de canciones')
    st.write('Busca una canción en el cuadro de búsqueda de la barra lateral y presiona Enter para ver las opciones.')
    st.write('Cuando hayas decidido cuál predecir, presiona el botón de abajo de la canción para ver los resultados.')
    st.write('---')

    if prediction:
        if st.button('Limpiar predicción'):
            prediction = False
        st.header('Resultados')
        st.markdown(f"""<iframe style="border-radius:12px" src="https://open.spotify.com/embed/track/{id}" width="100%" height="352" frameBorder="0" allowfullscreen="" allow="autoplay; clipboard-write; encrypted-media; fullscreen; picture-in-picture" loading="lazy"></iframe>""", unsafe_allow_html=True)
        st.success(f'Predicción de popularidad:\n  # {prediction:.2f}')
        st.progress(prediction/100)
        st.error(f'Predicción de popularidad (básica): {prediction_basic:.2f}')
        st.progress(prediction_basic/100)

        # st.write('---')

        col1, col2 = st.columns(2)

        with col1:          
            st.write('#### Información de la canción')
            st.write(f"**{title}** - {artist}")
            
            st.write(f"**Álbum:** {album}")
            st.write("---")
            st.write(f"**Duración:** {duration_ms/60000:.0f} minutos  y {duration_ms%60000/1000:.0f} segundos")
            release_date_spanish = pd.to_datetime(release_date).strftime('%d de %B de %Y')
            st.write(f"**Lanzamiento:** {release_date_spanish}")
            st.write(f"**Explícita:** {'Sí' if explicit else 'No'}")
            st.write(f"**Géneros del artista:** {artist_genres if artist_genres else 'No disponible'}")
            st.write("---")
            st.write(f"**Seguidores del artista:** {artist_followers}")
            st.write(f"**Oyentes en Last.fm:** {lastfm_listeners}")
            st.write(f"**Reproducciones en Last.fm:** {lastfm_playcounts}")

        with col2:
            st.write('#### Características de la canción')
            st.write(f"**Acousticness:** {acousticness:.2f}")
            st.write(f"**Danceability:** {danceability:.2f}")
            st.write(f"**Energy:** {energy:.2f}")
            st.write(f"**Instrumentalness:** {instrumentalness:.2f}")
            st.write(f"**Liveness:** {liveness:.2f}")
            st.write(f"**Loudness:** {loudness:.2f} dB")
            st.write(f"**Speechiness:** {speechiness:.2f}")
            st.write(f"**Valence:** {valence:.2f}")
            key_dict = {0: 'Do', 1: 'Do#', 2: 'Re', 3: 'Re#', 4: 'Mi', 5: 'Fa', 6: 'Fa#', 7: 'Sol', 8: 'Sol#', 9: 'La', 10: 'La#', 11: 'Si'}
            st.write(f"**Key:** {key_dict[key]}")
            mode_dict = {0: 'menor', 1: 'Mayor'}
            st.write(f"**Mode:** {mode_dict[mode]}")
            st.write(f"**Tempo:** {tempo:.2f} bpm")
            time_signature_dict = {0: 'No disponible', 1:'Otro', 3: '3/4', 4: '4/4', 5: '5/4'}
            st.write(f"**Compás:** {time_signature_dict[time_signature]}")
    else:
        st.write('Si lo deseas, puedes introducir manualmente las características de una canción para predecir su popularidad.')
        
        user_id = 'cualquiera'
        user_title = 'cualquiera'
        user_artist = 'cualquiera'
        user_artist_id = 'cualquiera'
        user_album = 'cualquiera'
        user_album_total_tracks = 'cualquiera'
        user_disc_number = 'cualquiera'
        user_track_number = 'cualquiera'
        col3, col4 = st.columns(2)

        with col3:
            user_release_date = st.date_input('Fecha de lanzamiento', value=pd.to_datetime('today'))
            user_explicit_value = st.selectbox('Lenguaje malsonante', ('Sí', 'No'))
            user_tonalidad = st.selectbox('Tonalidad', ('Do m', 'Do# m', 'Re m', 'Re# m', 'Mi m', 'Fa m', 'Fa# m', 'Sol m', 'Sol# m', 'La m', 'La# m', 'Si m', 'Do M', 'Do# M', 'Re M', 'Re# M', 'Mi M', 'Fa M', 'Fa# M', 'Sol M', 'Sol# M', 'La M', 'La# M', 'Si M'))
            user_time_signature = st.selectbox('Compás', ('3/4', '4/4', '5/4'))

            user_artist_genres_input = st.text_input('Géneros del artista (separados por comas)', value='rock, punk, alternative')
            user_duration_s = st.slider('Duración (segundos)', 0, 600, 1)
            user_artist_followers = st.slider("Seguidores del artista", 0, 100000000, 1000)
            user_lastfm_listeners = st.slider("Escuchas en Last.fm", 0, 1000000, 1)
            user_lastfm_playcounts = st.slider("Reproducciones en Last.fm", 0, 1000000, 1)

        with col4:
            # Get audio features
            user_acousticness = st.slider('Acousticness', 0.0, 1.0, 0.01)
            user_danceability = st.slider('Danceability', 0.0, 1.0, 0.01)
            user_energy = st.slider('Energy', 0.0, 1.0, 0.01)
            user_instrumentalness = st.slider('Instrumentalness', 0.0, 1.0, 0.01)
            user_liveness = st.slider('Liveness', 0.0, 1.0, 0.01)
            user_loudness = st.slider('Loudness', -60.0, 4.0, 0.1)
            user_speechiness = st.slider('Speechiness', 0.0, 1.0, 0.01)
            user_valence = st.slider('Valence', 0.0, 1.0, 0.01)

            user_tempo = st.slider('Tempo', 0, 250, 1)

        user_explicit = 0 if user_explicit_value == 'No' else 1

        user_duration_ms = user_duration_s * 1000

        user_key_name = user_tonalidad.split(' ')[0]
        user_mode_name = user_tonalidad.split(' ')[1]
        user_key_dict = {'Do': 0, 'Do#': 1, 'Re': 2, 'Re#': 3, 'Mi': 4, 'Fa': 5, 'Fa#': 6, 'Sol': 7, 'Sol#': 8, 'La': 9, 'La#': 10, 'Si': 11}
        user_key = user_key_dict[user_key_name]
        user_mode_dict = {'m': 0, 'M': 1}
        user_mode = user_mode_dict[user_mode_name]

        user_genres_list = [genre.strip() for genre in user_artist_genres_input.split(',')]
        user_artist_genres = json.dumps(user_genres_list)

        aux1, aux2, aux3 = st.columns([1,2,1])

        with aux2:  # Use the middle column for the button
        
            if st.button('Predecir popularidad de canción personalizada'):
                user_predictors = ['id', 'title', 'artist', 'artist_id', 'album', 'album_total_tracks', 'disc_number', 'track_number', 'release_date', 'duration_ms', 'explicit', 'acousticness', 'danceability', 'energy', 'instrumentalness', 'liveness', 'loudness', 'key', 'mode', 'tempo', 'time_signature',  'speechiness', 'valence', 'artist_genres', 'artist_followers', 'lastfm_listeners', 'lastfm_playcounts']
                
                user_input_features = pd.DataFrame(columns=user_predictors)
                user_input_features.loc[0] = [user_id, user_title, user_artist, user_artist_id, user_album, user_album_total_tracks, user_disc_number, user_track_number, user_release_date, user_duration_ms, user_explicit, user_acousticness, user_danceability, user_energy, user_instrumentalness, user_liveness, user_loudness, user_key, user_mode, user_tempo, user_time_signature, user_speechiness, user_valence, user_artist_genres, user_artist_followers, user_lastfm_listeners, user_lastfm_playcounts]
                
                # Predict the song's popularity
                # input_features_processed_search = preprocessing.transform(input_features_search)
                user_prediction = xgb_pipeline.predict(user_input_features)[0]
                user_prediction_basic = xgb_pipeline_basic.predict(user_input_features)[0]
                # st.progress(prediction/100)
                # st.success(f'Predicción de popularidad: {prediction:.2f}')
                # Print all the features
                # st.header('Resultados de la predicción personalizada')
                st.success(f'Predicción personalizada: {user_prediction:.2f}')
                st.progress(user_prediction/100)
                st.error(f'Predicción personalizada (básica): {user_prediction_basic:.2f}')
                st.progress(user_prediction_basic/100)


# Add a search bar for finding songs on Spotify

if __name__ == '__main__':
    main()