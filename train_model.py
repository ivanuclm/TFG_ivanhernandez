import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
import xgboost
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.model_selection import RandomizedSearchCV
from sklearn.compose import ColumnTransformer
from sklearn.base import BaseEstimator, TransformerMixin
import pickle
import warnings
from sklearn.preprocessing import OneHotEncoder, LabelEncoder
import ast
from sklearn.preprocessing import FunctionTransformer
import datetime as dt
import os

warnings.filterwarnings(action='ignore')

random_state = 19091952

# Definir una clase personalizada para aplicar múltiples funciones en secuencia
class CustomTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, drop_followers=True):
        self.drop_followers = drop_followers
        self.label_encoder = LabelEncoder()
    
    def fit(self, X, y=None):
        # Ajustar el LabelEncoder para la columna tonalidad
        X = create_tonalidad(X)
        self.label_encoder.fit(X['tonalidad'])
        return self
    
    def transform(self, X):
        X = calculate_months_elapsed(X)
        X = convert_duration(X)
        X = create_tonalidad(X)
        X['tonalidad'] = self.label_encoder.transform(X['tonalidad'])
        X = group_genres(X)
        X = calculate_log_playcounts_per_listener(X)
        X = drop_columns(X, self.drop_followers)
        return X
    

def calculate_months_elapsed(df):
    df['release_date'] = pd.to_datetime(df['release_date'])
    df['months_elapsed'] = (dt.datetime.now() - df['release_date']).dt.days / 30
    return df

def convert_duration(df):
    df['duration_s'] = df['duration_ms'] / 1000
    return df

def create_tonalidad(df):
    key_dict = {0: 'Do', 1: 'Do#', 2: 'Re', 3: 'Mib', 4: 'Mi', 5: 'Fa', 6: 'Fa#', 7: 'Sol', 8: 'Sol#', 9: 'La', 10: 'Sib', 11: 'Si'}
    mode_dict = {0: 'm', 1: 'M'}

    key_name = df['key'].map(key_dict)
    mode_name = df['mode'].map(mode_dict)
    df['tonalidad'] = key_name + ' ' + mode_name
    return df

def map_genres(subgenres):
    # Ensure subgenres is a string representation of a list
    if not isinstance(subgenres, str):
        subgenres = str(subgenres)
    
    # Define a list of main genres
    main_genres = ['Pop', 'Rock', 'Hip Hop', 'Indie', 'Country', 'Metal', 'Classical', 'Jazz', 'R&B', 'Reggae', 'Latin', 'Electronic', 'OST']

    # Define a dictionary of main genres and associated subgenres
    genres_dict = {
        'Pop': ['pop rock', 'commercial', 'synthpop', 'contemporary', 'modern', 'pop'],
        'Rock': ['hard rock', 'alternative rock', 'punk rock', 'road', 'garage rock'],
        'Hip Hop': ['rap', 'trap', 'boom bap', 'atl hip hop', 'bases', 'freestyle'],
        'Indie': ['indie rock', 'indie pop', 'indie folk'],
        'Country': ['country rock', 'country pop', 'bluegrass'],
        'Metal': ['heavy metal', 'death metal', 'black metal'],
        'OST' : ['bso', 'bollywood', 'filmi', 'hollywood', 'tollywood', 'soundtrack'],
        'Classical': ['baroque', 'romantic', 'modern classical', 'classic'],
        'Jazz': ['bebop', 'cool jazz', 'free jazz', 'lounge', 'swing', 'jazz'],
        'R&B': ['soul', 'funk', 'neo soul', 'delta blues', 'chicago blues', 'electric blues', 'funk', 'soul'],
        'Reggae': ['ska', 'dancehall', 'dub'],
        'Latin': ['salsa', 'reggaeton', 'latin pop', 'mexicana', 'mariachi', 'mexican', 'spanish', 'latino', 'banda', 'corrido', 'chihuahuense', 'colombian', 'sertanejo', 'arrocha'],
        'Electronic': ['house', 'techno', 'dubstep', 'dance'],
    }

    #ensure subgenres is a string
    # Parse the string into a list
    subgenres_list = ast.literal_eval(subgenres)
    for subgenre in subgenres_list:
        for main_genre, subgenres_in_dict in genres_dict.items():
            # Check if any subgenre in genres_dict is a substring of the subgenre
            if any(sub in subgenre for sub in subgenres_in_dict):
                return main_genre
            # Check if the main genre is in the subgenre
            elif main_genre.lower() in subgenre.lower():
                return main_genre
    return 'Otro'  # Return 'Otro' if no main genre is found

def group_genres(df):
    # Apply the function to the artist_genres column
    main_genre = df['artist_genres'].apply(map_genres)
    df['pop'] = (main_genre == 'Pop').astype(int)
    df['rock'] = (main_genre == 'Rock').astype(int)
    df['hip_hop'] = (main_genre == 'Hip Hop').astype(int)
    df['latin'] = (main_genre == 'Latin').astype(int)

    return df

def calculate_log_playcounts_per_listener(df):
    df['lastfm_listeners'].fillna(0, inplace=True)
    df['lastfm_playcounts'].fillna(0, inplace=True)
    playcounts_per_listener = df['lastfm_playcounts'] / df['lastfm_listeners']
    df['log_playcounts_per_listener'] = np.log1p(playcounts_per_listener.fillna(0))

    return df

# Definir la función para eliminar columnas no deseadas
def drop_columns(df, drop_followers=False):
    columns_to_drop = [
        'id', 'title', 'artist', 'artist_id', 'album', 'album_total_tracks', 'disc_number', 'track_number', 'release_date', 'duration_ms',
        'key', 'mode', 'artist_genres', 'lastfm_listeners', 'lastfm_playcounts'
    ]
    if drop_followers:
        columns_to_drop.extend(['artist_followers', 'log_playcounts_per_listener'])
    df = df.drop(columns=columns_to_drop, errors='ignore')
    return df



### Pipeline
def create_pipeline(model, drop_followers=False):
    ct_columns = ['time_signature']
    ct_ohe = ColumnTransformer(
        transformers=[
            ('one hot encode', OneHotEncoder(handle_unknown='ignore'), ct_columns)
        ], 
        remainder='passthrough'
    )
    
    pipeline = Pipeline(steps=[
        ('custom_transform', CustomTransformer(drop_followers=drop_followers)),
        ('one hot encode', ct_ohe),
        ('scaler', StandardScaler()),
        ('model', model)
    ])
    
    return pipeline



if __name__ == '__main__':

    print('Cargando el dataset...')
    df = pd.read_csv('dataset.csv')

    X = df.drop('popularity', axis=1)
    y = df['popularity']


    # create training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, random_state=random_state)

    print('Creando el modelo...')
    
    xgb = xgboost.XGBRegressor(eta=0.21,
                           gamma = 0.2,
                           max_depth = 10,
                           min_child_weight = 6,
                            subsample = 1,
                            random_state=random_state, 
                            n_jobs=3)

    print(xgb)

    print('Creando el pipeline...')
    xgb_pipeline = create_pipeline(model=xgb, drop_followers=False)
    empty_pipeline = create_pipeline(model=False, drop_followers=False)


    print('Entrenando el modelo...')
    xgb_pipeline.fit(X_train, y_train)

    # crear carpeta
    try:
        os.mkdir('app_files')
    except:
        print('app_files ya existe.')
    
    with open('app_files/xgb.pkl', 'wb') as file:
        pickle.dump(xgb, file)

    with open('app_files/xgb_pipeline.pkl', 'wb') as file:
        pickle.dump(xgb_pipeline, file)

    with open('app_files/empty_pipeline.pkl', 'wb') as file:
        pickle.dump(empty_pipeline, file)

    #Print the model
    print('Modelo completo guardado en app_files/xgb_pipeline.pkl')

    scan = input('¿Desea generar el modelo sin artist_followers ni lastfm? (s/n): ')

    if scan == 's':
        xgb_pipeline = create_pipeline(model=xgb, drop_followers=True)
        xgb_pipeline.fit(X_train, y_train)
        with open('app_files/xgb_pipeline_basic.pkl', 'wb') as file:
            pickle.dump(xgb_pipeline, file)
        
        print('Modelo sin artist_followers ni lastfm guardado en app_files/xgb_pipeline_basic.pkl')
    else:
        print('Modelo sin artist_followers ni lastfm no guardado.')
    
    print('Proceso finalizado.')