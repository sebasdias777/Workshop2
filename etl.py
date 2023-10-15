import pandas as pd
import logging
import json
import psycopg2

def connect_db():
    with open('./dagwor/db_config.json') as f:
        dbfile = json.load(f)
    connection = psycopg2.connect(
        database=dbfile["database"],
        user=dbfile["user"],
        password=dbfile["password"],
        host="192.168.200.1",
        port=5432
    )
    print("Database connection ok")
    return connection 

def read_db():
    conn = connect_db()
    df = pd.read_sql_query('SELECT * FROM grammy', conn)
    print("Base de datos leida")

    return df.to_json(orient='records')

def transform_db(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="read_db")
    json_data = json.loads(str_data)
    data = pd.json_normalize(data=json_data)
    df = data.drop(columns=['title', 'published_at', 'updated_at', 'img', 'workers'])
    # df.rename(columns={ 'winner':'nominated'}, inplace=True)

    
    return df.to_json(orient='records')            

def read_csv():
    spotify_df = pd.read_csv("./dagwor/spotify_dataset.csv", index_col=0)
    print("csv leido")

    return spotify_df.to_json(orient='records')

def transform_csv(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="read_csv")
    json_data = json.loads(str_data)
    datos_csv = pd.json_normalize(data=json_data)

    datos_csv = datos_csv.drop_duplicates(subset="track_id", keep='first', inplace=False)
    datos_csv['duration_min'] = datos_csv['duration_ms'] / 60000
    columns_to_drop = ['duration_ms', 'track_id', 'key', 'loudness', 'mode', 'speechiness',
                           'acousticness', 'instrumentalness', 'liveness', 'tempo', 'time_signature', 'track_genre']
    datos_csv.drop(columns=columns_to_drop, inplace=True) 

    rangos = [0.0, 0.4, 0.6, 1.0]
    etiquetas = ['Negativo', 'Neutro', 'Positivo']
    datos_csv['valence_category'] = pd.cut(datos_csv['valence'], bins=rangos, labels=etiquetas)
    tipos_bailabilidad = ['Baja', 'Media', 'Alta']
    datos_csv['cat_danceability'] = pd.cut(datos_csv['danceability'], bins=rangos, labels=tipos_bailabilidad)
    
    spotify_df = datos_csv.sort_values(by='danceability', ascending=False)
    spotify_df = spotify_df.drop_duplicates(subset=['artists', 'track_name'], keep='first')

    return spotify_df.to_json(orient='records')

def merge(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="transform_db")
    json_data = json.loads(str_data)
    grammy_df = pd.json_normalize(data=json_data)

    str_data = ti.xcom_pull(task_ids="transform_csv")
    json_data = json.loads(str_data)
    spotify_df = pd.json_normalize(data=json_data)

    df = spotify_df.merge(grammy_df, how="inner", left_on=['track_name'], right_on=['nominee'])
    return df.to_json(orient='records')

def load(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="merge")
    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)

    conn = connect_db()
    cur = conn.cursor()
    column_names = df.columns.tolist()
    query = f"""
            INSERT INTO canciones_grammy1({", ".join(column_names)})
            VALUES ({", ".join(["%s"] * len(column_names))})
        """
    for index, row in df.iterrows():
        values = tuple(row)
        cur.execute(query, values)
    conn.commit()
    cur.close()
    conn.close()
    print('Load')
    return df.to_json(orient='records')
    
def store(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="merge")
    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)
    df.to_csv('./dagwor/data.csv')
    print('Store')


