from fastapi import FastAPI, Query
import pandas as pd


app = FastAPI()


# Carga de datos
df_playtime_genre = pd.read_csv(
    'C:/Users/Daniel/Downloads/PI_01_07/Data/Df_funciones/playtime_genre.csv', low_memory=False)
df_sentiment_analysis = pd.read_csv(
    'C:/Users/Daniel/Downloads/PI_01_07/Data/Df_funciones/sentiment_year.csv', low_memory=False)
df_user_for_genre = pd.read_csv(
    'C:/Users/Daniel/Downloads/PI_01_07/Data/Df_funciones/user_for_genre.csv', low_memory=False)
df_users_recommend = pd.read_csv(
    'C:/Users/Daniel/Downloads/PI_01_07/Data/Df_funciones/user_recommend.csv', low_memory=False)
df_recomendacion_juego = pd.read_csv(
    'C:/Users/Daniel/Downloads/PI_01_07/Data/Df_funciones/reviews_clean.csv', low_memory=False)


@app.get('/PlayTimeGenre')
# 1 FUNCION PLAYTIMEGENRE
async def PlayTimeGenre(genre: str):

    # Creo un dataset solo con el listado de datos con el genero brindado
    df_genero = df_playtime_genre[df_playtime_genre['genres'].str.lower(
    ) == genre.lower() if isinstance(genre, str) else None]

    # Si el df está vacio, es porque no encontró informacion para el genero ingreado
    if df_genero.empty:
        return {f"No hay informacion para el genero {genre}": None}

    if not df_genero['playtime_forever'].empty:

        max_hours = df_genero.loc[df_genero['playtime_forever'].idxmax(
        ), 'date']

        return {f"Año de lanzamiento con mayor horas de juego para el genero {genre}": max_hours}
    else:
        return {f"No hay informacion para el genero {genre}": None}


@app.get('/UserForGenre}')
async def UserForGenre(genre: str | None = None):

    genre_df = df_user_for_genre[df_user_for_genre['genres'].str.lower(
    ) == genre.lower() if isinstance(genre, str) else None]

    if genre_df.empty:
        return {"Usuario con mas horas jugadas para el genero": None, "Horas jugadas por año": {}}

    if not genre_df.empty:
        user_playtime_sum = genre_df.groupby('user_id')['horas'].sum()

        max_horas = user_playtime_sum.idxmax()

        usuario = genre_df[genre_df['user_id'] == max_horas]

        horas_año = dict(zip(usuario['date'], usuario['horas']))

        return {f"Usuario con mas horas jugadas para el genero {genre}": max_horas, "Horas jugadas por año": horas_año}


@app.get('/UsersRecommend')
async def UsersRecommend(year: int):

    # Verificar si hay revisiones para el año dado
    if not df_users_recommend.empty:
        # Filtrar las revisiones para el año dado y recomendaciones positivas/neutrales
        recomendaciones = df_users_recommend[df_users_recommend['posted'] == str(
            year)]

        # Ordenar en orden descendente por la cantidad de recomendaciones
        recomendaciones = recomendaciones.sort_values(
            'análisis_sentimiento', ascending=False)

        # Crear una única línea de resultado
        resultado = {
            "Puesto 1": recomendaciones.iloc[0]['item_name'],
            "Puesto 2": recomendaciones.iloc[1]['item_name'],
            "Puesto 3": recomendaciones.iloc[2]['item_name']
        }

        return resultado


@app.get('/UsersNotRecommend')
def UsersNotRecommend(year):

    # Verificar si hay revisiones para el año dado
    if not df_users_recommend.empty:
        # Filtrar las revisiones para el año dado y recomendaciones positivas/neutrales
        recomendaciones = df_users_recommend[df_users_recommend['posted'] == str(
            year)]

        # Ordenar en orden descendente por la cantidad de recomendaciones
        recomendaciones = recomendaciones.sort_values(
            'análisis_sentimiento', ascending=True)

        # Crear una única línea de resultado
        resultado = {
            "Puesto 1": recomendaciones.iloc[0]['item_name'],
            "Puesto 2": recomendaciones.iloc[1]['item_name'],
            "Puesto 3": recomendaciones.iloc[2]['item_name']
        }

        return resultado


@app.get('/sentiment_analysis')
def sentiment_analysis(year: int):
    year = str(year)

    filtered_reviews = df_sentiment_analysis[df_sentiment_analysis['date'] == year]

    sentiment_counts = filtered_reviews['análisis_sentimiento'].value_counts(
    ).to_dict()

    result = {
        'Negative': sentiment_counts.get(0, 0),
        'Neutral': sentiment_counts.get(1, 0),
        'Positive': sentiment_counts.get(2, 0)
    }

    return result
