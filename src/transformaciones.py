from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_replace, avg, sum, max, min, mode
from pyspark.sql.types import LongType, IntegerType

def limpiar_datos (df_raw: DataFrame) -> DataFrame:
    """Aplica filtros de calidad, casteo de tipos e imputacion de nulos"""

    #1. Filtro de regex y casteo
    # Con la columna streams
    df = df_raw.filter(col("streams").rlike("^[0-9]+$")) # Para que solo tome datos numericos
    df = df.withColumn("streams", col("streams").cast(LongType()))

    #2. Limpieza de las comas 
    cols_con_comas = ["in_deezer_playlists", "in_shazam_charts"]
    for c in cols_con_comas:
        df = df.withColumn(c, regexp_replace(col(c), ",", "").cast(IntegerType()))

    #3. Imputacion de textos 
    dict_textos = {"track_name": "Unknown", "artist(s)_name": "Unknown", "key": "Unknown"}
    df = df.fillna(dict_textos)

    # 4. Imputación de números (Media)
    cols_numericas = [
        "streams", "bpm", "danceability_%", "valence_%", "energy_%",
        "acousticness_%", "instrumentalness_%", "liveness_%", "speechiness_%",
        "in_spotify_playlists", "in_spotify_charts", "in_apple_playlists",
        "in_apple_charts", "in_deezer_playlists", "in_deezer_charts", "in_shazam_charts"
    ]

    # Calculamos promedio de columna y rellenamos datos nulos
    for columna in cols_numericas:
        promedio_row = df.select(avg(col(columna))).first()
        if promedio_row and promedio_row[0] is not None:
            df = df.fillna({columna: int(promedio_row[0])})
            
    return df.dropDuplicates()

def generar_tabla_energia(df: DataFrame) -> DataFrame:
    """Genera la Tabla 1: Análisis de Modo y Energía agrupado por artista."""
    return df.groupBy("artist(s)_name").agg(
        mode("mode").alias("most_common_mode"),
        avg("energy_%").alias("average_energy"),
        max("energy_%").alias("max_energy_%"),
        min("energy_%").alias("min_energy_%"),
        avg("valence_%").alias("average_valence")
    )

def generar_tabla_popularidad(df: DataFrame) -> DataFrame:
    """Genera la Tabla 2: Popularidad multiplataforma por canción."""
    return df.groupBy("track_name", "artist(s)_name", "released_year").agg(
        sum("streams").alias("total_streams"),
        (sum("in_spotify_playlists") + sum("in_apple_playlists") + sum("in_deezer_playlists")).alias("total_playlists"),
        (sum("in_spotify_charts") + sum("in_apple_charts") + sum("in_deezer_charts") + sum("in_shazam_charts")).alias("total_charts")
    )

def generar_tabla_caracteristicas(df: DataFrame) -> DataFrame:
    """Genera la Tabla 3: Perfil acústico promedio por canción."""
    return df.groupBy("track_name", "artist(s)_name", "released_year").agg(
        avg("bpm").alias("average_bpm"),
        avg("acousticness_%").alias("average_acousticness"),
        avg("instrumentalness_%").alias("average_instrumentalness"),
        avg("liveness_%").alias("average_liveness"),
        avg("speechiness_%").alias("average_speechiness"),
        mode("key").alias("most_common_key"),
        max("danceability_%").alias("max_danceability_%")
    )

