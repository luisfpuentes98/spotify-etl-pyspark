import utils
import transformaciones as tr

def main(): 
    # Inicializar el Logger y Spark
    logger = utils.get_logger("Spotify_ETL")
    logger.info("Iniciando el pipeline ETL de Spotify...")

    spark = utils.create_spark_session("Proyecto_Spotify_2023")

    # Definicion de rutas relativas
    ruta_origen = "./data/raw/spotify-2023.csv"
    ruta_salida = "./data/processed/"

    try:
        # 1. Extraccion
        logger.info(f"Extrayendo datos desde: {ruta_origen}")
        df_raw = spark.read.option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "ISO-8859-1") \
        .csv(ruta_origen)

        #2. Transformacion
        logger.info("Aplicando limpieza de datos e imputacion de nulos...")
        df_limpio = tr.limpiar_datos(df_raw)

        logger.info("Generando Data Marts (Tablas maestras)...")
        t_energia = tr.generar_tabla_energia(df_limpio)
        t_popularidad = tr.generar_tabla_popularidad(df_limpio)
        t_caract = tr.generar_tabla_caracteristicas(df_limpio)

        # 3. Carga (Bypass de Hadoop para Windows)
        logger.info("Exportando resultados a CSV usando Pandas (Local Mode)...")
        
        # Convertimos los DataFrames de Spark a Pandas y guardamos
        t_energia.toPandas().to_csv(f"{ruta_salida}Analisis_Energia.csv", index=False)
        t_popularidad.toPandas().to_csv(f"{ruta_salida}Analisis_Popularidad.csv", index=False)
        t_caract.toPandas().to_csv(f"{ruta_salida}Analisis_Caracteristicas.csv", index=False)

        logger.info("¡Pipeline ETL ejecutado con éxito!")

    except Exception as e:
        logger.error(f"El pipeline falló debido a un error: {str(e)}")

    finally:
        logger.info("Cerrando sesion de Spark. Liberando recursos.")
        spark.stop()

if __name__ == "__main__":
    main()