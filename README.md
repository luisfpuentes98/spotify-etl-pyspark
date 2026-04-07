# 🎵 Spotify Data Analytics: Pipeline ETL End-to-End y Power BI

> **Diseño y ejecución de un pipeline de datos escalable (PySpark) y análisis visual (Power BI) basado en el ecosistema musical de Spotify.**

## 📌 Contexto del Proyecto y Naturaleza de los Datos
Este proyecto nace como una extensión de una investigación académica, evolucionando hacia una **Prueba de Concepto (PoC) de Ingeniería de Datos**. 

**Transparencia de los datos:** Se utilizó el dataset público "Most Streamed Spotify Songs 2023" (aprox. 1,000 registros). El objetivo principal de este repositorio no es el procesamiento de *Big Data* en términos de volumen, sino **demostrar el dominio de herramientas y arquitecturas escalables**. Se aplicó *PySpark* y modelado dimensional simulando un entorno de producción, garantizando que el pipeline creado sea capaz de procesar millones de registros si se conecta a un clúster real (ej. Databricks o AWS EMR).

## 📌 Resumen del Proyecto
El análisis de tendencias musicales requiere integrar datos multiplataforma. Este proyecto demuestra la capacidad de construir una arquitectura "End-to-End": desde la extracción y limpieza automatizada, hasta la creación de Data Marts estratégicos y su visualización. 

A través de un enfoque de **Ingeniería de Software**, el panel interactivo final transforma datos crudos en *insights* accionables sobre el comportamiento de la industria.

## 🛠️ Stack Tecnológico y Metodología
* **Herramientas Principales:** Python, PySpark, Pandas, Power BI.
* **Arquitectura de Software:** Código modular estructurado en `src/` aplicando buenas prácticas de la industria (Clean Code, Type Hinting, y Logging de ejecución).
* **Extracción y Limpieza (ETL):** Uso de PySpark para limpieza de caracteres con expresiones regulares (Regex) y estrategias de imputación de nulos (medias aritméticas).
* **Modelado de Datos:** Generación de 3 *Data Marts* analíticos independientes (`Popularidad`, `Energia`, `Caracteristicas`).
* **Lógica de Negocio:** Agrupaciones multiplataforma (Spotify vs. Apple Music) y exportación automatizada mediante un *Bypass* a Pandas para compatibilidad en sistemas locales.

## 💡 Descubrimientos Clave (Key Insights)

1. **La Cúspide de los Billones (El "Big 3"):** Al analizar el Top 10 global, el modelo revela un fuerte oligopolio en la cima liderado por **The Weeknd (14.1B), Taylor Swift (14.0B) y Ed Sheeran (13.9B)**. Estos tres artistas establecen una brecha masiva frente al resto del catálogo, superando por más del doble el volumen de reproducciones de leyendas que cierran el Top 10, como Eminem (6.1B).
2. **El Estándar del Éxito Comercial:** Se identificó un patrón paramétrico claro para la viralidad: las pistas con un índice de bailabilidad alto (>60%) acaparan por sí solas el **63.25% del volumen global de escuchas**. Si a esta métrica se le suma un nivel de energía superior al 50%, el segmento engloba casi el **75% del mercado actual**, demostrando que el consumidor moderno prioriza ritmos dinámicos frente a baladas acústicas.
3. **Catálogo Histórico y la Retención en Playlists:** El análisis cruzado demuestra que la longevidad en la industria depende de la inclusión en Listas de Reproducción. Artistas sin un volumen masivo de lanzamientos en 2023, como Eminem, Coldplay o Arctic Monkeys, logran mantenerse firmes dentro del Top 10 de retención debido a que sus pistas clásicas están presentes en más de 80,000 *playlists* globales, asegurando un flujo de ingresos pasivo y constante.

## 📸 Vista del Dashboard Ejecutivo Pagina 1
<img width="1277" height="715" alt="dashboard_principal_pag1" src="https://github.com/user-attachments/assets/a619634e-883e-489d-9f41-8ffb03eb7f55" />

## 📸 Vista del Dashboard Ejecutivo Pagina 1
<img width="1277" height="710" alt="dashboard_principal_pag2" src="https://github.com/user-attachments/assets/1b1fba2d-08e9-489d-a2c5-e9c003d2bf64" />

## 📸 Arquitectura del Pipeline ETL
<img width="5198" height="3859" alt="Arquitectura_del_Pipeline" src="https://github.com/user-attachments/assets/05293258-3082-4b18-9796-659faa6f2ad6" />

## ⚙️ Muestra de Código: Pipeline PySpark (Extract, Transform & Load)
Para garantizar la escalabilidad, la lógica matemática y la limpieza se aislaron en módulos independientes. Aquí se muestra un extracto del flujo de procesamiento principal:

```python
import logging
from pyspark.sql.functions import col, regexp_replace, avg
from pyspark.sql.types import IntegerType

# 1. Función de Limpieza e Imputación Dinámica (Módulo Transformaciones)
def limpiar_datos(df_raw):
    # Limpieza de caracteres especiales (comas en números)
    cols_con_comas = ["in_deezer_playlists", "in_shazam_charts"]
    for c in cols_con_comas:
        df_raw = df_raw.withColumn(c, regexp_replace(col(c), ",", "").cast(IntegerType()))

    # Imputación de nulos calculando la media de la columna en tiempo real
    promedio_row = df_raw.select(avg(col("streams"))).first()
    if promedio_row and promedio_row[0] is not None:
        df_limpio = df_raw.fillna({"streams": int(promedio_row[0])})
        
    return df_limpio.dropDuplicates()

# 2. Orquestación del Pipeline (Módulo Main)
logger.info("Iniciando el pipeline ETL de Spotify...")
try:
    # Extracción
    df_raw = spark.read.option("header", "true").csv("./data/raw/spotify-2023.csv")
    
    # Transformación
    df_limpio = limpiar_datos(df_raw)
    t_popularidad = generar_tabla_popularidad(df_limpio)
    
    # Carga (Bypass local a Pandas para evitar dependencias de Hadoop en Windows)
    t_popularidad.toPandas().to_csv("./data/processed/Analisis_Popularidad.csv", index=False)
    
    logger.info("¡Pipeline ETL ejecutado con éxito!")
    
except Exception as e:
    logger.error(f"El pipeline falló debido a un error: {str(e)}")
