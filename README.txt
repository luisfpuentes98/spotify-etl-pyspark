# 🎵 Pipeline ETL & Data Analytics: Spotify Most Streamed Songs 2023

## 📌 Visión General
Este proyecto es una solución **End-to-End** de procesamiento de datos masivos. Se construyó un pipeline ETL utilizando **PySpark** para extraer, limpiar y transformar el set de datos "Most Streamed Spotify Songs 2023", generando Data Marts estratégicos que luego fueron consumidos y visualizados en **Power BI** para la toma de decisiones.

El proyecto demuestra habilidades en procesamiento distribuido (Local Mode), limpieza de datos con expresiones regulares, imputación de nulos y modelado visual para inteligencia de negocios.

## 🏗️ Arquitectura del Proyecto

```mermaid
graph TD
    %% Estilos
    classDef source fill:#f9f,stroke:#333,stroke-width:2px;
    classDef process fill:#bbf,stroke:#333,stroke-width:2px;
    classDef output fill:#bfb,stroke:#333,stroke-width:2px;
    classDef script fill:#eee,stroke:#333,stroke-width:1px,stroke-dasharray: 5 5;

    %% Nodos de Origen
    A[(Raw Data<br>spotify-2023.csv)]:::source

    %% Nodos de Procesamiento (PySpark)
    subgraph Transformación de Datos [Pipeline PySpark ETL]
        B(Limpieza y Casteo<br>Regex & Relleno Nulos):::process
        C{Agrupación de<br>Data Marts}:::process
    end

    %% Nodos de Scripts
    subgraph Estructura Modular [src/]
        S1[utils.py<br>Spark Session]:::script
        S2[transformaciones.py<br>Lógica de Negocio]:::script
        S3[main.py<br>Orquestador]:::script
    end

    %% Nodos de Salida
    D[(Tabla 1:<br>Análisis Energía)]:::output
    E[(Tabla 2:<br>Análisis Popularidad)]:::output
    F[(Tabla 3:<br>Características Musicales)]:::output

    %% Conexiones
    A -->|Extracción| B
    B -->|Transformación| C
    C -->|Carga (Pandas Bypass)| D
    C -->|Carga (Pandas Bypass)| E
    C -->|Carga (Pandas Bypass)| F

    S3 -.->|Importa| S1
    S3 -.->|Importa| S2
    S3 -.->|Ejecuta| B