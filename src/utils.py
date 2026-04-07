import logging
from pyspark.sql import SparkSession

def get_logger(app_name: str) -> logging.Logger:
    """Configura un logger profesional para mostrar mensajes en consola con formato de servidor."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    return logging.getLogger(app_name)

def create_spark_session(app_name: str) -> SparkSession:
    """Inicializa y devuelve una sesión local de Spark."""
    return SparkSession.builder \
        .master("local[*]") \
        .appName(app_name) \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()