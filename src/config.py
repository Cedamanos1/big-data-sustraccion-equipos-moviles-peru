"""Configuración central: rutas del data lake y parámetros del proyecto.

El contrato entre capas son estas RUTAS y los ESQUEMAS de las tablas (ver README).
Cambiar de disco local a HDFS o S3 solo requiere cambiar OSIPTEL_LAKE_URI.
"""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
# HOME = carpeta de trabajo con data/ y docs/. Por defecto es la raíz del repositorio; para una demostración
# aislada (sin tocar los datos ni la evidencia reales) se apunta a otra: export OSIPTEL_HOME=/ruta/demo
HOME = Path(os.environ.get("OSIPTEL_HOME", ROOT)).resolve()
RAW = HOME / "data" / "raw"                       # originales intactos (no se versionan)
STAGING = HOME / "data" / "staging"                # XLSX convertidos a CSV UTF-8 para Spark
EVIDENCIA = HOME / "docs" / "evidencia"            # JSON de calidad, tiempos, métricas
TABLERO = HOME / "docs" / "index.html"             # capa de consumo

# Despliegue en la nube (AWS): un bucket S3 por zona. Si no se definen estas variables, todo sigue local.
#   OSIPTEL_S3_BRONZE  originales (raw/) y convertidos (staging/)
#   OSIPTEL_S3_SILVER  SILVER, QUARANTINE y bitácora (_control/)
#   OSIPTEL_S3_GOLD    GOLD, modelo exportado, evidencia y tablero (consumo/)
S3_BRONZE = os.environ.get("OSIPTEL_S3_BRONZE", "")
S3_SILVER = os.environ.get("OSIPTEL_S3_SILVER", "")
S3_GOLD = os.environ.get("OSIPTEL_S3_GOLD", "")
REGION = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-2"))
ES_NUBE = bool(S3_BRONZE and S3_SILVER and S3_GOLD)

# Destino del lake: local por defecto; p. ej. hdfs:///user/grupo11/osiptel o s3a://bucket/osiptel
LAKE = os.environ.get("OSIPTEL_LAKE_URI", f"s3a://{S3_SILVER}" if ES_NUBE else (HOME / "data" / "lake").as_uri())

# Streaming (Spark Structured Streaming) entre las fuentes y BRONZE: de dónde lee, dónde recuerda qué ya vio y dónde registra llegadas
RAW_URI = f"s3a://{S3_BRONZE}/raw/" if ES_NUBE else RAW.as_uri() + "/"
CHECKPOINT_URI = f"s3a://{S3_BRONZE}/_checkpoints/llegadas" if ES_NUBE else (HOME / "data" / "checkpoints" / "llegadas").as_uri()
LLEGADAS_URI = f"s3a://{S3_BRONZE}/bronze/archivos_llegados" if ES_NUBE else (HOME / "data" / "lake" / "bronze" / "archivos_llegados").as_uri()

FUENTES = {
    "robos": RAW / "robos",          # F1 · 9.1. EQUIPOS SUSTRAIDOS PERDIDOS O RECUPERADOS.xlsx
    "reclamos": RAW / "reclamos",    # F2 · 10.1 RECLAMOS PRESENTADOS.xlsx
    "cobertura": RAW / "cobertura",  # F3 · Cobertura móvil por empresa operadora.csv
}

OPERADORAS_PRINCIPALES = ["BITEL", "CLARO", "ENTEL", "MOVISTAR"]
PERIODO_COBERTURA = "2023-03-01"     # único periodo publicado de F3
MATERIAS_CALIDAD = ["CALIDAD E IDONEIDAD EN LA PRESTACION DEL SERVICIO", "FALTA DE SERVICIO"]


def ruta(zona, tabla):
    """Ruta lógica de una tabla dentro del lake: <LAKE>/<zona>/<tabla>.
    En la nube, GOLD vive en su propio bucket; SILVER, QUARANTINE y _control, en el bucket de SILVER."""
    if ES_NUBE and zona == "gold":
        return f"s3a://{S3_GOLD}/{zona}/{tabla}"
    return f"{LAKE.rstrip('/')}/{zona}/{tabla}"
