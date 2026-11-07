"""Configuración central: rutas del data lake y parámetros del proyecto.

El contrato entre capas son estas RUTAS y los ESQUEMAS de las tablas (ver README).
Cambiar de disco local a HDFS o S3 solo requiere cambiar OSIPTEL_LAKE_URI.
"""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"                       # originales intactos (no se versionan)
STAGING = ROOT / "data" / "staging"                # XLSX convertidos a CSV UTF-8 para Spark
EVIDENCIA = ROOT / "docs" / "evidencia"            # JSON de calidad, tiempos, métricas

# Destino del lake: local por defecto; p. ej. hdfs:///user/grupo11/osiptel o s3a://bucket/osiptel
LAKE = os.environ.get("OSIPTEL_LAKE_URI", (ROOT / "data" / "lake").as_uri())

FUENTES = {
    "robos": RAW / "robos",          # F1 · 9.1. EQUIPOS SUSTRAIDOS PERDIDOS O RECUPERADOS.xlsx
    "reclamos": RAW / "reclamos",    # F2 · 10.1 RECLAMOS PRESENTADOS.xlsx
    "cobertura": RAW / "cobertura",  # F3 · Cobertura móvil por empresa operadora.csv
}

OPERADORAS_PRINCIPALES = ["BITEL", "CLARO", "ENTEL", "MOVISTAR"]
PERIODO_COBERTURA = "2023-03-01"     # único periodo publicado de F3
MATERIAS_CALIDAD = ["CALIDAD E IDONEIDAD EN LA PRESTACION DEL SERVICIO", "FALTA DE SERVICIO"]


def ruta(zona, tabla):
    """Ruta lógica de una tabla dentro del lake: <LAKE>/<zona>/<tabla>."""
    return f"{LAKE.rstrip('/')}/{zona}/{tabla}"
