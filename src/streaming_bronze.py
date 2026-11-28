"""Intermediario FUENTES -> BRONZE con Spark Structured Streaming.

Un `readStream` vigila la carpeta RAW (en la nube, s3://<bronze>/raw/). Cada archivo que llega:
  1. se detecta una sola vez (el checkpoint recuerda qué rutas ya vio: tolera reinicios sin duplicar);
  2. se registra en BRONZE (tabla archivos_llegados: fuente, archivo, bytes, SHA-256, fecha, micro-lote) SIN modificar el original;
  3. opcionalmente dispara la carga incremental SILVER -> GOLD (--procesar).

Modos de disparo:
  (por defecto)  trigger availableNow: procesa todo lo nuevo desde el último checkpoint y termina (se programa con cron/EventBridge).
  --continuo     trigger processingTime: queda escuchando y reacciona cuando llega un archivo (demostración en vivo).

Los archivos de OSIPTEL se publican cada trimestre, así que el flujo es de baja frecuencia: se usa Structured Streaming por su
semántica incremental con checkpoint (exactly-once sobre archivos), no por latencia. Kafka no aporta aquí porque la fuente
no emite eventos; es la evolución si hubiera un productor continuo (ver D-21 y D-22).

Uso:
    python src/streaming_bronze.py                      # registra llegadas nuevas y termina
    python src/streaming_bronze.py --procesar           # además corre el pipeline incremental si llegó algo
    python src/streaming_bronze.py --continuo --procesar
"""
import argparse, os, re, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import RAW_URI, CHECKPOINT_URI, LLEGADAS_URI

PATRON_ARCHIVOS = "*.{xlsx,csv,XLSX,CSV}"
REGEX_FUENTE = r"/raw/([^/]+)/"


def fuente_de(ruta: str) -> str:
    """Fuente (robos, reclamos, cobertura) según la carpeta de RAW donde cayó el archivo."""
    m = re.search(REGEX_FUENTE, ruta)
    return m.group(1) if m else ""


def registro_llegadas(df, lote_id):
    """Columnas del registro de llegadas a partir del DataFrame binaryFile de un micro-lote (path, modificationTime, length, content)."""
    from pyspark.sql import functions as F
    return df.select(
        F.regexp_extract("path", REGEX_FUENTE, 1).alias("fuente"),
        F.element_at(F.split("path", "/"), -1).alias("archivo"),
        F.col("length").alias("bytes"),
        F.sha2("content", 256).alias("sha256"),
        F.col("modificationTime").alias("modificado"),
        F.lit(int(lote_id)).alias("lote_stream"),
        F.current_timestamp().alias("detectado_en"))


def iniciar(spark, continuo=False, procesar=False, intervalo="30 seconds", correr=None):
    """Arranca el stream y devuelve el StreamingQuery (ya terminado si el modo es availableNow)."""
    estado = {"llegadas": 0}

    def por_lote(df, lote_id):
        n = df.count()
        if n == 0:
            return
        registro_llegadas(df, lote_id).write.mode("append").option("compression", "snappy").parquet(LLEGADAS_URI)
        estado["llegadas"] += n
        print(f"[STREAM] micro-lote {lote_id}: {n} archivo(s) nuevo(s) registrados en BRONZE ({LLEGADAS_URI})")
        if procesar and correr:
            correr(spark)

    from pyspark.sql import types as T
    esquema = T.StructType([T.StructField("path", T.StringType()), T.StructField("modificationTime", T.TimestampType()),
                            T.StructField("length", T.LongType()), T.StructField("content", T.BinaryType())])
    lector = (spark.readStream.format("binaryFile").schema(esquema)
              .option("pathGlobFilter", PATRON_ARCHIVOS).option("recursiveFileLookup", "true")
              .load(RAW_URI))
    disparo = {"processingTime": intervalo} if continuo else {"availableNow": True}
    q = (lector.writeStream.foreachBatch(por_lote)
         .option("checkpointLocation", CHECKPOINT_URI).trigger(**disparo).start())
    return q, estado


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--continuo", action="store_true", help="queda escuchando (por defecto procesa lo pendiente y termina)")
    ap.add_argument("--procesar", action="store_true", help="si llegan archivos, corre el pipeline incremental (SILVER -> GOLD)")
    ap.add_argument("--intervalo", default="30 seconds")
    a = ap.parse_args()
    import pipeline
    sp = pipeline.spark_session("OSIPTEL_stream_bronze")
    q, est = iniciar(sp, a.continuo, a.procesar, a.intervalo, correr=lambda s: pipeline.correr(s))
    print(f"[STREAM] vigilando {RAW_URI} ({'continuo' if a.continuo else 'availableNow'}); checkpoint en {CHECKPOINT_URI}")
    q.awaitTermination()
    print(f"[STREAM] terminado: {est['llegadas']} archivo(s) nuevo(s) en esta ejecución")
