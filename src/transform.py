"""STAGING -> SILVER (+ QUARANTINE) con PySpark.

Cada función devuelve (validos, rechazados, resumen). Las reglas son las de reglas.py
expresadas con funciones nativas de Spark (sin UDF de Python, para no penalizar rendimiento).
Si el DataFrame trae la columna `copia` (prueba de escalabilidad), se conserva en claves y
agrupaciones para que las réplicas no se dedupliquen entre sí.
"""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from config import STAGING, FUENTES
from reglas import OPERADORA_PREFIJOS, OPERADORA_OTRAS, ESTADOS, DEPTO_UNIFICAR, ACENTOS_ORIGEN, ACENTOS_DESTINO

SCHEMA_ROBOS = T.StructType([T.StructField(c, T.StringType()) for c in ["empresa", "mes", "anio", "estado", "cantidad"]])
SCHEMA_RECLAMOS = T.StructType([T.StructField(c, T.StringType()) for c in
                                ["empresa", "mes", "departamento", "servicio", "forma", "materia", "reclamos"]])


# ---------------------------------------------------------------- utilidades
def norm(col):
    """Equivalente Spark de reglas.normalizar_texto."""
    s = F.upper(F.translate(F.trim(col), ACENTOS_ORIGEN, ACENTOS_DESTINO))
    s = F.regexp_replace(s, r"\s+", " ")
    return F.when((s == "") | (s == "NAN"), F.lit(None)).otherwise(s)


def operadora(col):
    s = norm(col)
    expr = F.lit(OPERADORA_OTRAS)
    for prefijo, marca in reversed(OPERADORA_PREFIJOS):
        expr = F.when(s.startswith(prefijo), F.lit(marca)).otherwise(expr)
    return expr


def departamento(col):
    s = norm(col)
    expr = F.when(s.isNull() | (s == "-"), F.lit(None)).otherwise(s)
    for origen, destino in DEPTO_UNIFICAR.items():
        expr = F.when(s == origen, F.lit(destino)).otherwise(expr)
    return expr


def a_mes(col):
    """'2017-01-01 00:00:00' o '2017-01-01' -> primer día del mes (date)."""
    return F.trunc(F.to_date(F.substring(F.trim(col), 1, 10), "yyyy-MM-dd"), "month")


def separar(df: DataFrame, reglas):
    """reglas: lista ordenada de (motivo, condición_de_ERROR). La primera que falle define el motivo."""
    motivo = F.lit(None).cast("string")
    for nombre, falla in reversed(reglas):
        motivo = F.when(F.coalesce(falla, F.lit(True)), F.lit(nombre)).otherwise(motivo)
    d = df.withColumn("motivo_rechazo", motivo)
    return d.filter(F.col("motivo_rechazo").isNull()).drop("motivo_rechazo"), d.filter(F.col("motivo_rechazo").isNotNull())


def _extra(df):
    return ["copia"] if "copia" in df.columns else []


def resumen(nombre, total, validos, rechazados, extra=None):
    r = {"fuente": nombre, "leidos": total, "aceptados": validos, "rechazados": rechazados,
         "pct_validos": round(100 * validos / total, 3) if total else None}
    r.update(extra or {})
    print(f"[SILVER] {nombre}: leídos={total:,} aceptados={validos:,} rechazados={rechazados:,}")
    return r


# ---------------------------------------------------------------- lectores
def leer_robos(spark: SparkSession):
    return spark.read.option("header", True).option("escape", '"').schema(SCHEMA_ROBOS).csv(str(STAGING / "robos" / "*.csv"))


def leer_reclamos(spark: SparkSession):
    return spark.read.option("header", True).option("escape", '"').schema(SCHEMA_RECLAMOS).option("multiLine", True).csv(str(STAGING / "reclamos" / "*.csv"))


def leer_cobertura(spark: SparkSession):
    return (spark.read.option("header", True).option("sep", ";").option("encoding", "ISO-8859-1")
            .option("inferSchema", False).csv(str(FUENTES["cobertura"] / "*.csv")))


# ---------------------------------------------------------------- F1 robos
def silver_robos(df: DataFrame, contar=True):
    estado = norm(F.col("estado"))
    est = F.lit(None).cast("string")
    for k, v in ESTADOS.items():
        est = F.when(estado == k, F.lit(v)).otherwise(est)
    d = (df.withColumn("operadora", operadora(F.col("empresa")))
           .withColumn("fecha_mes", a_mes(F.col("mes")))
           .withColumn("anio_int", F.col("anio").cast("double").cast("int"))
           .withColumn("estado_n", est)
           .withColumn("cantidad_n", F.col("cantidad").cast("double")))
    w = Window.partitionBy("empresa", "fecha_mes", "estado_n", *_extra(df)).orderBy(F.lit(1))
    d = d.withColumn("_ocurrencia", F.row_number().over(w))
    ok, bad = separar(d, [
        ("fecha_invalida", F.col("fecha_mes").isNull()),
        ("anio_incoherente", F.year("fecha_mes") != F.col("anio_int")),
        ("estado_fuera_dominio", F.col("estado_n").isNull()),
        ("cantidad_invalida", F.col("cantidad_n").isNull() | (F.col("cantidad_n") < 0) | (F.col("cantidad_n") != F.floor("cantidad_n"))),
        ("duplicado", F.col("_ocurrencia") > 1),
    ])
    silver = (ok.groupBy("fecha_mes", "operadora", F.col("estado_n").alias("estado"), *_extra(df))
                .agg(F.sum(F.col("cantidad_n").cast("long")).alias("cantidad"),
                     F.collect_set("empresa").alias("razones_sociales")))
    res = None
    if contar:
        n, nb = df.count(), bad.count()
        res = resumen("robos", n, n - nb, nb, {"motivos": {r["motivo_rechazo"]: r["count"] for r in bad.groupBy("motivo_rechazo").count().collect()}})
    return silver, bad.drop("_ocurrencia"), res


# ---------------------------------------------------------------- F2 reclamos
def silver_reclamos(df: DataFrame, contar=True):
    mat = norm(F.split(F.col("materia"), r"\|").getItem(0))
    d = (df.withColumn("operadora", operadora(F.col("empresa")))
           .withColumn("fecha_mes", a_mes(F.col("mes")))
           .withColumn("departamento_n", departamento(F.col("departamento")))
           .withColumn("servicio_n", norm(F.col("servicio")))
           .withColumn("materia_n", F.when(mat.isNull() | (mat == "-"), F.lit(None)).otherwise(mat))
           .withColumn("reclamos_n", F.col("reclamos").cast("double")))
    ok, bad = separar(d, [
        ("empresa_nula", F.col("empresa").isNull()),
        ("fecha_invalida", F.col("fecha_mes").isNull()),
        ("reclamos_invalido", F.col("reclamos_n").isNull() | (F.col("reclamos_n") < 0)),
    ])
    silver = ok.select("fecha_mes", F.year("fecha_mes").alias("anio"), "operadora", F.col("empresa").alias("razon_social"),
                       F.col("departamento_n").alias("departamento"), F.col("servicio_n").alias("servicio"),
                       F.col("materia_n").alias("materia"), "forma", F.col("reclamos_n").cast("long").alias("reclamos"),
                       (F.col("servicio_n") == "SERVICIOS MOVILES").alias("es_movil"), *_extra(df))
    res = None
    if contar:
        n, nb = df.count(), bad.count()
        sin_depto = silver.filter(F.col("departamento").isNull()).count()
        res = resumen("reclamos", n, n - nb, nb, {"sin_departamento_marcados": sin_depto})
    return silver, bad, res


# ---------------------------------------------------------------- F3 cobertura
def silver_cobertura(df: DataFrame, contar=True):
    d = df
    for c in ["2G", "3G", "4G", "5G", "CANT_EB_2G", "CANT_EB_3G", "CANT_EB_4G", "CANT_EB_5G"]:
        d = d.withColumn(c, F.col(c).cast("int"))
    d = (d.withColumn("operadora", operadora(F.col("EMPRESA_OPERADORA")))
          .withColumn("departamento_n", departamento(F.col("DEPARTAMENTO")))
          .withColumn("lat", F.regexp_replace("LATITUD", ",", ".").cast("double"))
          .withColumn("lon", F.regexp_replace("LONGITUD", ",", ".").cast("double"))
          .withColumn("periodo_fecha", F.to_date(F.concat(F.col("PERIODO"), F.lit("01")), "yyyyMMdd")))
    w = Window.partitionBy("EMPRESA_OPERADORA", "UBIGEO_CCPP", *_extra(df)).orderBy(F.lit(1))
    d = d.withColumn("_ocurrencia", F.row_number().over(w))
    banderas_ok = F.col("2G").isin(0, 1) & F.col("3G").isin(0, 1) & F.col("4G").isin(0, 1) & F.col("5G").isin(0, 1)
    ok, bad = separar(d, [
        ("ubigeo_invalido", ~F.col("UBIGEO_CCPP").rlike(r"^\d{10}$")),
        ("bandera_fuera_dominio", ~banderas_ok),
        ("coordenada_fuera_peru", ~(F.col("lat").between(-18.5, 0.1) & F.col("lon").between(-81.5, -68.5))),
        ("duplicado", F.col("_ocurrencia") > 1),
    ])
    silver = ok.select("periodo_fecha", "operadora", F.col("departamento_n").alias("departamento"),
                       F.col("UBIGEO_DISTRITO").alias("ubigeo_distrito"), F.col("UBIGEO_CCPP").alias("ubigeo_ccpp"),
                       F.col("2G").alias("tiene_2g"), F.col("3G").alias("tiene_3g"), F.col("4G").alias("tiene_4g"),
                       F.col("5G").alias("tiene_5g"), F.col("CANT_EB_4G").alias("estaciones_4g"), *_extra(df))
    res = None
    if contar:
        n, nb = df.count(), bad.count()
        res = resumen("cobertura", n, n - nb, nb, {"departamentos_crudos": df.select("DEPARTAMENTO").distinct().count(),
                                                    "departamentos_normalizados": silver.select("departamento").distinct().count()})
    return silver, bad.drop("_ocurrencia"), res
