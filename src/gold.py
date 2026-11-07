"""SILVER -> GOLD: integración de fuentes y modelo dimensional (capa de consumo).

Dos uniones, cada una a la MISMA granularidad en ambos lados para no inflar totales:
  J1 gold_operadora_mes          = robos (F1) ⟗ reclamos móviles (F2)   por operadora + mes
  J2 gold_depto_operadora_202303 = reclamos móviles (F2) ⟕ cobertura (F3) por departamento + operadora, mar-2023
J1 usa FULL OUTER JOIN; J2 usa LEFT JOIN con broadcast. La columna `origen_join` deja explícito qué filas no tienen pareja.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import PERIODO_COBERTURA, MATERIAS_CALIDAD, ruta
from reglas import OPERADORA_OTRAS


def _extra(df):
    return ["copia"] if "copia" in df.columns else []


def gold_operadora_mes(robos: DataFrame, reclamos: DataFrame) -> DataFrame:
    ex = _extra(robos)
    piv = (robos.groupBy("fecha_mes", "operadora", *ex).pivot("estado", ["SUSTRAIDO", "RECUPERADO", "PERDIDO"])
                .agg(F.sum("cantidad"))
                .withColumnRenamed("SUSTRAIDO", "sustraidos").withColumnRenamed("RECUPERADO", "recuperados")
                .withColumnRenamed("PERDIDO", "perdidos").withColumn("_r", F.lit(True)))
    rec = (reclamos.filter("es_movil").groupBy("fecha_mes", "operadora", *ex)
                   .agg(F.sum("reclamos").alias("reclamos_moviles")).withColumn("_c", F.lit(True)))
    j = piv.join(rec, ["fecha_mes", "operadora", *ex], "full_outer")
    j = j.withColumn("_r", F.coalesce("_r", F.lit(False))).withColumn("_c", F.coalesce("_c", F.lit(False)))
    return (j.withColumn("origen_join", F.when(F.col("_r") & F.col("_c"), "ambas")
                                         .when(F.col("_r"), "solo_robos").otherwise("solo_reclamos"))
             .withColumn("tasa_recuperacion_pct", F.round(100 * F.col("recuperados") / F.col("sustraidos"), 3))
             .withColumn("anio", F.year("fecha_mes"))
             .drop("_r", "_c"))


def gold_depto_operadora(cobertura: DataFrame, reclamos: DataFrame, broadcast=True) -> DataFrame:
    ex = _extra(cobertura)
    cob = (cobertura.groupBy("departamento", "operadora", *ex)
                    .agg(F.count("*").alias("ccpp"), F.sum("tiene_4g").alias("ccpp_4g"),
                         F.sum("estaciones_4g").alias("estaciones_4g"))
                    .withColumn("pct_ccpp_4g", F.round(100 * F.col("ccpp_4g") / F.col("ccpp"), 3))
                    .withColumn("_v", F.lit(True)))
    rec = (reclamos.filter(F.col("es_movil") & (F.col("fecha_mes") == F.lit(PERIODO_COBERTURA).cast("date"))
                           & F.col("departamento").isNotNull())
                   .groupBy("departamento", "operadora", *ex)
                   .agg(F.sum("reclamos").alias("reclamos_moviles"),
                        F.sum(F.when(F.col("materia").isin(MATERIAS_CALIDAD), F.col("reclamos")).otherwise(0)).alias("reclamos_calidad"))
                   .withColumn("_c", F.lit(True)))
    # LEFT JOIN desde reclamos hacia la cobertura agregada (~100 filas): Spark puede difundir (broadcast)
    # el lado pequeño y evita el shuffle del lado de reclamos. Un FULL OUTER no admite broadcast.
    # Las combinaciones con cobertura y sin reclamos se cuentan aparte en `cobertura_sin_reclamos`.
    j = rec.join(F.broadcast(cob) if broadcast else cob, ["departamento", "operadora", *ex], "left")
    j = j.withColumn("_v", F.coalesce("_v", F.lit(False)))
    return (j.withColumn("origen_join", F.when(F.col("_v"), "ambas").otherwise("solo_reclamos"))
             .withColumn("periodo", F.lit(PERIODO_COBERTURA).cast("date")).drop("_v", "_c"))


def cobertura_sin_reclamos(cobertura: DataFrame, gold_depto: DataFrame) -> int:
    """Combinaciones departamento-operadora con cobertura pero sin reclamos en mar-2023 (control del LEFT JOIN)."""
    claves = cobertura.select("departamento", "operadora").distinct()
    return claves.join(gold_depto.select("departamento", "operadora"), ["departamento", "operadora"], "left_anti").count()


def dimensiones(spark, robos: DataFrame, reclamos: DataFrame, cobertura: DataFrame):
    meses = robos.select("fecha_mes").union(reclamos.select("fecha_mes")).distinct()
    dim_tiempo = (meses.withColumn("anio", F.year("fecha_mes")).withColumn("mes", F.month("fecha_mes"))
                       .withColumn("trimestre", F.quarter("fecha_mes"))
                       .withColumn("periodo_covid", F.col("fecha_mes").between("2020-03-01", "2021-03-01")))
    razones = (robos.select("operadora", F.explode("razones_sociales").alias("razon_social"))
                    .union(reclamos.select("operadora", "razon_social")).distinct()
                    .groupBy(F.col("operadora").alias("operadora_id"))
                    .agg(F.sort_array(F.collect_set("razon_social")).alias("razones_sociales_historicas")))
    dim_operadora = razones.withColumn("tipo", F.when(F.col("operadora_id") == OPERADORA_OTRAS, "OMV y otras").otherwise("Operador móvil de red"))
    dim_departamento = (cobertura.select(F.col("departamento").alias("departamento_id"),
                                         F.substring("ubigeo_distrito", 1, 2).alias("ubigeo_dpto"))
                                 .union(reclamos.filter(F.col("departamento").isNotNull())
                                                .select(F.col("departamento").alias("departamento_id"), F.lit(None).cast("string")))
                                 .groupBy("departamento_id").agg(F.sort_array(F.collect_set("ubigeo_dpto")).alias("ubigeos_dpto")))
    return dim_tiempo, dim_operadora, dim_departamento


def escribir(df: DataFrame, zona: str, tabla: str, particion=None):
    w = df.write.mode("overwrite").option("compression", "snappy")
    if particion:
        w = w.partitionBy(*particion)
    w.parquet(ruta(zona, tabla))
    print(f"[ESCRITURA] {zona}/{tabla}")
