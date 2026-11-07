"""Orquestador del pipeline completo: RAW -> STAGING -> SILVER/QUARANTINE -> GOLD -> modelo -> tablero (docs/index.html).

Uso (desde la raíz del repositorio):
    python src/pipeline.py                 # corrida completa
    python src/pipeline.py --sin-modelo    # solo datos
Deja evidencia en docs/evidencia/: manifiesto.json, calidad.json, corrida.json, modelo_metricas.json
"""
import argparse, json, os, platform, sys, time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import EVIDENCIA, LAKE, ruta
import ingesta, transform, gold


def spark_session(app="OSIPTEL_pipeline"):
    from pyspark.sql import SparkSession
    b = SparkSession.builder.appName(app)
    if os.environ.get("SPARK_MASTER"):
        b = b.master(os.environ["SPARK_MASTER"])
    if os.environ.get("SPARK_DRIVER_MEMORY"):          # p. ej. export SPARK_DRIVER_MEMORY=3g (≈ 70 % de la RAM de la VM)
        b = b.config("spark.driver.memory", os.environ["SPARK_DRIVER_MEMORY"])
    spark = (b.config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTITIONS", "8"))
              .config("spark.sql.session.timeZone", "America/Lima")
              .config("spark.sql.ansi.enabled", "false")
              .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def correr(spark, modelo=True):
    tiempos, t = {}, time.perf_counter()
    manifiesto = ingesta.ingestar()
    tiempos["ingesta"] = round(time.perf_counter() - t, 2)

    t = time.perf_counter()
    calidad = {}
    s_rob, q_rob, calidad["robos"] = transform.silver_robos(transform.leer_robos(spark))
    s_rec, q_rec, calidad["reclamos"] = transform.silver_reclamos(transform.leer_reclamos(spark))
    s_cob, q_cob, calidad["cobertura"] = transform.silver_cobertura(transform.leer_cobertura(spark))
    gold.escribir(s_rob, "silver", "robos", ["operadora"])
    gold.escribir(s_rec, "silver", "reclamos", ["anio"])
    gold.escribir(s_cob, "silver", "cobertura", ["operadora"])
    for nombre, q in [("robos", q_rob), ("reclamos", q_rec), ("cobertura", q_cob)]:
        gold.escribir(q, "quarantine", nombre)
    tiempos["silver"] = round(time.perf_counter() - t, 2)

    t = time.perf_counter()
    rob = spark.read.parquet(ruta("silver", "robos"))
    rec = spark.read.parquet(ruta("silver", "reclamos")).cache()   # se reutiliza en J1, J2 y dimensiones
    cob = spark.read.parquet(ruta("silver", "cobertura"))
    j1 = gold.gold_operadora_mes(rob, rec)
    j2 = gold.gold_depto_operadora(cob, rec)
    gold.escribir(j1, "gold", "gold_operadora_mes", ["anio"])
    gold.escribir(j2, "gold", "gold_depto_operadora_202303")
    for nombre, df in zip(["dim_tiempo", "dim_operadora", "dim_departamento"], gold.dimensiones(spark, rob, rec, cob)):
        gold.escribir(df, "gold", nombre)
    j1 = spark.read.parquet(ruta("gold", "gold_operadora_mes"))
    j2 = spark.read.parquet(ruta("gold", "gold_depto_operadora_202303"))
    calidad["join_operadora_mes"] = {r["origen_join"]: r["count"] for r in j1.groupBy("origen_join").count().collect()}
    calidad["join_depto_operadora_202303"] = {r["origen_join"]: r["count"] for r in j2.groupBy("origen_join").count().collect()}
    calidad["join_depto_operadora_202303"]["solo_cobertura"] = gold.cobertura_sin_reclamos(cob, j2)
    # prueba de conservación de totales: el JOIN no debe inflar ni perder equipos sustraídos
    tot_silver = rob.filter("estado = 'SUSTRAIDO'").agg({"cantidad": "sum"}).first()[0]
    tot_gold = j1.agg({"sustraidos": "sum"}).first()[0]
    calidad["conservacion_totales_sustraidos"] = {"silver": int(tot_silver), "gold": int(tot_gold), "ok": int(tot_silver) == int(tot_gold)}
    rec.unpersist()
    tiempos["gold"] = round(time.perf_counter() - t, 2)

    if modelo:
        import modelo as m
        t = time.perf_counter()
        g_pd, j2_pd = j1.toPandas(), j2.toPandas()
        m.ejecutar(g_pd, EVIDENCIA)
        tiempos["modelo"] = round(time.perf_counter() - t, 2)
        import tablero
        from config import ROOT
        tablero.generar(g_pd, j2_pd, EVIDENCIA, ROOT / "docs" / "index.html", manifiesto["fuentes"])

    tiempos["total"] = round(sum(tiempos.values()), 2)
    EVIDENCIA.mkdir(parents=True, exist_ok=True)
    (EVIDENCIA / "calidad.json").write_text(json.dumps(calidad, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    corrida = {"fecha": datetime.now().isoformat(timespec="seconds"), "lake": LAKE, "spark": spark.version,
               "python": platform.python_version(), "maquina": platform.node(), "cpu": os.cpu_count(),
               "filas_fuente": manifiesto["filas_totales"], "tiempos_s": tiempos}
    (EVIDENCIA / "corrida.json").write_text(json.dumps(corrida, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(corrida, indent=2, ensure_ascii=False))
    return corrida


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--sin-modelo", action="store_true")
    a = ap.parse_args()
    correr(spark_session(), modelo=not a.sin_modelo)
