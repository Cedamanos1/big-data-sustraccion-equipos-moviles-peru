"""Orquestador del pipeline: RAW -> STAGING -> SILVER/QUARANTINE -> GOLD -> tablas analíticas (Spark SQL) -> modelo -> tablero.

Es INCREMENTAL por defecto: si el data lake ya tiene una bitácora de cargas (<lake>/_control/cargas), solo procesa los
meses que aún no están en SILVER y recalcula en GOLD únicamente los meses afectados (ver src/incremental.py).
Si no hay bitácora (primera vez, o un lake de una versión anterior) hace la carga inicial completa.

Uso (desde la raíz del repositorio):
    python src/pipeline.py                 # incremental (o carga inicial si el lake está vacío)
    python src/pipeline.py --completa      # reconstruye todo desde RAW (sobrescribe SILVER y GOLD)
    python src/pipeline.py --estado        # muestra la bitácora de cargas y el contenido del lake
    python src/pipeline.py --sin-modelo    # solo datos
Deja evidencia en docs/evidencia/: manifiesto.json, calidad.json, corrida.json, modelo_metricas.json
"""
import argparse, json, os, platform, sys, time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import EVIDENCIA, LAKE, TABLERO, ruta
import ingesta, transform, gold, incremental, analitica


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


def configuracion_fuentes():
    """Cómo se carga cada fuente de forma incremental: lector, mes de cada fila, regla SILVER, tabla y claves."""
    from pyspark.sql import functions as F
    return {
        "robos": dict(fuente="robos", leer=transform.leer_robos, mes_expr=transform.a_mes(F.col("mes")),
                      silver=transform.silver_robos, tabla="robos", col_mes="fecha_mes", particion=["operadora"],
                      claves=["empresa", "mes", "anio", "estado", "cantidad"]),
        "reclamos": dict(fuente="reclamos", leer=transform.leer_reclamos, mes_expr=transform.a_mes(F.col("mes")),
                         silver=transform.silver_reclamos, tabla="reclamos", col_mes="fecha_mes", particion=["anio"],
                         claves=["empresa", "mes", "departamento", "servicio", "forma", "materia", "reclamos"]),
        "cobertura": dict(fuente="cobertura", leer=transform.leer_cobertura,
                          mes_expr=F.to_date(F.concat(F.col("PERIODO"), F.lit("01")), "yyyyMMdd"),
                          silver=transform.silver_cobertura, tabla="cobertura", col_mes="periodo_fecha",
                          particion=["operadora"], claves=["EMPRESA_OPERADORA", "UBIGEO_CCPP", "PERIODO"]),
    }


# ------------------------------------------------------------------ carga completa (inicial o --completa)
def _silver_completo(spark, calidad):
    s_rob, q_rob, calidad["robos"] = transform.silver_robos(transform.leer_robos(spark))
    s_rec, q_rec, calidad["reclamos"] = transform.silver_reclamos(transform.leer_reclamos(spark))
    s_cob, q_cob, calidad["cobertura"] = transform.silver_cobertura(transform.leer_cobertura(spark))
    gold.escribir(s_rob, "silver", "robos", ["operadora"])
    gold.escribir(s_rec, "silver", "reclamos", ["anio"])
    gold.escribir(s_cob, "silver", "cobertura", ["operadora"])
    for nombre, q in [("robos", q_rob), ("reclamos", q_rec), ("cobertura", q_cob)]:
        gold.escribir(q, "quarantine", nombre)


def _gold_completo(spark):
    rob = spark.read.parquet(ruta("silver", "robos"))
    rec = spark.read.parquet(ruta("silver", "reclamos")).cache()   # se reutiliza en J1, J2 y dimensiones
    cob = spark.read.parquet(ruta("silver", "cobertura"))
    gold.escribir(gold.gold_operadora_mes(rob, rec), "gold", "gold_operadora_mes", ["fecha_mes"])  # una partición por mes
    gold.escribir(gold.gold_depto_operadora(cob, rec), "gold", "gold_depto_operadora_202303")
    for nombre, df in zip(["dim_tiempo", "dim_operadora", "dim_departamento"], gold.dimensiones(spark, rob, rec, cob)):
        gold.escribir(df, "gold", nombre)
    rec.unpersist()


# ------------------------------------------------------------------ controles de calidad (ambos modos)
def _controles(spark, calidad):
    rob = spark.read.parquet(ruta("silver", "robos"))
    cob = spark.read.parquet(ruta("silver", "cobertura"))
    j1 = spark.read.parquet(ruta("gold", "gold_operadora_mes"))
    j2 = spark.read.parquet(ruta("gold", "gold_depto_operadora_202303"))
    calidad["join_operadora_mes"] = {r["origen_join"]: r["count"] for r in j1.groupBy("origen_join").count().collect()}
    calidad["join_depto_operadora_202303"] = {r["origen_join"]: r["count"] for r in j2.groupBy("origen_join").count().collect()}
    calidad["join_depto_operadora_202303"]["solo_cobertura"] = gold.cobertura_sin_reclamos(cob, j2)
    # prueba de conservación de totales: el JOIN no debe inflar ni perder equipos sustraídos
    tot_silver = rob.filter("estado = 'SUSTRAIDO'").agg({"cantidad": "sum"}).first()[0]
    tot_gold = j1.agg({"sustraidos": "sum"}).first()[0]
    calidad["conservacion_totales_sustraidos"] = {"silver": int(tot_silver), "gold": int(tot_gold), "ok": int(tot_silver) == int(tot_gold)}
    calidad["acumulado"] = {n: spark.read.parquet(ruta("silver", n)).count() for n in ("robos", "reclamos", "cobertura")}
    return j1, j2


def correr(spark, modelo=True, completa=False):
    lote = datetime.now().isoformat(timespec="seconds")
    tiempos, t = {}, time.perf_counter()
    manifiesto = ingesta.ingestar()
    tiempos["ingesta"] = round(time.perf_counter() - t, 2)
    shas = {k: v["sha256"] for k, v in manifiesto["fuentes"].items()}

    previas = None if completa else incremental.ultimas_sha(spark)
    modo = "completa" if previas is None else "incremental"
    print(f"[PIPELINE] modo: {modo}" + ("" if modo == "incremental" else " (no hay bitácora de cargas o se pidió --completa)"))
    calidad, registros, nuevos_por_fuente, plan = {"modo": modo}, [], {}, None

    if modo == "completa":
        t = time.perf_counter(); _silver_completo(spark, calidad); tiempos["silver"] = round(time.perf_counter() - t, 2)
        t = time.perf_counter(); _gold_completo(spark); tiempos["gold"] = round(time.perf_counter() - t, 2)
        for k, cfg in configuracion_fuentes().items():
            meses = incremental.meses_cargados(spark, "silver", cfg["tabla"], cfg["col_mes"])
            a, b, lista = incremental.resumen_meses(meses)
            r = calidad[k]
            registros.append({"fuente": k, "modo": "completa", "sha256": shas[k], "filas_fuente": manifiesto["fuentes"][k]["filas"],
                              "filas_nuevas": r["leidos"], "aceptados": r["aceptados"], "rechazados": r["rechazados"],
                              "meses_nuevos": len(meses), "mes_min": a, "mes_max": b, "meses_lista": lista})
        cambio_fuente = True
    else:
        t = time.perf_counter()
        for k, cfg in configuracion_fuentes().items():
            if incremental.decidir_modo(shas[k], previas.get(k)) == "sin_cambios":
                print(f"[INCREMENTAL] {k}: archivo sin cambios (misma huella SHA-256) → no se procesa")
                registros.append({"fuente": k, "modo": "sin_cambios", "sha256": shas[k], "filas_fuente": manifiesto["fuentes"][k]["filas"],
                                  "filas_nuevas": 0, "aceptados": 0, "rechazados": 0, "meses_nuevos": 0})
                calidad[k] = {"fuente": k, "sin_cambios": True}
                continue
            reg, calidad[k], nuevos_por_fuente[k] = incremental.cargar_fuente(spark, cfg, shas[k], manifiesto["fuentes"][k]["filas"])
            registros.append(reg)
        tiempos["silver"] = round(time.perf_counter() - t, 2)
        cambio_fuente = any(r["modo"] != "sin_cambios" for r in registros)
        t = time.perf_counter()
        if cambio_fuente:
            plan = incremental.actualizar_gold(spark, nuevos_por_fuente.get("cobertura"), gold)
        else:
            print("[INCREMENTAL] sin datos nuevos: SILVER y GOLD no se tocan")
        tiempos["gold"] = round(time.perf_counter() - t, 2)

    j1, j2 = _controles(spark, calidad)

    # capa analítica: tablas an_* con Spark SQL sobre GOLD (se rehacen solo si hubo datos nuevos o faltan)
    if cambio_fuente or not analitica.existen(spark):
        t = time.perf_counter()
        analitica.construir(spark, EVIDENCIA / "analitica")
        tiempos["analitica"] = round(time.perf_counter() - t, 2)
    else:
        print("[ANALÍTICA] sin datos nuevos: tablas analíticas vigentes")

    if modelo:
        import modelo as m, tablero
        pendiente = cambio_fuente or not TABLERO.exists() or not (EVIDENCIA / "pronostico_mes_siguiente.csv").exists()
        if pendiente:
            t = time.perf_counter()
            g_pd, j2_pd = j1.toPandas(), j2.toPandas()
            m.ejecutar(g_pd, EVIDENCIA)
            tiempos["modelo"] = round(time.perf_counter() - t, 2)
            datos = tablero.generar(g_pd, j2_pd, EVIDENCIA, TABLERO, manifiesto["fuentes"], an=analitica.leer(spark))
            calidad["reconciliacion_tablero_sql"] = datos["reconciliacion"]
        else:
            print("[MODELO] sin datos nuevos: pronóstico y tablero vigentes")

    # la bitácora se escribe AL FINAL: si algo falla antes, la siguiente corrida repite y se autorrepara
    incremental.registrar_cargas(spark, [dict(r, lote=lote, fecha_carga=lote) for r in registros])

    tiempos["total"] = round(sum(tiempos.values()), 2)
    EVIDENCIA.mkdir(parents=True, exist_ok=True)
    (EVIDENCIA / "calidad.json").write_text(json.dumps(calidad, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    corrida = {"fecha": lote, "modo": modo, "lake": LAKE, "spark": spark.version,
               "python": platform.python_version(), "maquina": platform.node(), "cpu": os.cpu_count(),
               "filas_fuente": manifiesto["filas_totales"],
               "fuentes": {r["fuente"]: {"modo": r["modo"], "filas_nuevas": r["filas_nuevas"], "meses_nuevos": r["meses_nuevos"],
                                         "meses": r.get("meses_lista") or ""} for r in registros},
               "gold_meses_recalculados": plan["j1_meses"] if plan else ([] if modo == "incremental" else "todos"),
               "tiempos_s": tiempos}
    (EVIDENCIA / "corrida.json").write_text(json.dumps(corrida, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(corrida, indent=2, ensure_ascii=False))
    return corrida


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--sin-modelo", action="store_true")
    ap.add_argument("--completa", action="store_true", help="reconstruye SILVER y GOLD desde RAW (sobrescribe)")
    ap.add_argument("--estado", action="store_true", help="muestra la bitácora de cargas y el contenido del lake")
    a = ap.parse_args()
    sp = spark_session()
    if a.estado:
        incremental.mostrar_estado(sp)
    else:
        correr(sp, modelo=not a.sin_modelo, completa=a.completa)
