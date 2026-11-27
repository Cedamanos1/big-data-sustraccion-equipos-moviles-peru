"""Carga incremental: agrega datos nuevos al data lake sin perder lo anterior ni reprocesar todo.

Idea central: las reglas de SILVER trabajan MES A MES (fecha, duplicados y agregación son por mes), así que
procesar solo los meses que todavía no están en SILVER da exactamente el mismo resultado que procesar todo.

Flujo de una corrida incremental
  1. Compuerta SHA-256: si el archivo fuente no cambió desde la última carga, esa fuente no se toca.
  2. Si cambió (OSIPTEL publica el acumulado completo), se leen sus filas pero SOLO se procesan y escriben
     las de los meses que no están en SILVER (los demás se descartan antes de aplicar reglas).
  3. SILVER se ANEXA (mode append): lo cargado antes queda intacto. Lo rechazado se anexa a QUARANTINE sin duplicar.
  4. GOLD se actualiza solo para los meses DESFASADOS (SILVER ≠ GOLD): los nuevos y los que cambiaron de categoría,
     por ejemplo un mes que estaba «solo_robos» y ahora ya tiene reclamos. Se reemplaza únicamente esa partición.
  5. Se registra la carga en la bitácora <lake>/_control/cargas (qué cambió, cuántas filas, qué meses).

Si la corrida se interrumpe, la siguiente se repara sola: el paso 4 detecta lo que falta en GOLD.
Limitación declarada: no detecta correcciones de OSIPTEL sobre meses YA cargados; para eso existe --completa.

Las funciones puras (sin Spark) están arriba y se prueban en tests/test_incremental.py; Spark se importa dentro de
cada función para que las pruebas corran sin él.
"""
from datetime import date, datetime

from config import ruta, PERIODO_COBERTURA

CONTROL = ("_control", "cargas")
COLUMNAS_CONTROL = ["lote", "fecha_carga", "fuente", "modo", "sha256", "filas_fuente", "filas_nuevas",
                    "aceptados", "rechazados", "meses_nuevos", "mes_min", "mes_max", "meses_lista"]


# ============================================================ lógica pura (sin Spark)
def meses_nuevos(meses_fuente, meses_cargados):
    """Meses (texto ISO 'YYYY-MM-DD') presentes en la fuente que aún no están cargados. Ignora los nulos."""
    cargados = set(meses_cargados)
    return sorted({m for m in meses_fuente if m is not None and m not in cargados})


def decidir_modo(sha_actual, sha_ultima):
    """'sin_cambios' si el archivo es idéntico al de la última carga; en otro caso 'incremental'."""
    return "sin_cambios" if (sha_ultima is not None and sha_actual == sha_ultima) else "incremental"


def guardar_evidencia(modo, hubo_cambios, existe_evidencia):
    """¿Esta corrida debe actualizar calidad.json y corrida.json?
    Una corrida incremental «sin cambios» no debe pisar la evidencia de la última corrida que sí procesó datos."""
    return modo != "incremental" or hubo_cambios or not existe_evidencia


def plan_gold(meses_desfasados, cobertura_nueva, existe_j2, existen_dimensiones):
    """Qué hay que recalcular en GOLD. J1 se recalcula por mes; J2 y dimensiones son tablas pequeñas."""
    meses = sorted(set(meses_desfasados))
    return {
        "j1_meses": meses,
        "recalcular_j2": bool(cobertura_nueva or (PERIODO_COBERTURA in meses) or not existe_j2),
        "recalcular_dimensiones": bool(meses or cobertura_nueva or not existen_dimensiones),
    }


def resumen_meses(meses):
    """(mínimo, máximo, texto) de una lista de meses ISO; para la bitácora y los mensajes."""
    if not meses:
        return None, None, ""
    m = sorted(meses)
    return m[0], m[-1], ",".join(x[:7] for x in m)


def _iso(m):
    return m.isoformat() if m is not None else None


# ============================================================ utilidades Spark
def leer_o_none(spark, zona, tabla):
    try:
        return spark.read.parquet(ruta(zona, tabla))
    except Exception:  # la ruta no existe todavía
        return None


def meses_cargados(spark, zona, tabla, columna):
    df = leer_o_none(spark, zona, tabla)
    if df is None:
        return []
    return [_iso(r[0]) for r in df.select(columna).distinct().collect() if r[0] is not None]


# ============================================================ bitácora de cargas
def ultimas_sha(spark):
    """{fuente: sha256 de la última carga}, o None si todavía no hay bitácora (→ carga inicial completa)."""
    c = leer_o_none(spark, *CONTROL)
    if c is None:
        return None
    from pyspark.sql import functions as F
    out = {}
    for r in c.orderBy(F.col("fecha_carga").desc()).collect():
        out.setdefault(r["fuente"], r["sha256"])
    return out


def registrar_cargas(spark, filas):
    from pyspark.sql import types as T
    texto, largo = T.StringType(), T.LongType()
    tipos = {"filas_fuente": largo, "filas_nuevas": largo, "aceptados": largo, "rechazados": largo, "meses_nuevos": largo}
    schema = T.StructType([T.StructField(c, tipos.get(c, texto)) for c in COLUMNAS_CONTROL])
    datos = [tuple(f.get(c) for c in COLUMNAS_CONTROL) for f in filas]
    spark.createDataFrame(datos, schema).write.mode("append").parquet(ruta(*CONTROL))


def mostrar_estado(spark):
    """Imprime la bitácora de cargas y el contenido actual de cada capa (para la demostración)."""
    from pyspark.sql import functions as F
    c = leer_o_none(spark, *CONTROL)
    print("\n=== Bitácora de cargas (_control/cargas) ===")
    if c is None:
        print("(sin cargas registradas)")
    else:
        (c.orderBy("fecha_carga", "fuente")
          .select("fecha_carga", "fuente", "modo", "filas_fuente", "filas_nuevas", "meses_nuevos", "mes_min", "mes_max")
          .show(50, truncate=False))
    print("=== Contenido actual del lake ===")
    for zona, tabla, col in [("silver", "robos", "fecha_mes"), ("silver", "reclamos", "fecha_mes"),
                             ("silver", "cobertura", "periodo_fecha"), ("gold", "gold_operadora_mes", "fecha_mes")]:
        df = leer_o_none(spark, zona, tabla)
        if df is None:
            print(f"{zona}/{tabla}: (vacía)"); continue
        r = df.agg(F.count("*").alias("n"), F.min(col).alias("a"), F.max(col).alias("b"), F.countDistinct(col).alias("m")).first()
        print(f"{zona}/{tabla}: {r['n']:,} filas · {r['m']} meses · {r['a']} → {r['b']}")


# ============================================================ SILVER incremental
def _anexar_cuarentena(spark, q, tabla, claves):
    """Anexa a QUARANTINE solo los rechazos que no estaban ya (las filas con fecha inválida se releen en cada carga)."""
    from pyspark.sql import functions as F
    if q.limit(1).count() == 0:
        return 0
    previo = leer_o_none(spark, "quarantine", tabla)
    if previo is not None:
        p = previo.select(*[F.col(c).alias("_p_" + c) for c in claves]).distinct()
        cond = None
        for c in claves:
            e = q[c].eqNullSafe(p["_p_" + c])
            cond = e if cond is None else (cond & e)
        q = q.join(p, cond, "left_anti")
    q = q.cache()
    n = q.count()
    if n:
        q.write.mode("append").option("compression", "snappy").parquet(ruta("quarantine", tabla))
    q.unpersist()
    return n


def cargar_fuente(spark, cfg, sha, filas_fuente):
    """Procesa una fuente de forma incremental. cfg: dict con leer, mes_expr, silver, tabla, col_mes, particion, claves."""
    from pyspark.sql import functions as F
    cargados = meses_cargados(spark, "silver", cfg["tabla"], cfg["col_mes"])
    df = cfg["leer"](spark).withColumn("_mes", cfg["mes_expr"])
    if cargados:
        fechas = [date.fromisoformat(m) for m in cargados]
        df = df.filter(F.col("_mes").isNull() | ~F.col("_mes").isin(*fechas))
    df = df.cache()
    nuevos = meses_nuevos([_iso(r[0]) for r in df.select("_mes").distinct().collect()], [])
    filas_nuevas = df.count()
    reg = {"fuente": cfg["fuente"], "sha256": sha, "filas_fuente": int(filas_fuente), "filas_nuevas": int(filas_nuevas),
           "aceptados": 0, "rechazados": 0, "meses_nuevos": len(nuevos), "modo": "incremental"}
    reg["mes_min"], reg["mes_max"], reg["meses_lista"] = resumen_meses(nuevos)
    resumen = {"fuente": cfg["fuente"], "leidos": int(filas_nuevas), "aceptados": 0, "rechazados": 0}
    if filas_nuevas:
        silver, q, res = cfg["silver"](df.drop("_mes"))
        (silver.write.mode("append").option("compression", "snappy").partitionBy(*cfg["particion"])
               .parquet(ruta("silver", cfg["tabla"])))
        _anexar_cuarentena(spark, q, cfg["tabla"], cfg["claves"])
        resumen = res
        reg["aceptados"], reg["rechazados"] = int(res["aceptados"]), int(res["rechazados"])
    df.unpersist()
    print(f"[INCREMENTAL] {cfg['fuente']}: {len(nuevos)} meses nuevos"
          + (f" ({reg['mes_min'][:7]} → {reg['mes_max'][:7]})" if nuevos else "") + f", {filas_nuevas:,} filas procesadas de {filas_fuente:,}")
    return reg, resumen, nuevos


# ============================================================ GOLD incremental
def meses_desfasados(spark):
    """Meses cuyo resultado en GOLD no coincide con SILVER (faltan, o cambiaron de categoría). Es la autorreparación."""
    from pyspark.sql import functions as F
    rob, rec = leer_o_none(spark, "silver", "robos"), leer_o_none(spark, "silver", "reclamos")
    g = leer_o_none(spark, "gold", "gold_operadora_mes")
    s_rob = (rob.groupBy("fecha_mes").agg(
        F.sum(F.when(F.col("estado") == "SUSTRAIDO", F.col("cantidad")).otherwise(0)).alias("s_sus"),
        F.sum(F.when(F.col("estado") == "RECUPERADO", F.col("cantidad")).otherwise(0)).alias("s_rec"),
        F.sum(F.when(F.col("estado") == "PERDIDO", F.col("cantidad")).otherwise(0)).alias("s_per"))
        .withColumn("_r", F.lit(True)))
    s_cla = (rec.filter("es_movil").groupBy("fecha_mes").agg(F.sum("reclamos").alias("s_cla")).withColumn("_c", F.lit(True)))
    s = s_rob.join(s_cla, "fecha_mes", "full_outer").withColumn("_s", F.lit(True))
    if g is None:
        return sorted(_iso(r[0]) for r in s.select("fecha_mes").collect() if r[0] is not None)
    g_chk = (g.groupBy("fecha_mes").agg(F.sum("sustraidos").alias("g_sus"), F.sum("recuperados").alias("g_rec"),
                                         F.sum("perdidos").alias("g_per"), F.sum("reclamos_moviles").alias("g_cla"))
              .withColumn("_g", F.lit(True)))
    c = s.join(g_chk, "fecha_mes", "full_outer")
    z = F.lit(0)
    dif = ((F.coalesce("_s", F.lit(False)) != F.coalesce("_g", F.lit(False)))
           | (F.coalesce("s_sus", z) != F.coalesce("g_sus", z)) | (F.coalesce("s_rec", z) != F.coalesce("g_rec", z))
           | (F.coalesce("s_per", z) != F.coalesce("g_per", z)) | (F.coalesce("s_cla", z) != F.coalesce("g_cla", z)))
    return sorted(_iso(r[0]) for r in c.filter(dif).select("fecha_mes").collect() if r[0] is not None)


def actualizar_gold(spark, meses_nuevos_cobertura, gold_mod):
    """Recalcula en GOLD solo lo desfasado. Devuelve el plan aplicado. gold_mod es el módulo gold (inyectado)."""
    from pyspark.sql import functions as F
    desf = meses_desfasados(spark)
    plan = plan_gold(desf, bool(meses_nuevos_cobertura),
                     leer_o_none(spark, "gold", "gold_depto_operadora_202303") is not None,
                     leer_o_none(spark, "gold", "dim_tiempo") is not None)
    if plan["j1_meses"]:
        fechas = [date.fromisoformat(m) for m in plan["j1_meses"]]
        rob = spark.read.parquet(ruta("silver", "robos")).filter(F.col("fecha_mes").isin(*fechas))
        rec = spark.read.parquet(ruta("silver", "reclamos")).filter(F.col("fecha_mes").isin(*fechas))
        j1 = gold_mod.gold_operadora_mes(rob, rec)
        # Solo se reemplazan las particiones (meses) presentes en j1; el resto de GOLD no se toca.
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        try:
            (j1.write.mode("overwrite").option("compression", "snappy").partitionBy("fecha_mes")
               .parquet(ruta("gold", "gold_operadora_mes")))
        finally:
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
        print(f"[INCREMENTAL] GOLD J1: {len(plan['j1_meses'])} meses recalculados ({plan['j1_meses'][0][:7]} → {plan['j1_meses'][-1][:7]})")
    else:
        print("[INCREMENTAL] GOLD J1 al día: no hay meses por recalcular")
    if plan["recalcular_j2"] or plan["recalcular_dimensiones"]:
        rob = spark.read.parquet(ruta("silver", "robos"))
        rec = spark.read.parquet(ruta("silver", "reclamos"))
        cob = spark.read.parquet(ruta("silver", "cobertura"))
        if plan["recalcular_j2"]:
            gold_mod.escribir(gold_mod.gold_depto_operadora(cob, rec), "gold", "gold_depto_operadora_202303")
        if plan["recalcular_dimensiones"]:
            for nombre, d in zip(["dim_tiempo", "dim_operadora", "dim_departamento"], gold_mod.dimensiones(spark, rob, rec, cob)):
                gold_mod.escribir(d, "gold", nombre)
    return plan
