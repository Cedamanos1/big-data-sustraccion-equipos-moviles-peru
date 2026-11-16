"""Capa analítica: tablas consultables con Spark SQL que alimentan el tablero.

GOLD guarda los datos integrados (join de las tres fuentes + dimensiones). Esta capa agrega las TABLAS ANALÍTICAS
(an_*): indicadores ya calculados con SQL, listos para consultar o conectar a un tablero. El tablero
(docs/index.html) lee estas tablas; el cálculo equivalente en pandas se conserva solo para RECONCILIAR (si ambos
caminos no coinciden, el pipeline lo informa).

    python src/analitica.py               # (re)construye las tablas analíticas desde GOLD
    python src/analitica.py --consultas   # además corre consultas de ejemplo (útil en la demostración)

Tablas (en <lake>/gold/ y como CSV en docs/evidencia/analitica/):
    an_kpi_operadora   KPI de los últimos 12 meses por operadora: tasa de recuperación, brecha vs. mediana,
                       prioridad y potencial de recuperación
    an_serie_mensual   serie mensual por operadora con media móvil de 3 meses y variación interanual (ventanas SQL)
    an_resumen_anual   totales por año y operadora (une GOLD con dim_tiempo)
    an_depto_calidad   reclamos por departamento, % de calidad y % de centros poblados con 4G (mar-2023)
"""
import json
import time
from pathlib import Path

OPERADORAS = "('BITEL','CLARO','ENTEL','MOVISTAR')"

VISTAS = ["gold_operadora_mes", "gold_depto_operadora_202303", "dim_tiempo", "dim_operadora", "dim_departamento"]

CONSULTAS = {
    "an_kpi_operadora": f"""
        WITH base AS (
            SELECT fecha_mes, operadora, sustraidos, recuperados
            FROM gold_operadora_mes
            WHERE operadora IN {OPERADORAS} AND sustraidos IS NOT NULL),
        corte AS (SELECT MAX(fecha_mes) AS ult FROM base),
        u12 AS (
            SELECT b.operadora, SUM(b.sustraidos) AS sus12, SUM(b.recuperados) AS rec12
            FROM base b CROSS JOIN corte c
            WHERE b.fecha_mes > add_months(c.ult, -12)
            GROUP BY b.operadora),
        p12 AS (
            SELECT b.operadora, SUM(b.sustraidos) AS sus_prev12
            FROM base b CROSS JOIN corte c
            WHERE b.fecha_mes <= add_months(c.ult, -12) AND b.fecha_mes > add_months(c.ult, -24)
            GROUP BY b.operadora),
        t AS (
            SELECT u.operadora, u.sus12, u.rec12, p.sus_prev12, 100.0 * u.rec12 / u.sus12 AS tasa
            FROM u12 u JOIN p12 p ON u.operadora = p.operadora),
        m AS (SELECT PERCENTILE(tasa, 0.5) AS mediana FROM t)
        SELECT t.operadora,
               c.ult AS corte,
               CAST(t.sus12 AS BIGINT) AS sus12,
               CAST(t.rec12 AS BIGINT) AS rec12,
               CAST(t.sus_prev12 AS BIGINT) AS sus_prev12,
               ROUND(t.tasa, 2) AS tasa12,
               ROUND(100.0 * (t.sus12 / t.sus_prev12 - 1), 1) AS var_anual,
               ROUND(m.mediana, 2) AS mediana_tasa,
               ROUND(m.mediana - t.tasa, 2) AS brecha_vs_mediana,
               CAST(ROUND(GREATEST(0.0, m.mediana - t.tasa) / 100.0 * t.sus12) AS INT) AS potencial,
               CASE WHEN m.mediana - t.tasa > 1 THEN 'Prioritaria'
                    WHEN m.mediana - t.tasa > 0 THEN 'En seguimiento'
                    ELSE 'Sobre la mediana' END AS estado
        FROM t CROSS JOIN m CROSS JOIN corte c
        ORDER BY t.operadora""",

    "an_serie_mensual": f"""
        SELECT g.fecha_mes, g.operadora, t.anio, t.trimestre, t.periodo_covid,
               g.sustraidos, g.recuperados, g.reclamos_moviles,
               ROUND(100.0 * g.recuperados / g.sustraidos, 2) AS tasa_recuperacion_pct,
               ROUND(AVG(g.sustraidos) OVER (PARTITION BY g.operadora ORDER BY g.fecha_mes
                                             ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 1) AS sus_media_movil_3m,
               ROUND(100.0 * (g.sustraidos / LAG(g.sustraidos, 12) OVER (PARTITION BY g.operadora ORDER BY g.fecha_mes) - 1), 1)
                   AS var_interanual_pct
        FROM gold_operadora_mes g
        JOIN dim_tiempo t ON g.fecha_mes = t.fecha_mes
        WHERE g.operadora IN {OPERADORAS} AND g.sustraidos IS NOT NULL
        ORDER BY g.operadora, g.fecha_mes""",

    "an_resumen_anual": f"""
        SELECT t.anio, g.operadora,
               SUM(g.sustraidos) AS sustraidos, SUM(g.recuperados) AS recuperados,
               ROUND(100.0 * SUM(g.recuperados) / SUM(g.sustraidos), 2) AS tasa_recuperacion_pct,
               SUM(g.reclamos_moviles) AS reclamos_moviles
        FROM gold_operadora_mes g
        JOIN dim_tiempo t ON g.fecha_mes = t.fecha_mes
        WHERE g.operadora IN {OPERADORAS}
        GROUP BY t.anio, g.operadora
        ORDER BY t.anio, g.operadora""",

    "an_depto_calidad": """
        SELECT departamento,
               SUM(ccpp) AS ccpp, SUM(ccpp_4g) AS ccpp_4g,
               SUM(reclamos_moviles) AS reclamos_moviles, SUM(reclamos_calidad) AS reclamos_calidad,
               ROUND(100.0 * SUM(reclamos_calidad) / SUM(reclamos_moviles), 1) AS pct_calidad,
               ROUND(100.0 * SUM(ccpp_4g) / NULLIF(SUM(ccpp), 0), 1) AS pct_4g
        FROM gold_depto_operadora_202303
        GROUP BY departamento
        HAVING SUM(reclamos_moviles) > 0
        ORDER BY reclamos_moviles DESC""",
}

# Consultas de ejemplo para la demostración (lo que haría un analista contra las tablas analíticas).
EJEMPLOS = {
    "¿Qué operadora tiene la mayor brecha de recuperación frente a la mediana?":
        "SELECT operadora, tasa12, mediana_tasa, brecha_vs_mediana, potencial, estado FROM an_kpi_operadora ORDER BY brecha_vs_mediana DESC",
    "¿Cuáles son los 5 meses con más equipos sustraídos (las 4 operadoras)?":
        "SELECT fecha_mes, SUM(sustraidos) AS sustraidos FROM an_serie_mensual GROUP BY fecha_mes ORDER BY sustraidos DESC LIMIT 5",
    "¿Cómo cambió el promedio mensual durante el periodo COVID?":
        "SELECT periodo_covid, ROUND(AVG(sustraidos), 0) AS promedio_mensual, COUNT(*) AS observaciones FROM an_serie_mensual GROUP BY periodo_covid",
    "¿En qué departamentos pesa más la calidad del servicio? (≥ 1 000 reclamos)":
        "SELECT departamento, reclamos_moviles, pct_calidad, pct_4g FROM an_depto_calidad WHERE reclamos_moviles >= 1000 ORDER BY pct_calidad DESC LIMIT 5",
}


def registrar_vistas(spark):
    """Expone las tablas GOLD como vistas SQL (spark.sql puede consultarlas por nombre)."""
    from config import ruta
    for v in VISTAS:
        spark.read.parquet(ruta("gold", v)).createOrReplaceTempView(v)


def existen(spark):
    from incremental import leer_o_none
    return all(leer_o_none(spark, "gold", n) is not None for n in CONSULTAS)


def construir(spark, salida_csv: Path):
    """Ejecuta cada consulta SQL sobre GOLD y guarda la tabla analítica en el lake (Parquet) y como CSV."""
    import gold
    registrar_vistas(spark)
    salida_csv.mkdir(parents=True, exist_ok=True)
    for nombre, sql in CONSULTAS.items():
        t = time.perf_counter()
        gold.escribir(spark.sql(sql), "gold", nombre)
        tabla = leer_uno(spark, nombre)
        tabla.to_csv(salida_csv / f"{nombre}.csv", index=False)
        print(f"[ANALÍTICA] {nombre}: {len(tabla):,} filas ({time.perf_counter() - t:.1f} s)")


def leer_uno(spark, nombre):
    from config import ruta
    return spark.read.parquet(ruta("gold", nombre)).toPandas()


def leer(spark):
    """Las tablas analíticas como DataFrames de pandas, para el tablero."""
    return {n: leer_uno(spark, n) for n in CONSULTAS}


def consultas_ejemplo(spark):
    from config import ruta
    for n in CONSULTAS:
        spark.read.parquet(ruta("gold", n)).createOrReplaceTempView(n)
    for pregunta, sql in EJEMPLOS.items():
        print(f"\n>>> {pregunta}\n    {' '.join(sql.split())}")
        spark.sql(sql).show(truncate=False)


# ------------------------------------------------------------------ reconciliación (lógica pura, sin Spark)
TOLERANCIAS = {"tasa12": 0.011, "var_anual": 0.11, "brecha_vs_mediana": 0.011, "potencial": 1}


def reconciliar(desde_pandas: list, desde_sql: list):
    """Compara las operadoras calculadas con pandas contra las de las tablas analíticas (SQL).
    Los enteros deben coincidir exactamente; las razones, dentro de la tolerancia de redondeo."""
    a = {o["op"]: o for o in desde_pandas}
    b = {o["op"]: o for o in desde_sql}
    dif = {}
    ok = set(a) == set(b)
    for op in sorted(set(a) & set(b)):
        for campo in ("sus12", "rec12"):
            if a[op][campo] != b[op][campo]:
                ok = False
                dif[f"{op}.{campo}"] = (a[op][campo], b[op][campo])
        if a[op]["estado"] != b[op]["estado"]:
            ok = False
            dif[f"{op}.estado"] = (a[op]["estado"], b[op]["estado"])
        for campo, tol in TOLERANCIAS.items():
            d = abs(a[op][campo] - b[op][campo])
            if d > tol:
                ok = False
                dif[f"{op}.{campo}"] = (a[op][campo], b[op][campo])
    return {"ok": ok, "operadoras": sorted(b), "diferencias": dif}


if __name__ == "__main__":
    import argparse, os, sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from config import EVIDENCIA
    from pipeline import spark_session
    ap = argparse.ArgumentParser()
    ap.add_argument("--consultas", action="store_true", help="corre consultas de ejemplo sobre las tablas analíticas")
    a = ap.parse_args()
    sp = spark_session("OSIPTEL_analitica")
    construir(sp, EVIDENCIA / "analitica")
    if a.consultas:
        consultas_ejemplo(sp)
