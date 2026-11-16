"""Modelo con Spark MLlib: pronóstico de equipos sustraídos por operadora, comparado con scikit-learn.

Usa EXACTAMENTE las mismas variables, la misma variable objetivo (variación relativa respecto del último mes
conocido), la misma partición temporal y la misma validación cruzada temporal que src/modelo.py, para que la
comparación sea justa. Lo único que cambia es la librería que entrena: Spark MLlib (GBTRegressor y
RandomForestRegressor) en lugar de scikit-learn.

Entrada : GOLD gold_operadora_mes (≈ 600 filas, ya agregada en Spark).
Salida  : docs/evidencia/modelo_mllib_metricas.json, predicciones_prueba_mllib_H{1,3}.csv y
          pronostico_mes_siguiente_mllib.csv; imprime la tabla comparativa contra scikit-learn.

Uso (desde la raíz del repositorio):
    python src/modelo_spark.py

Nota honesta de ingeniería: con ≈ 600 filas el entrenamiento no necesita un clúster; MLlib se usa porque el diseño
debe poder crecer (si GOLD tuviera millones de filas el mismo código se distribuye sin cambios) y para documentar
la comparación con scikit-learn. Mallas de hiperparámetros más pequeñas que las de scikit-learn para acotar el tiempo.
"""
import json
from pathlib import Path

SEED = 42
N_SPLITS = 4
MALLA_GBT = dict(maxIter=[100], maxDepth=[2, 3], stepSize=[0.03, 0.1])
MALLA_RF = dict(numTrees=[100], maxDepth=[3, 6], minInstancesPerNode=[3, 5])


def particiones_temporales(n: int, n_splits: int = N_SPLITS):
    """Pliegues de validación cruzada temporal de ventana creciente (misma regla que sklearn.TimeSeriesSplit).
    Devuelve [(fin_entrenamiento, ini_validacion, fin_validacion), ...] sobre posiciones 0..n-1 (fin exclusivo)."""
    tam = n // (n_splits + 1)
    inicio = n - n_splits * tam
    return [(i, i, i + tam) for i in range(inicio, n, tam)]


def _a_spark(spark, df, cols):
    """pandas -> Spark sin depender de pyarrow: lista de filas de float."""
    # una sola partición: con ≈ 600 filas, repartir en tareas solo agrega sobrecarga a cada iteración de los árboles
    return spark.createDataFrame(df[cols].astype("float64").values.tolist(), cols).coalesce(1)


def _combinaciones(malla: dict):
    claves, listas = list(malla), list(malla.values())
    out = [{}]
    for k, vals in zip(claves, listas):
        out = [dict(o, **{k: v}) for o in out for v in vals]
    return out


def _ajustar(spark, tr, te, feats, nombre, malla):
    """Elige hiperparámetros por validación cruzada temporal (MAE sobre la variación) y devuelve
    (predicciones sobre te, mejores parámetros, importancias)."""
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
    from pyspark.ml.evaluation import RegressionEvaluator
    from pyspark.sql import functions as F

    def crear(params):
        if nombre == "gbt":
            return GBTRegressor(featuresCol="features", labelCol="y", seed=SEED, **params)
        return RandomForestRegressor(featuresCol="features", labelCol="y", seed=SEED, **params)

    ens = VectorAssembler(inputCols=feats, outputCol="features")
    ev = RegressionEvaluator(labelCol="y", predictionCol="prediction", metricName="mae")
    cols = feats + ["y", "_i"]
    sp_tr = ens.transform(_a_spark(spark, tr.reset_index(drop=True).assign(_i=range(len(tr))), cols)).cache()

    import time
    puntajes, combos = [], _combinaciones(malla)
    for k, params in enumerate(combos, 1):
        t0, maes = time.perf_counter(), []
        for fin_tr, ini_va, fin_va in particiones_temporales(len(tr)):
            m = crear(params).fit(sp_tr.filter(F.col("_i") < fin_tr))
            va = sp_tr.filter((F.col("_i") >= ini_va) & (F.col("_i") < fin_va))
            maes.append(ev.evaluate(m.transform(va)))
        puntajes.append((sum(maes) / len(maes), params))
        print(f"[MLLIB] {nombre} combinación {k}/{len(combos)} {params} → MAE(variación)={puntajes[-1][0]:.4f} ({time.perf_counter() - t0:.0f} s)", flush=True)
    _, mejores = min(puntajes, key=lambda x: x[0])

    modelo = crear(mejores).fit(sp_tr)
    sp_te = ens.transform(_a_spark(spark, te.reset_index(drop=True).assign(y=0.0, _i=range(len(te))), cols))
    pred = {int(r["_i"]): float(r["prediction"]) for r in modelo.transform(sp_te).select("_i", "prediction").collect()}
    importancias = dict(sorted(zip(feats, [round(float(x), 3) for x in modelo.featureImportances.toArray()]),
                               key=lambda x: -x[1]))
    sp_tr.unpersist()
    return [pred[i] for i in range(len(te))], mejores, importancias


def evaluar(spark, g, H):
    import numpy as np
    import modelo as m
    d, feats = m.preparar(g, H)
    tr = d[(d.fecha_mes < m.INICIO_PRUEBA) & ~d.fecha_mes.between(*m.COVID)].sort_values(["fecha_mes", "operadora"], kind="stable")
    te = d[d.fecha_mes >= m.INICIO_PRUEBA]
    escala = float(np.abs(tr.sustraidos - tr.base).mean())
    preds = te[["fecha_mes", "operadora", "sustraidos"]].copy()
    preds["ingenuo"] = te.base
    res, params, imp = {}, {}, {}
    for nombre, etiqueta, malla in [("gbt", "mllib_gbt", MALLA_GBT), ("rf", "mllib_random_forest", MALLA_RF)]:
        var, params[etiqueta], imp[etiqueta] = _ajustar(spark, tr, te, feats, nombre, malla)
        preds[etiqueta] = te.base.values * (1 + np.array(var))
    for n in ["ingenuo", "mllib_gbt", "mllib_random_forest"]:
        res[n] = m.metricas(preds.sustraidos.values, preds[n].values, escala)
        res[n]["MAPE_por_operadora"] = {op: round(float(100 * (np.abs(x.sustraidos - x[n]) / x.sustraidos).mean()), 2)
                                        for op, x in preds.groupby("operadora")}
    mae_op = {op: round(float(np.abs(x.sustraidos - x["mllib_gbt"]).mean()), 1) for op, x in preds.groupby("operadora")}
    info = dict(horizonte_meses=H, n_train=len(tr), n_test=len(te), hiperparametros=params, metricas=res,
                importancias_gbt=imp["mllib_gbt"], MAE_prueba_por_operadora=mae_op)
    return preds, info


def pronostico_siguiente(spark, g, info_h1):
    """Reentrena el GBT de MLlib con toda la historia (sin COVID) y pronostica el mes siguiente."""
    import modelo as m
    futuro, hist, feats = m.preparar(g, 1, futuro=True)
    tr = hist[~hist.fecha_mes.between(*m.COVID)].sort_values(["fecha_mes", "operadora"], kind="stable")
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.regression import GBTRegressor
    cols = feats + ["y"]
    ens = VectorAssembler(inputCols=feats, outputCol="features")
    modelo = GBTRegressor(featuresCol="features", labelCol="y", seed=SEED,
                          **info_h1["hiperparametros"]["mllib_gbt"]).fit(ens.transform(_a_spark(spark, tr, cols)))
    fu = futuro.reset_index(drop=True).assign(y=0.0, _i=range(len(futuro)))
    pred = {int(r["_i"]): float(r["prediction"]) for r in
            modelo.transform(ens.transform(_a_spark(spark, fu, feats + ["y", "_i"]))).select("_i", "prediction").collect()}
    out = futuro[["fecha_mes", "operadora", "base"]].reset_index(drop=True).copy()
    out["pronostico"] = [round(b * (1 + pred[i])) for i, b in enumerate(out.base)]
    out["mae_prueba"] = out.operadora.map(info_h1["MAE_prueba_por_operadora"])
    out["variacion_pct"] = (100 * (out.pronostico / out.base - 1)).round(1)
    return out.rename(columns={"base": "ultimo_observado"})


def comparar(info_mllib: dict, ruta_sklearn: Path):
    """Tabla MAPE/MAE de scikit-learn frente a MLlib para cada horizonte (None si no hay métricas de scikit-learn)."""
    if not ruta_sklearn.exists():
        return None
    sk = json.loads(ruta_sklearn.read_text(encoding="utf-8"))
    tabla = {}
    for h, v in info_mllib.items():
        filas = {n: dict(MAE=r["MAE"], MAPE=r["MAPE"]) for n, r in sk[h]["metricas"].items()}
        filas.update({n: dict(MAE=r["MAE"], MAPE=r["MAPE"]) for n, r in v["metricas"].items() if n != "ingenuo"})
        mejor = min(filas, key=lambda n: filas[n]["MAPE"])
        tabla[h] = dict(modelos=filas, mejor_por_MAPE=mejor)
    return tabla


def sesion(app="OSIPTEL_modelo_mllib"):
    """Sesión local para MLlib. -Xss16m evita el StackOverflowError del GBT (el boosting encadena iteraciones y la
    pila de Java por defecto no alcanza tras muchos ajustes seguidos)."""
    import os
    from pyspark.sql import SparkSession
    b = (SparkSession.builder.appName(app)
         .config("spark.driver.extraJavaOptions", "-Xss16m")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.session.timeZone", "America/Lima")
         .config("spark.ui.showConsoleProgress", "false"))
    if os.environ.get("SPARK_DRIVER_MEMORY"):
        b = b.config("spark.driver.memory", os.environ["SPARK_DRIVER_MEMORY"])
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def ejecutar(spark, g, salida: Path):
    import tempfile
    import pandas as pd
    # con directorio de checkpoint el GBT corta el linaje cada 10 iteraciones (checkpointInterval=10 por defecto)
    spark.sparkContext.setCheckpointDir(tempfile.mkdtemp(prefix="mllib_ckpt_"))
    g = g.copy()
    g["fecha_mes"] = pd.to_datetime(g["fecha_mes"])
    salida.mkdir(parents=True, exist_ok=True)
    info = {}
    for H in (1, 3):
        preds, info[f"H{H}"] = evaluar(spark, g, H)
        preds.to_csv(salida / f"predicciones_prueba_mllib_H{H}.csv", index=False)
    prox = pronostico_siguiente(spark, g, info["H1"])
    prox.to_csv(salida / "pronostico_mes_siguiente_mllib.csv", index=False)
    comp = comparar(info, salida / "modelo_metricas.json")
    (salida / "modelo_mllib_metricas.json").write_text(
        json.dumps(dict(spark=spark.version, resultados=info, comparacion_con_sklearn=comp), indent=2, ensure_ascii=False),
        encoding="utf-8")
    print("[MLLIB PRONÓSTICO]", prox.assign(fecha_mes=prox.fecha_mes.dt.date).to_dict("records"))
    for h in info:
        print(f"[MLLIB {h}] " + " | ".join(f"{n}: MAE={r['MAE']:,.0f} MAPE={r['MAPE']}%" for n, r in info[h]["metricas"].items()))
        if comp:
            print(f"[COMPARACIÓN {h}] " + " | ".join(f"{n}: {r['MAPE']}%" for n, r in comp[h]["modelos"].items())
                  + f"  → mejor: {comp[h]['mejor_por_MAPE']}")
    return info, comp


if __name__ == "__main__":
    import os, sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from config import EVIDENCIA, ruta
    sp = sesion()
    ejecutar(sp, sp.read.parquet(ruta("gold", "gold_operadora_mes")).toPandas(), EVIDENCIA)
