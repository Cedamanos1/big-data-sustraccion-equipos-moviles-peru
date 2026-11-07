"""Modelo base y modelo definitivo: pronóstico de equipos sustraídos por operadora.

Entrada: GOLD gold_operadora_mes (≈ 600 filas). Se baja a pandas porque el conjunto
agregado cabe holgadamente en memoria; entrenar en Spark ML no aportaría paralelismo útil
(ver informe §1.7 y §4.3).

Formulación: se predice la VARIACIÓN RELATIVA respecto del último mes conocido
    y = sustraidos_t / sustraidos_{t-H} - 1
y luego se reconstruye el nivel. Los árboles no extrapolan niveles; predecir la variación
evita ese problema. H = 1 (principal) y H = 3 (secundario).
Partición temporal: entrenamiento hasta jun-2024 (excluye el choque COVID mar-2020 a mar-2021),
prueba jul-2024 a ene-2026. Hiperparámetros por validación cruzada temporal (TimeSeriesSplit).
"""
import json
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.model_selection import TimeSeriesSplit, GridSearchCV

OPS = ["BITEL", "CLARO", "ENTEL", "MOVISTAR"]
INICIO_PRUEBA = "2024-07-01"
COVID = ("2020-03-01", "2021-03-01")
SEED = 42
MODELOS = ["ingenuo", "ingenuo_estacional", "ridge", "random_forest", "gradient_boosting"]


def preparar(g: pd.DataFrame, H: int, futuro: bool = False):
    """Construye variables. Con futuro=True agrega, por operadora, la fila del mes siguiente al último
    observado (sin valor real) para poder pronosticarlo."""
    d = g[g.operadora.isin(OPS) & g.sustraidos.notna()][["fecha_mes", "operadora", "sustraidos"]]
    if futuro:
        ult = d.groupby("operadora", as_index=False).fecha_mes.max()
        ult["fecha_mes"] = ult.fecha_mes + pd.DateOffset(months=H)
        ult["sustraidos"] = np.nan
        d = pd.concat([d, ult], ignore_index=True)
    d = d.sort_values(["operadora", "fecha_mes"]).copy()
    s = d.groupby("operadora").sustraidos
    d["base"] = s.shift(H)                                   # último valor conocido al pronosticar
    d["y"] = d.sustraidos / d.base - 1
    for k in range(H, H + 3):
        d[f"var_lag{k}"] = s.shift(k) / s.shift(k + 1) - 1  # variaciones recientes
    d["var_estacional"] = s.shift(12) / s.shift(12 + H) - 1  # misma variación un año antes
    d["nivel_vs_media12"] = s.shift(H) / s.transform(lambda x: x.shift(H).rolling(12).mean()) - 1
    d["mes"] = d.fecha_mes.dt.month
    d["covid"] = d.fecha_mes.between(*COVID).astype(int)
    feats = [c for c in d.columns if c.startswith("var_")] + ["nivel_vs_media12", "mes", "covid"]
    d = d.replace([np.inf, -np.inf], np.nan)
    if futuro:
        return d[d.sustraidos.isna()].dropna(subset=feats + ["base"]), d.dropna(), feats
    return d.dropna(), feats


def metricas(y, p, escala):
    e = np.abs(y - p)
    return dict(MAE=round(float(e.mean()), 1), MAPE=round(float(100 * (e / y).mean()), 2),
                MASE=round(float(e.mean() / escala), 3), sesgo=round(float((p - y).mean()), 1))


def evaluar(g: pd.DataFrame, H: int):
    d, feats = preparar(g, H)
    tr = d[(d.fecha_mes < INICIO_PRUEBA) & ~d.fecha_mes.between(*COVID)].sort_values(["fecha_mes", "operadora"], kind="stable")
    te = d[d.fecha_mes >= INICIO_PRUEBA]
    escala = float(np.abs(tr.sustraidos - tr.base).mean())   # MAE del ingenuo en entrenamiento (para MASE)
    tscv = TimeSeriesSplit(n_splits=4)
    modelos = {
        "ridge": GridSearchCV(Ridge(), {"alpha": [0.1, 1, 10]}, cv=tscv, scoring="neg_mean_absolute_error"),
        "random_forest": GridSearchCV(RandomForestRegressor(random_state=SEED),
                                      {"n_estimators": [300], "max_depth": [3, 6, None], "min_samples_leaf": [3, 5, 10]},
                                      cv=tscv, scoring="neg_mean_absolute_error"),
        "gradient_boosting": GridSearchCV(GradientBoostingRegressor(random_state=SEED),
                                          {"n_estimators": [100, 200, 400], "max_depth": [2, 3], "learning_rate": [0.03, 0.1]},
                                          cv=tscv, scoring="neg_mean_absolute_error"),
    }
    preds = te[["fecha_mes", "operadora", "sustraidos"]].copy()
    preds["ingenuo"] = te.base
    preds["ingenuo_estacional"] = te.base * (1 + te.var_estacional)
    mejores = {}
    for n, m in modelos.items():
        m.fit(tr[feats], tr.y)
        preds[n] = te.base * (1 + m.predict(te[feats]))
        mejores[n] = m.best_params_
    res = {}
    for n in MODELOS:
        res[n] = metricas(preds.sustraidos.values, preds[n].values, escala)
        res[n]["MAPE_por_operadora"] = {op: round(float(100 * (np.abs(x.sustraidos - x[n]) / x.sustraidos).mean()), 2)
                                        for op, x in preds.groupby("operadora")}
    mae_op = {op: round(float(np.abs(x.sustraidos - x["gradient_boosting"]).mean()), 1) for op, x in preds.groupby("operadora")}
    gbm = modelos["gradient_boosting"].best_estimator_
    imp = dict(sorted(zip(feats, gbm.feature_importances_.round(3).tolist()), key=lambda x: -x[1]))
    info = dict(horizonte_meses=H, n_train=len(tr), n_test=len(te),
                train=f"{tr.fecha_mes.min().date()} a {tr.fecha_mes.max().date()} (sin {COVID[0][:7]} a {COVID[1][:7]})",
                test=f"{te.fecha_mes.min().date()} a {te.fecha_mes.max().date()}",
                escala_MASE=round(escala, 1), hiperparametros=mejores, metricas=res, importancias_gb=imp,
                MAE_prueba_por_operadora=mae_op)
    return preds, info


def pronostico_siguiente(g: pd.DataFrame, info_h1: dict):
    """Reentrena el modelo elegido (Gradient Boosting, hiperparámetros elegidos en la validación)
    con TODA la historia disponible (sin el periodo COVID) y pronostica el mes siguiente al último dato.
    El intervalo usa el MAE de prueba de cada operadora (≈ error esperado)."""
    futuro, hist, feats = preparar(g, 1, futuro=True)
    tr = hist[~hist.fecha_mes.between(*COVID)].sort_values(["fecha_mes", "operadora"], kind="stable")
    gb = GradientBoostingRegressor(random_state=SEED, **info_h1["hiperparametros"]["gradient_boosting"]).fit(tr[feats], tr.y)
    out = futuro[["fecha_mes", "operadora", "base"]].copy()
    out["pronostico"] = (futuro.base * (1 + gb.predict(futuro[feats]))).round()
    out["mae_prueba"] = out.operadora.map(info_h1["MAE_prueba_por_operadora"])
    out["variacion_pct"] = (100 * (out.pronostico / out.base - 1)).round(1)
    return out.rename(columns={"base": "ultimo_observado"})


def ejecutar(g: pd.DataFrame, salida: Path):
    """g: gold_operadora_mes en pandas. Escribe métricas y predicciones en `salida`."""
    g = g.copy()
    g["fecha_mes"] = pd.to_datetime(g["fecha_mes"])
    salida.mkdir(parents=True, exist_ok=True)
    info = {}
    for H in (1, 3):
        preds, info[f"H{H}"] = evaluar(g, H)
        preds.to_csv(salida / f"predicciones_prueba_H{H}.csv", index=False)
    (salida / "modelo_metricas.json").write_text(json.dumps(info, indent=2, ensure_ascii=False), encoding="utf-8")
    prox = pronostico_siguiente(g, info["H1"])
    prox.to_csv(salida / "pronostico_mes_siguiente.csv", index=False)
    print("[PRONÓSTICO]", prox.assign(fecha_mes=prox.fecha_mes.dt.date).to_dict("records"))
    for h, v in info.items():
        print(f"[MODELO {h}] " + " | ".join(f"{m}: MAE={r['MAE']:,.0f} MAPE={r['MAPE']}%" for m, r in v["metricas"].items()))
    return info


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from config import ruta, EVIDENCIA
    spark = SparkSession.builder.appName("OSIPTEL_modelo").getOrCreate()
    g = spark.read.parquet(ruta("gold", "gold_operadora_mes")).toPandas()
    ejecutar(g, EVIDENCIA)
