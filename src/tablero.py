"""Capa de consumo: genera el tablero interactivo docs/index.html a partir de GOLD.

    python src/tablero.py

El HTML es autocontenido (datos embebidos, sin librerías externas): se abre con doble clic
o se publica con GitHub Pages (Settings → Pages → rama main, carpeta /docs).
Lee SOLO: gold_operadora_mes, gold_depto_operadora_202303, modelo_metricas.json,
predicciones_prueba_H1.csv y pronostico_mes_siguiente.csv (nunca RAW ni _benchmark).
"""
import json, os, sys
from datetime import datetime
from pathlib import Path
import numpy as np
import pandas as pd

OPS = ["BITEL", "CLARO", "ENTEL", "MOVISTAR"]
PLANTILLA = Path(__file__).resolve().parent / "tablero_plantilla.html"


def construir_datos(g: pd.DataFrame, j: pd.DataFrame, evid: Path, manifiesto: dict | None = None) -> dict:
    g = g.copy(); g["fecha_mes"] = pd.to_datetime(g["fecha_mes"])
    g4 = g[g.operadora.isin(OPS) & g.sustraidos.notna()].sort_values(["fecha_mes", "operadora"])
    ult = g4.fecha_mes.max()
    u12 = g4[g4.fecha_mes > ult - pd.DateOffset(months=12)]
    p12 = g4[(g4.fecha_mes <= ult - pd.DateOffset(months=12)) & (g4.fecha_mes > ult - pd.DateOffset(months=24))]

    serie = [{"m": r.fecha_mes.strftime("%Y-%m"), "op": r.operadora,
              "sus": int(r.sustraidos), "rec": None if pd.isna(r.recuperados) else int(r.recuperados),
              "recl": None if pd.isna(r.reclamos_moviles) else int(r.reclamos_moviles)} for r in g4.itertuples()]

    r12 = u12.groupby("operadora")[["sustraidos", "recuperados"]].sum()
    r12["tasa"] = 100 * r12.recuperados / r12.sustraidos
    mediana = float(r12.tasa.median())
    s12 = p12.groupby("operadora").sustraidos.sum()
    prox = pd.read_csv(evid / "pronostico_mes_siguiente.csv", parse_dates=["fecha_mes"]).set_index("operadora")
    operadoras = []
    for op, r in r12.iterrows():
        brecha = mediana - r.tasa
        operadoras.append({
            "op": op, "sus12": int(r.sustraidos), "rec12": int(r.recuperados), "tasa12": round(float(r.tasa), 2),
            "var_anual": round(float(100 * (r.sustraidos / s12[op] - 1)), 1),
            "brecha_vs_mediana": round(float(brecha), 2),
            "potencial": int(round(max(0.0, brecha) / 100 * r.sustraidos)),
            "estado": "Prioritaria" if brecha > 1 else ("En seguimiento" if brecha > 0 else "Sobre la mediana"),
            "pron": int(prox.loc[op, "pronostico"]), "pron_mae": int(round(prox.loc[op, "mae_prueba"])),
            "pron_var": float(prox.loc[op, "variacion_pct"]),
        })
    met = json.loads((evid / "modelo_metricas.json").read_text(encoding="utf-8"))["H1"]
    pr = pd.read_csv(evid / "predicciones_prueba_H1.csv", parse_dates=["fecha_mes"]).sort_values(["operadora", "fecha_mes"])
    prueba = [{"m": r.fecha_mes.strftime("%Y-%m"), "op": r.operadora, "real": int(r.sustraidos),
               "gb": int(round(r.gradient_boosting)), "ing": int(round(r.ingenuo))} for r in pr.itertuples()]
    dd = j.groupby("departamento")[["ccpp", "ccpp_4g", "reclamos_moviles", "reclamos_calidad"]].sum()
    deptos = [{"d": dep.title().replace(" Y ", " y ").replace(" De ", " de "), "recl": int(r.reclamos_moviles), "pct_cal": round(100 * r.reclamos_calidad / r.reclamos_moviles, 1),
               "pct4g": round(100 * r.ccpp_4g / r.ccpp, 1) if r.ccpp else None}
              for dep, r in dd.iterrows() if r.reclamos_moviles > 0]
    return {
        "generado": datetime.now().strftime("%d/%m/%Y %H:%M"), "corte": ult.strftime("%Y-%m"),
        "mes_pronostico": prox.fecha_mes.iloc[0].strftime("%Y-%m"),
        "kpi": {"sus_mes": round(float(u12.sustraidos.sum() / 12)), "var_anual": round(float(100 * (u12.sustraidos.sum() / p12.sustraidos.sum() - 1)), 1),
                "tasa": round(float(100 * u12.recuperados.sum() / u12.sustraidos.sum()), 2), "mediana": round(mediana, 2),
                "brecha": round(float(r12.tasa.max() - r12.tasa.min()), 1),
                "pron_total": int(prox.pronostico.sum()), "potencial_total": int(sum(o["potencial"] for o in operadoras)),
                "mape": met["metricas"]["gradient_boosting"]["MAPE"], "mape_ing": met["metricas"]["ingenuo"]["MAPE"],
                "mae": met["metricas"]["gradient_boosting"]["MAE"], "mae_ing": met["metricas"]["ingenuo"]["MAE"]},
        "operadoras": operadoras, "serie": serie, "prueba": prueba, "deptos": deptos,
        "fuentes": manifiesto or {},
    }


def generar(g, j, evid: Path, destino: Path, manifiesto=None):
    datos = construir_datos(g, j, evid, manifiesto)
    html = PLANTILLA.read_text(encoding="utf-8").replace("/*__DATOS__*/null", json.dumps(datos, ensure_ascii=False))
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(html, encoding="utf-8")
    print(f"[TABLERO] {destino} (corte {datos['corte']}, pronóstico {datos['mes_pronostico']})")
    return datos


if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from config import ruta, EVIDENCIA, ROOT
    from pipeline import spark_session
    spark = spark_session("OSIPTEL_tablero")
    man = EVIDENCIA / "manifiesto.json"
    generar(spark.read.parquet(ruta("gold", "gold_operadora_mes")).toPandas(),
            spark.read.parquet(ruta("gold", "gold_depto_operadora_202303")).toPandas(),
            EVIDENCIA, ROOT / "docs" / "index.html",
            json.loads(man.read_text(encoding="utf-8")).get("fuentes") if man.exists() else None)
