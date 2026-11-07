"""Contraste para §1.7 y §3.7: el MISMO pipeline (SILVER + JOIN de GOLD) hecho en pandas, que trabaja
todo en memoria, sobre réplicas 1×, 10×, 25×, 50×, 75×… de los datos reales.

    python src/benchmark_pandas.py --factores 1 10 25 50 75 100

Sirve para medir en qué factor una herramienta en memoria deja de caber en esta máquina, y compararlo con
Spark (src/benchmark.py), que procesa por particiones. Cada factor corre en un proceso aparte con un límite
de memoria (85 % de la RAM) para que, al llenarse, falle con MemoryError en vez de congelar la VM.
Las réplicas solo miden rendimiento: no se guardan ni se usan en el análisis.
Requiere haber corrido antes src/pipeline.py (lee data/staging y el CSV de cobertura en data/raw).
"""
import argparse, json, os, resource, subprocess, sys, time
from pathlib import Path

SRC = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SRC)
from config import STAGING, FUENTES, EVIDENCIA
import reglas

BENCH = EVIDENCIA / "benchmark"


def _mapear(serie, funcion):
    """Aplica una regla de reglas.py sobre los valores únicos (rápido) y la propaga a toda la columna."""
    unicos = serie.dropna().unique()
    return serie.map(dict(zip(unicos, map(funcion, unicos))))


def un_factor(k, limite_bytes):
    resource.setrlimit(resource.RLIMIT_AS, (limite_bytes, limite_bytes))
    import pandas as pd
    t0 = time.perf_counter()
    rob = pd.read_csv(STAGING / "robos" / "robos.csv", dtype=str)
    rec = pd.read_csv(STAGING / "reclamos" / "reclamos.csv", dtype=str)
    cob = pd.read_csv(next(FUENTES["cobertura"].glob("*.csv")), sep=";", encoding="latin-1", dtype=str)
    filas = (len(rob) + len(rec) + len(cob)) * k
    # réplicas en memoria (lo que haría un analista con pandas si los datos crecieran)
    rob = pd.concat([rob.assign(copia=i) for i in range(k)], ignore_index=True)
    rec = pd.concat([rec.assign(copia=i) for i in range(k)], ignore_index=True)
    cob = pd.concat([cob.assign(copia=i) for i in range(k)], ignore_index=True)
    t_lectura = time.perf_counter() - t0

    t1 = time.perf_counter()
    # SILVER: mismas reglas que transform.py
    rob["operadora"] = _mapear(rob.empresa, reglas.operadora)
    rob["estado_n"] = _mapear(rob.estado, reglas.estado)
    rob["fecha_mes"] = pd.to_datetime(rob.mes.str[:10], errors="coerce")
    rob["cantidad"] = pd.to_numeric(rob.cantidad, errors="coerce")
    s_rob = rob.groupby(["copia", "fecha_mes", "operadora", "estado_n"], as_index=False).cantidad.sum()
    rec["operadora"] = _mapear(rec.empresa, reglas.operadora)
    rec["departamento_n"] = _mapear(rec.departamento, reglas.departamento)
    rec["materia_n"] = _mapear(rec.materia, reglas.materia)
    rec["es_movil"] = _mapear(rec.servicio, reglas.normalizar_texto).eq("SERVICIOS MOVILES")
    rec["fecha_mes"] = pd.to_datetime(rec.mes.str[:10], errors="coerce")
    rec["reclamos"] = pd.to_numeric(rec.reclamos, errors="coerce")
    cob["operadora"] = _mapear(cob.EMPRESA_OPERADORA, reglas.operadora)
    cob["departamento_n"] = _mapear(cob.DEPARTAMENTO, reglas.departamento)
    cob["tiene_4g"] = pd.to_numeric(cob["4G"], errors="coerce")
    cob = cob.drop_duplicates(["copia", "EMPRESA_OPERADORA", "UBIGEO_CCPP"])
    t_silver = time.perf_counter() - t1

    t2 = time.perf_counter()
    # GOLD: los dos JOIN a la misma granularidad
    piv = s_rob.pivot_table(index=["copia", "fecha_mes", "operadora"], columns="estado_n", values="cantidad", aggfunc="sum").reset_index()
    mov = rec[rec.es_movil]
    j1 = piv.merge(mov.groupby(["copia", "fecha_mes", "operadora"], as_index=False).reclamos.sum(),
                   on=["copia", "fecha_mes", "operadora"], how="outer")
    r23 = mov[(mov.fecha_mes == "2023-03-01") & mov.departamento_n.notna()]
    c23 = cob.groupby(["copia", "departamento_n", "operadora"], as_index=False).agg(ccpp=("UBIGEO_CCPP", "count"), ccpp_4g=("tiene_4g", "sum"))
    j2 = r23.groupby(["copia", "departamento_n", "operadora"], as_index=False).reclamos.sum().merge(c23, how="left")
    t_gold = time.perf_counter() - t2
    import psutil
    out = {"factor": k, "filas_entrada": filas, "filas_gold": int(len(j1) + len(j2)),
           "tiempo_lectura_replica_s": round(t_lectura, 1), "tiempo_silver_s": round(t_silver, 1),
           "tiempo_gold_s": round(t_gold, 1), "tiempo_total_s": round(t_lectura + t_silver + t_gold, 1),
           "rss_proceso_mb": round(psutil.Process().memory_info().rss / 1e6)}
    (BENCH / f"pandas_resultado_x{k}.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
    print("[RESULTADO]", out, flush=True)


def serie(factores, continuar):
    import psutil
    (BENCH / "logs").mkdir(parents=True, exist_ok=True)
    total = psutil.virtual_memory().total
    limite = int(total * 0.85)
    res = {"herramienta": "pandas (en memoria, un solo proceso)", "ram_total_mb": round(total / 1e6),
           "limite_memoria_proceso_mb": round(limite / 1e6), "factores": []}
    for k in factores:
        log = BENCH / "logs" / f"pandas_x{k}.log"
        print(f"\n[PANDAS] factor {k}× → {log}", flush=True)
        t0 = time.perf_counter(); pico = 0
        with open(log, "w", encoding="utf-8") as fh:
            p = subprocess.Popen([sys.executable, __file__, "--un-factor", str(k), "--limite", str(limite)], stdout=fh, stderr=subprocess.STDOUT)
            proc = psutil.Process(p.pid)
            while p.poll() is None:
                try:
                    pico = max(pico, proc.memory_info().rss)
                except psutil.Error:
                    pass
                time.sleep(1)
        rc = p.returncode
        texto = log.read_text(encoding="utf-8", errors="ignore")
        if rc == 0:
            estado = "completado"
        elif "MemoryError" in texto or "Unable to allocate" in texto:
            estado = "MemoryError: no cabe en memoria"
        elif rc in (137, -9):
            estado = "eliminado por el sistema operativo (sin memoria)"
        else:
            estado = f"error (código {rc}); ver log"
        fila = {"factor": k, "estado": estado, "duracion_proceso_s": round(time.perf_counter() - t0, 1), "rss_pico_mb": round(pico / 1e6)}
        r = BENCH / f"pandas_resultado_x{k}.json"
        if rc == 0 and r.exists():
            fila.update(json.loads(r.read_text(encoding="utf-8")))
        res["factores"].append(fila)
        (EVIDENCIA / "escalabilidad_pandas.json").write_text(json.dumps(res, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"[PANDAS] {k}×: {estado} · {fila['duracion_proceso_s']} s · memoria pico del proceso {fila['rss_pico_mb']} MB", flush=True)
        if estado != "completado" and not continuar:
            print("[PANDAS] Se encontró el límite de memoria de esta máquina; se detiene la prueba.")
            break


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--factores", type=int, nargs="+", default=[1, 10, 25, 50, 75, 100])
    ap.add_argument("--continuar-tras-falla", action="store_true")
    ap.add_argument("--un-factor", type=int, help=argparse.SUPPRESS)
    ap.add_argument("--limite", type=int, help=argparse.SUPPRESS)
    a = ap.parse_args()
    BENCH.mkdir(parents=True, exist_ok=True)
    if a.un_factor:
        un_factor(a.un_factor, a.limite)
    else:
        serie(a.factores, a.continuar_tras_falla)
