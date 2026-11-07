"""Mediciones de rendimiento (informe §3.6) y prueba de escalabilidad (informe §3.7).

    python src/benchmark.py --optimizacion                       # 6 repeticiones por variante
    python src/benchmark.py --escalabilidad --factores 1 10 100 150 200 300
    python src/graficar_benchmark.py                             # figuras para el informe

Cómo se mide:
- Optimización: 1 corrida de calentamiento (no cuenta) + N repeticiones; se reporta promedio,
  desviación estándar y mediana. La ejecución adaptativa de Spark (AQE) se desactiva en TODAS las
  corridas para que la única diferencia entre "antes" y "después" sea la técnica evaluada. Los planes
  físicos (explain) se guardan como evidencia.
- Escalabilidad: cada factor corre en un PROCESO SEPARADO. Así, si el sistema operativo mata el
  proceso por falta de memoria (código 137) o la JVM lanza OutOfMemoryError, la prueba registra la
  falla en vez de perderse. Se muestrea la RAM y el swap cada 2 s.

REGLA: las réplicas son copias de los datos REALES con una columna `copia` y se escriben en
<lake>/_benchmark. Solo sirven para medir rendimiento; NUNCA se usan en el EDA, el modelo ni el tablero.
Requiere haber corrido antes src/pipeline.py (usa STAGING y SILVER).
"""
import argparse, contextlib, io, json, os, platform, shutil, statistics, subprocess, sys, time
from datetime import datetime
from pathlib import Path

SRC = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SRC)
from config import EVIDENCIA, ruta, LAKE

BENCH = EVIDENCIA / "benchmark"


def _estadisticos(tiempos):
    return {"promedio_s": round(statistics.mean(tiempos), 3),
            "desv_estandar_s": round(statistics.stdev(tiempos), 3) if len(tiempos) > 1 else 0.0,
            "mediana_s": round(statistics.median(tiempos), 3),
            "corridas_s": [round(t, 3) for t in tiempos]}


def _medir(fn, n):
    fn()  # calentamiento: no se cuenta
    tiempos = []
    for i in range(n):
        t = time.perf_counter(); fn(); tiempos.append(time.perf_counter() - t)
        print(f"   corrida {i + 1}/{n}: {tiempos[-1]:.2f} s", flush=True)
    return _estadisticos(tiempos)


def _guardar_plan(df, nombre):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode="formatted")
    (BENCH / f"plan_{nombre}.txt").write_text(buf.getvalue(), encoding="utf-8")


def _info_maquina():
    info = {"fecha": datetime.now().isoformat(timespec="seconds"), "maquina": platform.node(),
            "so": platform.platform(), "python": platform.python_version(), "cpu_logicas": os.cpu_count()}
    try:
        import psutil
        info["ram_total_gb"] = round(psutil.virtual_memory().total / 1e9, 2)
        info["swap_total_gb"] = round(psutil.swap_memory().total / 1e9, 2)
    except ImportError:
        pass
    info["disco_libre_gb"] = round(shutil.disk_usage(EVIDENCIA).free / 1e9, 1)
    return info


# --------------------------------------------------------------------------- optimización (§3.6)
def optimizacion(n):
    from pyspark.sql import functions as F
    import gold
    from pipeline import spark_session
    spark = spark_session("OSIPTEL_benchmark_optimizacion")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    res = {"maquina": _info_maquina(), "spark": spark.version, "repeticiones": n,
           "nota": "AQE desactivado en todas las corridas; 1 corrida de calentamiento descartada"}
    rob = spark.read.parquet(ruta("silver", "robos"))
    cob = spark.read.parquet(ruta("silver", "cobertura"))
    noop = lambda df: df.write.format("noop").mode("overwrite").save()   # ejecuta el plan sin escribir

    # A. caché + broadcast al construir las dos tablas GOLD (silver/reclamos se usa dos veces)
    def sin_optim():
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        rec = spark.read.parquet(ruta("silver", "reclamos"))
        noop(gold.gold_operadora_mes(rob, rec)); noop(gold.gold_depto_operadora(cob, rec, broadcast=False))

    def con_optim():
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
        rec = spark.read.parquet(ruta("silver", "reclamos")).cache(); rec.count()
        noop(gold.gold_operadora_mes(rob, rec)); noop(gold.gold_depto_operadora(cob, rec, broadcast=True))
        rec.unpersist()

    print("[A] GOLD sin caché ni broadcast"); res["A_antes_sin_cache_ni_broadcast"] = _medir(sin_optim, n)
    print("[A] GOLD con caché y broadcast"); res["A_despues_con_cache_y_broadcast"] = _medir(con_optim, n)
    rec = spark.read.parquet(ruta("silver", "reclamos"))
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    _guardar_plan(gold.gold_depto_operadora(cob, rec, broadcast=False), "A_antes_join_sin_broadcast")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
    _guardar_plan(gold.gold_depto_operadora(cob, rec, broadcast=True), "A_despues_join_broadcast")

    # B. poda de particiones: consulta de un año sobre reclamos particionado por anio vs sin particionar
    sin_part = ruta("_benchmark", "reclamos_sin_particion")
    spark.read.parquet(ruta("silver", "reclamos")).write.mode("overwrite").parquet(sin_part)
    consulta = lambda path: (spark.read.parquet(path).filter(F.col("anio") == 2025)
                             .groupBy("operadora").agg(F.sum("reclamos").alias("reclamos")))
    print("[B] consulta 2025 sin partición"); res["B_antes_sin_particion"] = _medir(lambda: noop(consulta(sin_part)), n)
    print("[B] consulta 2025 particionada"); res["B_despues_particionado_anio"] = _medir(lambda: noop(consulta(ruta("silver", "reclamos"))), n)
    _guardar_plan(consulta(sin_part), "B_antes_sin_particion")
    _guardar_plan(consulta(ruta("silver", "reclamos")), "B_despues_particionado")
    # igualdad de resultados (requisito: la optimización no cambia la respuesta)
    a = sorted(map(tuple, consulta(sin_part).collect())); b = sorted(map(tuple, consulta(ruta("silver", "reclamos")).collect()))
    res["B_resultados_iguales"] = a == b

    for k in ("A", "B"):
        antes, despues = [v["promedio_s"] for n_, v in res.items() if n_.startswith(k + "_") and isinstance(v, dict)][:2]
        res[f"{k}_mejora_pct"] = round(100 * (antes - despues) / antes, 1)
        res[f"{k}_veces_mas_rapido"] = round(antes / despues, 2)
    (EVIDENCIA / "optimizacion.json").write_text(json.dumps(res, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({k: v for k, v in res.items() if "mejora" in k or "veces" in k or "iguales" in k}, indent=2))


# --------------------------------------------------------------------------- un factor (proceso hijo)
def un_factor(k):
    """Corre SILVER + GOLD sobre la réplica k× y escribe su resultado. Se ejecuta en un proceso aparte."""
    import transform, gold
    from pipeline import spark_session
    spark = spark_session(f"OSIPTEL_benchmark_x{k}")
    base = {"robos": transform.leer_robos(spark), "reclamos": transform.leer_reclamos(spark),
            "cobertura": transform.leer_cobertura(spark)}
    filas = sum(df.count() for df in base.values()) * k
    rep = {n: df.crossJoin(spark.range(k).withColumnRenamed("id", "copia")) for n, df in base.items()}
    destino = f"x{k}"
    t0 = time.perf_counter()
    s_rob, _, _ = transform.silver_robos(rep["robos"], contar=False)
    s_rec, _, _ = transform.silver_reclamos(rep["reclamos"], contar=False)
    s_cob, _, _ = transform.silver_cobertura(rep["cobertura"], contar=False)
    for n, df in [("robos", s_rob), ("reclamos", s_rec), ("cobertura", s_cob)]:
        df.write.mode("overwrite").parquet(ruta("_benchmark", f"{destino}/silver_{n}"))
    t_silver = time.perf_counter() - t0
    t1 = time.perf_counter()
    rob = spark.read.parquet(ruta("_benchmark", f"{destino}/silver_robos"))
    rec = spark.read.parquet(ruta("_benchmark", f"{destino}/silver_reclamos"))
    cob = spark.read.parquet(ruta("_benchmark", f"{destino}/silver_cobertura"))
    gold.gold_operadora_mes(rob, rec).write.mode("overwrite").parquet(ruta("_benchmark", f"{destino}/gold_operadora_mes"))
    gold.gold_depto_operadora(cob, rec).write.mode("overwrite").parquet(ruta("_benchmark", f"{destino}/gold_depto"))
    t_gold = time.perf_counter() - t1
    mb = None
    if LAKE.startswith("file:"):
        p = Path(ruta("_benchmark", destino).replace("file://", ""))
        mb = round(sum(f.stat().st_size for f in p.rglob("*") if f.is_file()) / 1e6, 1)
    out = {"factor": k, "filas_entrada": filas, "tiempo_silver_s": round(t_silver, 1), "tiempo_gold_s": round(t_gold, 1),
           "tiempo_total_s": round(t_silver + t_gold, 1), "mb_en_disco": mb}
    (BENCH / f"resultado_x{k}.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
    print("[RESULTADO]", out, flush=True)


# --------------------------------------------------------------------------- escalabilidad (§3.7)
def escalabilidad(factores, hasta_fallar, timeout_min):
    try:
        import psutil
    except ImportError:
        sys.exit("Instale psutil (pip install psutil): se necesita para medir la memoria.")
    (BENCH / "logs").mkdir(parents=True, exist_ok=True)
    resumen = {"maquina": _info_maquina(), "spark_driver_memory": os.environ.get("SPARK_DRIVER_MEMORY", "por defecto (1g)"),
               "nota": "réplicas solo para medir rendimiento; cada factor en un proceso aparte", "factores": []}
    salida = EVIDENCIA / "escalabilidad.json"
    for k in factores:
        log = BENCH / "logs" / f"x{k}.log"
        muestras = []
        print(f"\n[ESCALABILIDAD] factor {k}× → log en {log}", flush=True)
        t0 = time.perf_counter()
        with open(log, "w", encoding="utf-8") as fh:
            p = subprocess.Popen([sys.executable, os.path.join(SRC, "benchmark.py"), "--un-factor", str(k)],
                                 stdout=fh, stderr=subprocess.STDOUT)
            proc = psutil.Process(p.pid)
            estado = None
            while p.poll() is None:
                vm, sw = psutil.virtual_memory(), psutil.swap_memory()
                try:   # memoria del proceso y sus hijos (incluye la JVM de Spark)
                    rss = sum(c.memory_info().rss for c in [proc] + proc.children(recursive=True))
                except psutil.Error:
                    rss = 0
                muestras.append((round(time.perf_counter() - t0, 1), round(vm.used / 1e6), round(sw.used / 1e6), round(rss / 1e6)))
                if time.perf_counter() - t0 > timeout_min * 60:
                    p.kill(); estado = f"detenido por tiempo (> {timeout_min} min)"
                    break
                time.sleep(2)
            rc = p.wait()
        dur = round(time.perf_counter() - t0, 1)
        with open(BENCH / "logs" / f"memoria_x{k}.csv", "w", encoding="utf-8") as f:
            f.write("segundo,ram_sistema_mb,swap_mb,rss_proceso_mb\n" + "\n".join(",".join(map(str, m)) for m in muestras))
        texto = log.read_text(encoding="utf-8", errors="ignore")
        if estado is None:
            if rc == 0:
                estado = "completado"
            elif rc in (137, -9) or "Killed" in texto:
                estado = "eliminado por el sistema operativo (sin memoria, código 137)"
            elif "OutOfMemoryError" in texto:
                estado = "OutOfMemoryError de la JVM"
            elif "No space left on device" in texto:
                estado = "sin espacio en disco"
            else:
                estado = f"error (código {rc}); ver log"
        fila = {"factor": k, "estado": estado, "duracion_proceso_s": dur,
                "ram_sistema_pico_mb": max((m[1] for m in muestras), default=None),
                "swap_pico_mb": max((m[2] for m in muestras), default=None),
                "rss_proceso_pico_mb": max((m[3] for m in muestras), default=None)}
        r = BENCH / f"resultado_x{k}.json"
        if rc == 0 and r.exists():
            fila.update(json.loads(r.read_text(encoding="utf-8")))
        resumen["factores"].append(fila)
        ok = [f for f in resumen["factores"] if f["estado"] == "completado"]
        if ok:
            t1 = ok[0]["tiempo_total_s"]
            for f in ok:
                f["tiempo_relativo_vs_primero"] = round(f["tiempo_total_s"] / t1, 2)
                f["volumen_relativo_vs_primero"] = round(f["factor"] / ok[0]["factor"], 2)
        salida.write_text(json.dumps(resumen, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"[ESCALABILIDAD] {k}×: {estado} · {dur} s · RAM pico {fila['ram_sistema_pico_mb']} MB · swap pico {fila['swap_pico_mb']} MB", flush=True)
        if estado != "completado" and hasta_fallar:
            print("[ESCALABILIDAD] Se encontró el límite de esta máquina; se detiene la prueba.")
            break
    return resumen


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--optimizacion", action="store_true")
    ap.add_argument("--repeticiones", type=int, default=6)
    ap.add_argument("--escalabilidad", action="store_true")
    ap.add_argument("--factores", type=int, nargs="+", default=[1, 10, 100, 150, 200, 300])
    ap.add_argument("--continuar-tras-falla", action="store_true", help="no detenerse en el primer factor que falle")
    ap.add_argument("--timeout-min", type=int, default=180, help="minutos máximos por factor")
    ap.add_argument("--un-factor", type=int, help=argparse.SUPPRESS)
    ap.add_argument("--limpiar", action="store_true", help="borra <lake>/_benchmark al terminar (solo disco local)")
    a = ap.parse_args()
    BENCH.mkdir(parents=True, exist_ok=True)
    if a.un_factor:
        un_factor(a.un_factor)
    if a.optimizacion:
        optimizacion(a.repeticiones)
    if a.escalabilidad:
        escalabilidad(a.factores, not a.continuar_tras_falla, a.timeout_min)
    if a.limpiar and LAKE.startswith("file:"):
        shutil.rmtree(Path(ruta("_benchmark", "").replace("file://", "")), ignore_errors=True)
