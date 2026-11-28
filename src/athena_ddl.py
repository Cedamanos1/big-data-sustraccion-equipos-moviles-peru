"""Registra las tablas de GOLD en Amazon Athena / AWS Glue Data Catalog.

Lee el esquema REAL de cada tabla Parquet de GOLD con Spark y genera los CREATE EXTERNAL TABLE (no se escriben a mano,
así no pueden desviarse del contrato). Después, con --ejecutar, los envía a Athena con la AWS CLI.

    python src/athena_ddl.py                # solo escribe nube/athena_gold.sql
    python src/athena_ddl.py --ejecutar     # además crea la base osiptel_gold y las tablas en Athena
"""
import argparse, os, subprocess, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import ruta, S3_GOLD, REGION, ROOT

BASE = "osiptel_gold"
TABLAS = {  # tabla -> columnas de partición (como las escribe el pipeline)
    "gold_operadora_mes": ["fecha_mes"], "gold_depto_operadora_202303": [],
    "dim_tiempo": [], "dim_operadora": [], "dim_departamento": [],
    "an_kpi_operadora": [], "an_serie_mensual": [], "an_resumen_anual": [], "an_depto_calidad": [],
}


def ddl_tabla(tabla, dtypes, particion, ubicacion, base=BASE):
    """CREATE EXTERNAL TABLE de Athena a partir de [(columna, tipo_spark)]. Los tipos simpleString de Spark son válidos en Hive."""
    cols = [f"  `{n}` {t}" for n, t in dtypes if n not in particion]
    tipos = dict(dtypes)
    sql = f"CREATE EXTERNAL TABLE IF NOT EXISTS {base}.{tabla} (\n" + ",\n".join(cols) + "\n)"
    if particion:
        sql += "\nPARTITIONED BY (" + ", ".join(f"`{p}` {tipos[p]}" for p in particion) + ")"
    sql += f"\nSTORED AS PARQUET\nLOCATION '{ubicacion.replace('s3a://', 's3://')}/'"
    sql += "\nTBLPROPERTIES ('parquet.compression'='SNAPPY')"
    return sql + ";"


def _athena(sql, workgroup="primary"):
    salida = f"s3://{S3_GOLD}/_athena/"
    qid = subprocess.run(["aws", "athena", "start-query-execution", "--query-string", sql, "--region", REGION,
                          "--result-configuration", f"OutputLocation={salida}", "--work-group", workgroup,
                          "--query", "QueryExecutionId", "--output", "text"],
                         capture_output=True, text=True, check=True).stdout.strip()
    while True:
        est = subprocess.run(["aws", "athena", "get-query-execution", "--query-execution-id", qid, "--region", REGION,
                              "--query", "QueryExecution.Status.State", "--output", "text"],
                             capture_output=True, text=True, check=True).stdout.strip()
        if est in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1.5)
    if est != "SUCCEEDED":
        raise RuntimeError(f"Athena {est}: {sql[:80]}…")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--ejecutar", action="store_true"); a = ap.parse_args()
    from pipeline import spark_session
    spark = spark_session("OSIPTEL_athena_ddl")
    sentencias = [f"CREATE DATABASE IF NOT EXISTS {BASE};"]
    for tabla, part in TABLAS.items():
        try:
            df = spark.read.parquet(ruta("gold", tabla))
        except Exception:
            print(f"[DDL] {tabla}: no existe todavía en GOLD, se omite"); continue
        sentencias.append(ddl_tabla(tabla, df.dtypes, part, ruta("gold", tabla)))
        if part:
            sentencias.append(f"MSCK REPAIR TABLE {BASE}.{tabla};")
    destino = ROOT / "nube" / "athena_gold.sql"
    destino.parent.mkdir(exist_ok=True)
    destino.write_text("\n\n".join(sentencias) + "\n", encoding="utf-8")
    print(f"[DDL] {len(sentencias)} sentencias en {destino}")
    if a.ejecutar:
        for s in sentencias:
            _athena(s)
        print(f"[ATHENA] base {BASE} lista. Prueba: SELECT * FROM {BASE}.gold_operadora_mes LIMIT 10;")
