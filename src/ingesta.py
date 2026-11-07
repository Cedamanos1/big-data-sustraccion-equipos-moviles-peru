"""Ingesta RAW -> STAGING.

- Calcula SHA-256, tamaño y filas de cada archivo original (manifiesto de procedencia).
- Convierte los XLSX (que Spark no lee de forma nativa) a CSV UTF-8 en data/staging.
  La conversión se hace en el driver con pandas porque los XLSX son pequeños
  (70 KB y 7 MB); Spark lee luego el CSV con un esquema explícito.
- El CSV de cobertura NO se convierte: Spark lo lee directo desde RAW (ISO-8859-1, ';').
- Nunca modifica data/raw.
"""
import hashlib, json, time
from datetime import datetime
from pathlib import Path
import pandas as pd

from config import FUENTES, STAGING, EVIDENCIA

# Columnas destino de staging (contrato con transform.py)
COLS_ROBOS = ["empresa", "mes", "anio", "estado", "cantidad"]
COLS_RECLAMOS = ["empresa", "mes", "departamento", "servicio", "forma", "materia", "reclamos"]


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for bloque in iter(lambda: f.read(1 << 20), b""):
            h.update(bloque)
    return h.hexdigest()


def _unico(carpeta: Path, patron: str) -> Path:
    archivos = sorted(carpeta.glob(patron))
    if len(archivos) != 1:
        raise FileNotFoundError(f"Se esperaba 1 archivo {patron} en {carpeta}, hay {len(archivos)}. Ver README §2.")
    return archivos[0]


def _xlsx_a_csv(path: Path, fila_encabezado: int, columnas, destino: Path) -> int:
    """Lee la hoja 'Dataset' saltando el título, descarta la columna A vacía y filas totalmente vacías."""
    df = pd.read_excel(path, sheet_name="Dataset", header=fila_encabezado, dtype=str).iloc[:, 1:]
    if df.shape[1] != len(columnas):
        raise ValueError(f"{path.name}: se esperaban {len(columnas)} columnas y hay {df.shape[1]}: {list(df.columns)}")
    df.columns = columnas
    df = df.dropna(how="all")
    destino.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(destino, index=False, encoding="utf-8")
    return len(df)


def ingestar():
    t0 = time.perf_counter()
    manifiesto = {"fecha_ejecucion": datetime.now().isoformat(timespec="seconds"), "fuentes": {}}

    f1 = _unico(FUENTES["robos"], "*.xlsx")
    n1 = _xlsx_a_csv(f1, 3, COLS_ROBOS, STAGING / "robos" / "robos.csv")
    f2 = _unico(FUENTES["reclamos"], "*.xlsx")
    n2 = _xlsx_a_csv(f2, 4, COLS_RECLAMOS, STAGING / "reclamos" / "reclamos.csv")
    f3 = _unico(FUENTES["cobertura"], "*.csv")
    with open(f3, "rb") as f:
        n3 = sum(1 for _ in f) - 1  # menos el encabezado

    for clave, path, filas, fmt in [("robos", f1, n1, "XLSX"), ("reclamos", f2, n2, "XLSX"), ("cobertura", f3, n3, "CSV ;")]:
        manifiesto["fuentes"][clave] = {"archivo": path.name, "formato": fmt, "bytes": path.stat().st_size,
                                        "sha256": sha256(path), "filas": int(filas)}
    manifiesto["filas_totales"] = n1 + n2 + n3
    manifiesto["tiempo_ingesta_s"] = round(time.perf_counter() - t0, 2)
    EVIDENCIA.mkdir(parents=True, exist_ok=True)
    (EVIDENCIA / "manifiesto.json").write_text(json.dumps(manifiesto, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[INGESTA] {manifiesto['filas_totales']:,} filas en {manifiesto['tiempo_ingesta_s']} s")
    return manifiesto


if __name__ == "__main__":
    ingestar()
