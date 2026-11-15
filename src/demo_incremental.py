"""Prepara la demostración de la carga incremental con DATOS REALES, en una carpeta aislada (demo/).

Simula que OSIPTEL publica meses nuevos: primero deja en demo/data/raw versiones recortadas de F1 y F2
(hasta cierto mes); después 'llegan' los archivos completos. No toca data/raw, el lake real ni docs/.

    export OSIPTEL_HOME="$PWD/demo"                 # el pipeline trabajará dentro de demo/
    python src/demo_incremental.py preparar         # 1. datos hasta ago-2025 (robos) y jul-2025 (reclamos)
    python src/pipeline.py                          # 2. carga inicial
    python src/pipeline.py --estado                 #    bitácora y contenido del lake
    python src/demo_incremental.py llegan-datos     # 3. llegan los meses nuevos (archivos completos)
    python src/pipeline.py                          # 4. INCREMENTAL: solo procesa lo nuevo
    python src/pipeline.py                          # 5. sin cambios: no procesa nada
    python src/demo_incremental.py limpiar          # borra demo/
"""
import argparse, shutil, sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
REAL = ROOT / "data" / "raw"          # originales reales (solo lectura)
DEMO = ROOT / "demo"
FUENTES = {
    "robos": dict(fila=3, cols=["empresa", "mes", "anio", "estado", "cantidad"]),
    "reclamos": dict(fila=4, cols=["empresa", "mes", "departamento", "servicio", "forma", "materia", "reclamos"]),
}


def _unico(carpeta: Path, patron: str) -> Path:
    archivos = sorted(carpeta.glob(patron))
    if len(archivos) != 1:
        sys.exit(f"Se esperaba 1 archivo {patron} en {carpeta}; hay {len(archivos)}. Ver README §2.")
    return archivos[0]


def _recortar(origen: Path, destino: Path, fila: int, cols, corte: str):
    """Copia el XLSX conservando solo los meses <= corte (formato 'YYYY-MM'), con la misma estructura de hoja."""
    df = pd.read_excel(origen, sheet_name="Dataset", header=fila, dtype=str).iloc[:, 1:]
    df.columns = cols
    df = df.dropna(how="all")
    n0 = len(df)
    df = df[df["mes"].str[:7] <= corte]
    df.insert(0, "_", None)                      # columna A vacía, como en el archivo original
    destino.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(destino, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="Dataset", startrow=fila, index=False)
    print(f"  {destino.name}: {len(df):,} de {n0:,} filas (meses hasta {corte})")


def preparar(corte_robos: str, corte_reclamos: str):
    print(f"Preparando demo/ con datos reales recortados (robos ≤ {corte_robos}, reclamos ≤ {corte_reclamos})")
    for nombre, corte in [("robos", corte_robos), ("reclamos", corte_reclamos)]:
        f = FUENTES[nombre]
        origen = _unico(REAL / nombre, "*.xlsx")
        _recortar(origen, DEMO / "data" / "raw" / nombre / origen.name, f["fila"], f["cols"], corte)
    cob = _unico(REAL / "cobertura", "*.csv")
    destino = DEMO / "data" / "raw" / "cobertura"
    destino.mkdir(parents=True, exist_ok=True)
    shutil.copy2(cob, destino / cob.name)
    print(f"  {cob.name}: copiado completo (es una sola foto, marzo 2023)")
    print("\nSiguiente: export OSIPTEL_HOME=\"$PWD/demo\"  y  python src/pipeline.py   (carga inicial)")


def llegan_datos():
    if not DEMO.exists():
        sys.exit("Primero: python src/demo_incremental.py preparar")
    for nombre in FUENTES:
        origen = _unico(REAL / nombre, "*.xlsx")
        shutil.copy2(origen, DEMO / "data" / "raw" / nombre / origen.name)
        print(f"  Llegó {origen.name} completo (con los meses nuevos)")
    print("\nSiguiente: python src/pipeline.py   (carga INCREMENTAL: solo procesa lo nuevo)")


def limpiar():
    shutil.rmtree(DEMO, ignore_errors=True)
    print("demo/ eliminada")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("accion", choices=["preparar", "llegan-datos", "limpiar"])
    ap.add_argument("--corte-robos", default="2025-08", help="último mes de F1 en la primera entrega (YYYY-MM)")
    ap.add_argument("--corte-reclamos", default="2025-07", help="último mes de F2 en la primera entrega (YYYY-MM)")
    a = ap.parse_args()
    if a.accion == "preparar":
        preparar(a.corte_robos, a.corte_reclamos)
    elif a.accion == "llegan-datos":
        llegan_datos()
    else:
        limpiar()
