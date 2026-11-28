"""Puente con Amazon S3 para el despliegue en la nube (usa la AWS CLI de la EC2; las credenciales vienen del rol de IAM).

Cómo encaja en el pipeline (ver docs/diagramas/despliegue_nube_borrador.png):
  Fuentes (descarga manual del portal) --subir-raw--> S3 bronze/raw/        originales, nunca se modifican
  EC2: bajar_raw -> ingesta.py (SHA-256, XLSX -> CSV) -> subir_staging -> S3 bronze/staging/
  Spark lee y escribe SILVER/QUARANTINE/_control y GOLD directamente en S3 por s3a:// (ver config.ruta)
  EC2: subir_resultados -> S3 gold/evidencia/ (incluye el modelo exportado) y gold/consumo/index.html (tablero)

Uso:
    python src/nube.py subir-raw [carpeta]   # sube los originales a bronze/raw (por defecto data/raw)
    python src/nube.py bajar-raw             # trae los originales a la EC2
    python src/nube.py estado                # lista lo que hay en cada bucket
    python src/nube.py url-tablero           # URL prefirmada (vence en 1 h) para ver el tablero sin hacer público el bucket
"""
import subprocess, sys
from pathlib import Path

from config import RAW, STAGING, EVIDENCIA, TABLERO, S3_BRONZE, S3_GOLD, REGION, ES_NUBE


def comando_sync(origen: str, destino: str, solo_nuevos=True):
    """Arma el comando `aws s3 sync` (función pura, para poder probarla sin AWS)."""
    cmd = ["aws", "s3", "sync", str(origen), str(destino), "--region", REGION, "--only-show-errors"]
    return cmd


def _ejecutar(cmd, simular=False):
    if simular:
        return cmd
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"Falló {' '.join(cmd[:4])}: {r.stderr.strip() or r.stdout.strip()}")
    return r.stdout


def subir_raw(carpeta=None, simular=False):
    """Local -> s3://bronze/raw/. Los originales se suben tal cual; sync no borra nada del bucket."""
    return _ejecutar(comando_sync(carpeta or RAW, f"s3://{S3_BRONZE}/raw/"), simular)


def bajar_raw(simular=False):
    """s3://bronze/raw/ -> data/raw de la EC2 (copia de trabajo; la copia inmutable queda en S3)."""
    return _ejecutar(comando_sync(f"s3://{S3_BRONZE}/raw/", RAW), simular)


def subir_staging(simular=False):
    return _ejecutar(comando_sync(STAGING, f"s3://{S3_BRONZE}/staging/"), simular)


def bajar_evidencia(simular=False):
    """Recupera evidencia y modelo exportado de S3 (p. ej. en una EC2 nueva), para aplicar el modelo sin reentrenar."""
    return _ejecutar(comando_sync(f"s3://{S3_GOLD}/evidencia/", EVIDENCIA), simular)


def subir_resultados(simular=False):
    """Evidencia (con modelo_gb_h1.joblib) y tablero -> bucket gold. El tablero queda en consumo/index.html."""
    out = [_ejecutar(comando_sync(EVIDENCIA, f"s3://{S3_GOLD}/evidencia/"), simular)]
    if Path(TABLERO).exists() or simular:
        cmd = ["aws", "s3", "cp", str(TABLERO), f"s3://{S3_GOLD}/consumo/index.html", "--region", REGION,
               "--content-type", "text/html; charset=utf-8", "--only-show-errors"]
        out.append(_ejecutar(cmd, simular))
    return out


def url_tablero(segundos=3600):
    return _ejecutar(["aws", "s3", "presign", f"s3://{S3_GOLD}/consumo/index.html",
                      "--expires-in", str(segundos), "--region", REGION]).strip()


def estado():
    for b in (S3_BRONZE, S3_GOLD):
        print(f"--- s3://{b}/ ---")
        print(_ejecutar(["aws", "s3", "ls", f"s3://{b}/", "--recursive", "--human-readable", "--summarize",
                         "--region", REGION]))


if __name__ == "__main__":
    if not ES_NUBE:
        sys.exit("Definir OSIPTEL_S3_BRONZE, OSIPTEL_S3_SILVER y OSIPTEL_S3_GOLD (ver nube/README_NUBE.md).")
    accion = sys.argv[1] if len(sys.argv) > 1 else ""
    if accion == "subir-raw":
        subir_raw(sys.argv[2] if len(sys.argv) > 2 else None); print("[NUBE] originales subidos a bronze/raw")
    elif accion == "bajar-raw":
        bajar_raw(); print("[NUBE] originales descargados a data/raw")
    elif accion == "estado":
        estado()
    elif accion == "url-tablero":
        print(url_tablero())
    else:
        sys.exit(__doc__)
