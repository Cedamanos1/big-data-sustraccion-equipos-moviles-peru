"""Reglas de negocio en Python puro (sin Spark).

Se usan en tres lugares para garantizar que den el mismo resultado:
  1. transform.py las replica con funciones de Spark (mismas tablas de equivalencia).
  2. tests/test_reglas.py las verifica.
  3. analisis de referencia (pandas) para cuadrar cifras del informe.
"""
import unicodedata

# Tabla de equivalencias de operadoras: prefijo de razón social normalizada -> marca comercial.
# Telefónica del Perú S.A.A. cambió su razón social a Integratel Perú S.A.A. en jun-2025:
# ambas se consolidan en MOVISTAR para no partir la serie.
OPERADORA_PREFIJOS = [
    ("AMERICA MOVIL", "CLARO"),
    ("TELEFONICA", "MOVISTAR"),
    ("INTEGRATEL", "MOVISTAR"),
    ("ENTEL", "ENTEL"),
    ("VIETTEL", "BITEL"),
]
OPERADORA_OTRAS = "OMV_OTROS"   # Flash, Guinea Mobile, Dolphin, Americatel, etc.

ESTADOS = {"SUSTRAIDOS": "SUSTRAIDO", "RECUPERADOS": "RECUPERADO", "PERDIDOS": "PERDIDO"}

# Cobertura publica Lima y Callao por separado; Reclamos los publica juntos.
# Para que el JOIN sea a la MISMA granularidad, se unifican en "LIMA Y CALLAO".
DEPTO_UNIFICAR = {"LIMA": "LIMA Y CALLAO", "CALLAO": "LIMA Y CALLAO", "LIMA Y CALLAO": "LIMA Y CALLAO"}

ACENTOS_ORIGEN = "ÁÉÍÓÚÜÑáéíóúüñ"
ACENTOS_DESTINO = "AEIOUUNaeiouun"


def normalizar_texto(valor):
    """Mayúsculas, sin tildes y con espacios simples. None si viene vacío."""
    if valor is None:
        return None
    s = str(valor)
    if s.strip() == "" or s.lower() == "nan":
        return None
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return " ".join(s.upper().split())


def operadora(razon_social):
    s = normalizar_texto(razon_social) or ""
    for prefijo, marca in OPERADORA_PREFIJOS:
        if s.startswith(prefijo):
            return marca
    return OPERADORA_OTRAS


def departamento(valor):
    s = normalizar_texto(valor)
    if s in (None, "-"):
        return None
    return DEPTO_UNIFICAR.get(s, s)


def estado(valor):
    return ESTADOS.get(normalizar_texto(valor))


def materia(valor):
    """Toma la primera materia cuando vienen varias separadas por '|'."""
    s = normalizar_texto(valor)
    if s in (None, "-"):
        return None
    return normalizar_texto(str(valor).split("|")[0])
