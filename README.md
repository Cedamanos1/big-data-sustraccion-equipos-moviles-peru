# Pipeline Big Data de OSIPTEL — PySpark + JupyterLab

**Implementación entregada:** código reproducible para `RAW → STAGING → SILVER → GOLD`, tres fuentes oficiales, tratamiento del IMEI, validaciones, rechazos, integración controlada, exportación Parquet y pruebas optativas de rendimiento. El código **no se ha ejecutado sobre los archivos oficiales completos** en esta entrega; se necesita descargarlos y correrlo en el entorno del equipo.

## 1. Requisitos

- JupyterLab y Apache Spark/PySpark ya configurados con kernel Python compatible (como solicitó el equipo).
- Biblioteca `openpyxl` para interpretar el XLSX del dataset agregado: en una celda de JupyterLab ejecutar `%pip install openpyxl` solo si hace falta.
- Java compatible con la versión instalada de Spark y capacidad en disco para archivos históricos y datos Parquet.
- No hace falta HDFS: el código usa almacenamiento local por defecto. Para HDFS, configura `OSIPTEL_LAKE_URI=hdfs:///ruta/osiptel` **antes** de importar el módulo. La conversión de archivos XLSX/ZIP inicial ocurre en disco accesible desde el driver de Jupyter.

## 2. Descarga de datos (MANUAL, oficiales)

### Fuente A — Agregados mensuales

https://www.datosabiertos.gob.pe/dataset/equipos-sustraidos-perdidos-y-recuperados

Descarga el XLSX y colócalo en `data/raw/01_agregados/`. Columnas esperadas, con variantes aceptadas: `Empresa`, `Año`, `Mes`, `Estado del equipo móvil`, `Cantidad de equipos móvil`.

### Fuente B — Históricos por transacción

https://www.datosabiertos.gob.pe/dataset/equipos-celulares-robados-perdidos-o-recuperados-osiptel

Descarga los ZIP `BD 2012`, `BD 2013`, `BD 2014`, `BD 2015`, `BD 2016` y colócalos **sin descomprimir** en `data/raw/02_historicos/`. Dentro de los ZIP se esperan archivos CSV/TXT/DAT con encabezados como `ID, NE, ARCHIVO, FECHAHORA, REPORTE, PAIS, OPERADOR, TECNOLOGIA`. Si el portal cambia estructura o archivos, adapta el lector y documenta el ajuste, no inventes campos.

### Fuente C — Cobertura por centro poblado

https://www.datosabiertos.gob.pe/dataset/cobertura-de-servicio-m%C3%B3vil-por-empresa-operadora

Descarga el CSV y colócalo en `data/raw/03_cobertura/`. El CSV oficial utiliza `;` como separador, con campos como `PERIODO`, `EMPRESA_OPERADORA`, `UBIGEO_CCPP`, `DEPARTAMENTO`, `2G`, `3G`, `4G`, `5G`.

**IMPORTANTE:** son datos de diferentes períodos. La cobertura NO indica dónde se produjo un robo. No sumar reportes agregados con eventos históricos sin conciliación ni denominar «tasa real de recuperación» al cociente entre recuperados y sustraídos de un período.

## 3. Ejecutar en JupyterLab (recomendado)

1. Extrae el ZIP de este proyecto y coloca allí los datasets según el paso 2.
2. Abre JupyterLab dentro de la carpeta `OSIPTEL_Pipeline_PySpark/`.
3. Abre `Pipeline_OSIPTEL_Jupyter.ipynb` y ejecuta sus celdas en orden.
4. Observa las salidas, revisa `docs/evidence/calidad.json`, y verifica las tablas GOLD.
5. Ejecuta opcionalmente medición de particionado y escalabilidad, con cautela (100× puede consumir decenas de GB).

También puedes ejecutar desde la terminal de JupyterLab:

```bash
python osiptel_pipeline.py
python osiptel_pipeline.py --optimize
python osiptel_pipeline.py --benchmark-full --factors 1 10
# Solo si tus recursos lo permiten y con aprobación del equipo:
python osiptel_pipeline.py --benchmark-full --factors 1 10 100
```

## 4. Salidas generadas

| Ruta | Contenido |
|---|---|
| `data/raw/*` | Archivos de origen intactos, descargados por el equipo |
| `data/staging/*` | CSV normalizado para Spark; los IMEI se sustituyen por HMAC-SHA256 |
| `data/lake/silver/agregados` | Agregados limpios |
| `data/lake/silver/historicos` | Eventos históricos limpios (seudonimizados) |
| `data/lake/silver/cobertura` | Cobertura limpia y validada |
| `data/lake/rejects/*` | Filas rechazadas, con motivo explícito |
| `data/lake/gold/fact_reportes_por_fuente` | Fuente A y B unidas en un esquema común, pero diferenciadas por `fuente` y `unidad` |
| `data/lake/gold/comparacion_periodos_comunes` | JOIN de registros A y B **solo** cuando coinciden mes + operadora + categoría. Puede estar vacío. |
| `data/lake/gold/cobertura_departamental` | Centros poblados con cobertura por tecnología y empresa |
| `data/lake/gold/perfil_operadora_interfuentes` | JOIN descriptivo por operadora, sin inferencias temporales ni geográficas |
| `docs/evidence/manifest.json` | Procedencia técnica, filas de staging, hash SHA-256 y bytes |
| `docs/evidence/calidad.json` | Total de registros, aceptados/rechazados, tiempos de ejecución |
| `docs/evidence/escalabilidad_pipeline.json` | Tiempos para replicaciones utilizadas **solo para rendimiento** |
| `docs/evidence/optimizacion.json` | Tiempos de consulta con/sin particionado |

`benchmark_full` escribe réplicas temporales en `data/lake/_benchmark`: **no consumir estos datos para gráficos ni modelos**. El benchmark cubre el ETL PySpark desde datos normalizados en staging, sin repetir la fase de descompresión y lectura de Excel. Para la rúbrica, documenta explícitamente esta delimitación.

## 5. Seguridad y límites metodológicos

- El IMEI completo se conserva exclusivamente en el archivo RAW oficial; en staging y SILVER se almacena `imei_hash` calculado con HMAC-SHA256. La clave se mantiene en `.secrets/imei_hmac.key` y **no se debe subir a Git**. Si se pierde, los hashes ya generados no serán reproducibles (salvo que se respalde de forma segura).
- Si dejas de trabajar con el proyecto, elimina staging y archivos de origen según las políticas de manejo de datos del curso.
- `BLOQUEADO` y categorías desconocidas quedan como `OTRO` para no contarlas automáticamente como robos. El estado `LIBERADO` tampoco equivale automáticamente a `RECUPERADO`.
- Fuente A cuenta equipos reportados; Fuente B cuenta eventos/reportes. Se mantienen diferenciados, incluso al construir la vista GOLD unificada.
- Un centro poblado cubierto no significa que exista una estación base dentro de ese centro, ni que se haya registrado allí un robo.
- La prueba de escalabilidad replica datos reales **solo para medir rendimiento**, no para análisis o predicción.
- Los indicadores `docs/evidence/*.json` se generan al ejecutar, no se adjuntan con valores inventados.

## 6. Pruebas incluidas y validación

Ejecutar `python -m unittest discover -s tests -v`. Las pruebas unitarias comprueban encabezados de las fuentes, lectura de CSV con punto y coma y hashing de IMEI. Se verificó la sintaxis del script con `py_compile`; **no hay certificación de la ejecución Spark integral ni de compatibilidad con todos los archivos fuente originales** hasta que se pruebe en tu entorno.

## 7. Posibles ajustes tras una primera corrida

- Si el archivo XLSX del portal cambia encabezados u hojas, ampliar `ALIASES['agregados']` de forma documentada.
- Si los archivos del histórico no contienen encabezado o usan otra codificación/delimitador, preparar un parser explícito para el formato publicado.
- Si el Spark del laboratorio es remoto, colocar los archivos staging en almacenamiento accesible para los ejecutores o usar HDFS, ya que el driver Jupyter prepara archivos locales.
- Revisar categoría, período, estado y nulidad de cada dataset antes de interpretar resultados o entrenar un pronóstico.

## Fuentes

- OSIPTEL. *Equipos sustraídos, perdidos y recuperados*. Plataforma Nacional de Datos Abiertos del Perú.
- MTC/OSIPTEL. *Equipos celulares robados, perdidos o recuperados*. Plataforma Nacional de Datos Abiertos del Perú.
- OSIPTEL. *Cobertura de servicio móvil por empresa operadora*. Plataforma Nacional de Datos Abiertos del Perú.
