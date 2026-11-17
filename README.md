# Big Data de la sustracción de equipos móviles en el Perú (OSIPTEL) — v2
![Pruebas](https://github.com/Cedamanos1/big-data-sustraccion-equipos-moviles-peru/actions/workflows/pruebas.yml/badge.svg)

Proyecto integrador · Big Data y Analítica de Datos · UPAO 2026-20 · Grupo 11
Ruta de despliegue: **On-premise** (Apache Spark 3.5.1 en VM local, JupyterLab; HDFS opcional).

**Problema:** en 2025 se reportaron 1 462 490 equipos sustraídos (≈ 122 000 por mes) y solo
7.3 de cada 100 figuran como recuperados, con diferencias grandes entre operadoras. El pipeline
integra tres fuentes abiertas de OSIPTEL, mide la calidad de los datos, pronostica las sustracciones
del mes siguiente por operadora y alimenta un **tablero interactivo** (`docs/index.html`) que indica
a qué operadora priorizar en la supervisión del RENTESEG.

## 1. Requisitos

| Componente | Versión probada |
|---|---|
| Java | OpenJDK 17 (cualquier versión 11, 17 o 21 funciona) |
| Apache Spark / PySpark | 3.5.1 |
| Python | 3.11 o superior |
| Librerías | ver `requirements.txt` (pandas, openpyxl, scikit-learn, matplotlib, psutil) |

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

## 2. Datos (no se versionan; se descargan de la fuente oficial)

| Carpeta | Archivo | Fuente |
|---|---|---|
| `data/raw/robos/` | `9.1. EQUIPOS SUSTRAIDOS PERDIDOS O RECUPERADOS.xlsx` | datosabiertos.gob.pe → «Número de equipos móviles reportados como sustraídos, perdidos y recuperados [OSIPTEL]» |
| `data/raw/reclamos/` | `10.1 RECLAMOS PRESENTADOS.xlsx` | datosabiertos.gob.pe → «Reclamos presentados» (OSIPTEL) |
| `data/raw/cobertura/` | `Cobertura móvil por empresa operadora.csv` | datosabiertos.gob.pe → «Cobertura de servicio móvil por empresa operadora» |

Huellas SHA-256 de los archivos usados en el informe (verificar con `sha256sum`):

```
db9f686cb7fd6e656ab6c74345fe6d70450890a8d7118144034116f8d924808b  9.1. EQUIPOS SUSTRAIDOS PERDIDOS O RECUPERADOS.xlsx
4b27d6ff5cc6758cc08a5461b9b5e5f0a8fe5085797d69e0a111bc602b5689f3  10.1 RECLAMOS PRESENTADOS.xlsx
0e2c5336ca94ad9ab2e94dd1ae2d7a7a487ffda47c54e93267db9f0df575f2f0  Cobertura móvil por empresa operadora.csv
```

`data/muestras/` contiene las primeras 300 filas de cada fuente para revisar el formato sin descargar nada.

## 3. Ejecución (desde la raíz del repositorio)

```bash
python -m unittest discover -s tests -v              # 1. pruebas de reglas (sin Spark)
python src/pipeline.py                                # 2. INCREMENTAL: RAW→STAGING→SILVER→GOLD→tablas analíticas + modelo + tablero (solo meses nuevos)
python src/analitica.py --consultas                   #    tablas analíticas (Spark SQL) y consultas de ejemplo
python src/modelo_spark.py                            #    modelo con Spark MLlib y comparación con scikit-learn (~10 min)
python src/figuras.py                                 # 3. figuras del EDA (PNG para el informe)
python src/benchmark.py --optimizacion                # 4. medición antes/después (§3.6)
export SPARK_DRIVER_MEMORY=3g                         #    (≈ 70 % de la RAM de la VM)
python src/benchmark.py --escalabilidad               # 5. 1×, 10×, 100×, 150×, 200×, 300× hasta que falle (§3.7)
python src/benchmark_pandas.py                        # 6. contraste: el mismo pipeline en pandas hasta su límite de memoria
python src/graficar_benchmark.py                      # 7. figuras de rendimiento para el informe
```

### 3.1 Carga incremental (cómo entran datos nuevos)

El pipeline es **incremental por defecto**: si el lake ya tiene bitácora de cargas (`<lake>/_control/cargas`), solo procesa
los meses que aún no están en SILVER, los **anexa** sin tocar lo anterior y recalcula en GOLD únicamente los meses afectados
(una partición por mes). Con la primera corrida, o si el lake viene de una versión anterior, hace la carga inicial completa.

```bash
python src/pipeline.py              # incremental (o carga inicial si el lake está vacío)
python src/pipeline.py --estado     # bitácora de cargas y contenido de cada capa
python src/pipeline.py --completa   # reconstruye todo desde RAW (sobrescribe SILVER y GOLD)
```

Cómo decide qué procesar: (1) si el archivo fuente tiene la misma huella SHA-256 que en la última carga, esa fuente no se toca;
(2) si cambió, se descartan las filas de meses que ya están en SILVER **antes** de aplicar las reglas; (3) SILVER y QUARANTINE se
anexan; (4) GOLD se recalcula solo para los meses donde SILVER y GOLD difieren (nuevos, o que cambiaron de categoría, por ejemplo
un mes que pasa de «solo_robos» a «ambas» cuando llegan sus reclamos); (5) se registra la carga. Si una corrida se interrumpe,
la siguiente se repara sola. Limitación: no detecta correcciones de OSIPTEL sobre meses ya cargados (para eso, `--completa`).

**Demostración con datos reales** (carpeta aislada `demo/`; no toca los datos ni la evidencia reales), con el guion completo y las preguntas probables en [`docs/guion_demostracion.md`](docs/guion_demostracion.md):

```bash
export OSIPTEL_HOME="$PWD/demo"
python src/demo_incremental.py preparar      # F1 hasta ago-2025 y F2 hasta jul-2025
python src/pipeline.py                       # carga inicial
python src/demo_incremental.py llegan-datos  # llegan los archivos completos (F1 hasta ene-2026, F2 hasta oct-2025)
python src/pipeline.py                       # INCREMENTAL: solo procesa los meses nuevos y actualiza pronóstico y tablero
python src/pipeline.py                       # sin cambios: no procesa nada
python src/pipeline.py --estado
unset OSIPTEL_HOME                           # volver a trabajar con los datos reales
```

O bien abrir `notebooks/Pipeline_OSIPTEL_v2.ipynb` en JupyterLab y ejecutar las celdas en orden.
Para usar HDFS: `export OSIPTEL_LAKE_URI=hdfs:///user/grupo11/osiptel` antes de ejecutar.

## 4. Resultados principales (corrida del 06/11/2026, VM Debian 12 · 4 vCPU · 6.7 GB RAM)

| Resultado | Valor | Evidencia |
|---|---|---|
| Registros integrados | 276 008 (0 rechazados; totales conservados tras el JOIN) | `docs/evidencia/calidad.json` |
| Pronóstico a 1 mes (Gradient Boosting) | MAPE 6.10 % · MAE 1 708 (−12.7 % vs. ingenuo) | `docs/evidencia/modelo_metricas.json` |
| Escalabilidad con Spark | 300× (82.8 M filas) en 858 s con ≤ 5.1 GB de RAM | `docs/evidencia/escalabilidad.json` |
| Punto de quiebre en memoria (pandas) | completa 100× (27.6 M); MemoryError a 150× (41.4 M) | `docs/evidencia/escalabilidad_pandas.json` |
| Prioridad de supervisión | Movistar (5.0) y Entel (6.4) bajo la mediana de recuperación (7.7) | `docs/index.html` |

## 5. Tablas analíticas y tablero (capa de consumo)

**Tablas analíticas (Spark SQL).** Sobre GOLD, `src/analitica.py` crea cuatro tablas `an_*` consultables con SQL
(se guardan en `<lake>/gold/` y como CSV en `docs/evidencia/analitica/`):

| Tabla | Contenido |
|---|---|
| `an_kpi_operadora` | últimos 12 meses por operadora: tasa de recuperación, brecha vs. mediana, prioridad y potencial |
| `an_serie_mensual` | serie mensual con media móvil de 3 meses y variación interanual (funciones de ventana) |
| `an_resumen_anual` | totales por año y operadora (GOLD unido a `dim_tiempo`) |
| `an_depto_calidad` | reclamos por departamento, % de calidad y % de centros poblados con 4G (mar-2023) |

`python src/analitica.py --consultas` además responde cuatro preguntas de ejemplo con SQL (útil en la demostración).

`docs/index.html` es el tablero interactivo para la Dirección de Fiscalización e Instrucción (DFI) de OSIPTEL:
prioridades de supervisión por operadora, decisión sugerida, pronóstico del mes siguiente, verificación
del modelo y reclamos por departamento. Se regenera en cada corrida (`src/tablero.py`) y no usa librerías externas.
Sus operadoras y departamentos salen de las tablas analíticas; el cálculo equivalente en pandas se conserva solo para
**reconciliar** (resultado en `calidad.json` → `reconciliacion_tablero_sql`).

- Ver en local: abrir `docs/index.html` con doble clic.
- Publicarlo: GitHub → Settings → Pages → *Deploy from a branch* → `main` / `/docs`. Queda en
  `https://cedamanos1.github.io/big-data-sustraccion-equipos-moviles-peru/`.

## 6. Organización del data lake (contrato entre capas)

```
data/raw/<fuente>/                      originales intactos (solo lectura)
data/staging/<fuente>/*.csv             XLSX convertidos a CSV UTF-8
<lake>/silver/robos        (part. operadora)   fecha_mes, operadora, estado, cantidad, razones_sociales
<lake>/silver/reclamos     (part. anio)        fecha_mes, operadora, razon_social, departamento, servicio, materia, forma, reclamos, es_movil
<lake>/silver/cobertura    (part. operadora)   periodo_fecha, operadora, departamento, ubigeo_ccpp, tiene_2g..5g, estaciones_4g
<lake>/quarantine/<fuente>                     filas rechazadas + motivo_rechazo
<lake>/gold/gold_operadora_mes (part. fecha_mes) hechos: sustraidos, recuperados, perdidos, reclamos_moviles, tasa_recuperacion_pct, origen_join
<lake>/gold/gold_depto_operadora_202303        hechos: ccpp, ccpp_4g, pct_ccpp_4g, reclamos_moviles, reclamos_calidad, origen_join
<lake>/gold/dim_tiempo | dim_operadora | dim_departamento
<lake>/gold/an_kpi_operadora | an_serie_mensual | an_resumen_anual | an_depto_calidad   tablas analíticas (Spark SQL)
<lake>/_control/cargas                          bitácora de cargas (una fila por fuente y corrida: modo, filas nuevas, meses)
<lake>/_benchmark/                              réplicas SOLO para medir rendimiento (borrar con --limpiar)
docs/evidencia/                                 manifiesto, calidad, corrida, modelo, optimización, escalabilidad
```

## 7. Estructura del código

| Archivo | Responsabilidad |
|---|---|
| `src/config.py` | rutas y parámetros; único lugar que cambia al migrar de disco local a HDFS/S3 |
| `src/reglas.py` | reglas de normalización en Python puro (operadoras, departamentos, estados) |
| `src/ingesta.py` | RAW → STAGING, manifiesto SHA-256 |
| `src/incremental.py` | carga incremental: meses nuevos, bitácora de cargas, SILVER por anexión, GOLD por meses afectados |
| `src/demo_incremental.py` | prepara la demostración con datos reales (llegada de meses nuevos) en `demo/` |
| `src/transform.py` | STAGING → SILVER/QUARANTINE con PySpark (reglas de calidad) |
| `src/gold.py` | JOIN entre fuentes y modelo dimensional |
| `src/modelo.py` | modelo base (ingenuo) y definitivo (Gradient Boosting), evaluación temporal |
| `src/modelo_spark.py` | mismo problema con Spark MLlib (GBT y Random Forest) y comparación con scikit-learn |
| `src/analitica.py` | tablas analíticas `an_*` con Spark SQL, consultas de ejemplo y reconciliación con el tablero |
| `src/figuras.py` | figuras del EDA para el informe |
| `src/tablero.py` + `src/tablero_plantilla.html` | tablero interactivo `docs/index.html` |
| `src/benchmark.py` | optimización (caché, broadcast, particionado; 6 repeticiones) y escalabilidad 1×→300× hasta la falla |
| `src/benchmark_pandas.py` | mismo pipeline en pandas (en memoria) para medir el punto de quiebre de una sola máquina |
| `src/graficar_benchmark.py` | figuras de rendimiento (§3.6 y §3.7) |
| `legacy/` | versión 1 (informe parcial), conservada por trazabilidad; incluye la corrida de los históricos 2012–2015 |

## 8. Seguridad y ética

- Las tres fuentes v2 son **agregadas**: no contienen datos personales ni IMEI.
- Los históricos 2012–2015 (v1, `legacy/`) sí traen IMEI: se seudonimizan con HMAC-SHA256 y la clave
  (`.secrets/`) está excluida de Git. No se publican en el repositorio.
- Las réplicas de escalabilidad nunca alimentan el EDA, el modelo ni el tablero.

## 9. Equipo

| Integrante | Rol 1.ª mitad | Rol 2.ª mitad |
|---|---|---|
| Flores Álvarez, Rodrigo Nicolás | Coordinación | Calidad y documentación |
| Cedamanos Guevara, Julio Eduardo | Ingeniería de datos | Análisis y modelado |
| Limay Capristán, Jesús Ahlan | Análisis y modelado | Ingeniería de datos |
| Olazábal Ávila, Fernando | Calidad y documentación | Coordinación |
