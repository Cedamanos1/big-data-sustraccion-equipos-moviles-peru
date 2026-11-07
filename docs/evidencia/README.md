# Evidencias de ejecución

Cada archivo de esta carpeta lo genera el código del repositorio. Ninguno se edita a mano.
Corrida de referencia: 06/11/2026, VM Debian 12 · 4 vCPU · 6.7 GB de RAM · Spark 3.5.1.

| Archivo | Qué demuestra | Lo genera | Sección del informe |
|---|---|---|---|
| `manifiesto.json` | Huella SHA-256, filas y fecha de cada archivo fuente: los datos usados son los oficiales y no se alteraron | `src/ingesta.py` | §2 Datos |
| `calidad.json` | Filas leídas, aceptadas y rechazadas por fuente; resultado de los JOIN (`ambas` / `solo_…`); conservación de totales SILVER = GOLD (15 836 038) | `src/pipeline.py` | §3 Calidad |
| `corrida.json` | Máquina, versión de Spark y tiempo de cada etapa del pipeline | `src/pipeline.py` | §3 |
| `modelo_metricas.json` | MAE, MAPE, MASE y sesgo de los modelos (ingenuo, ingenuo estacional, Ridge, Random Forest, Gradient Boosting) en H1 y H3, con el periodo de entrenamiento y de prueba | `src/modelo.py` | §4 Modelo |
| `predicciones_prueba_H1.csv`, `predicciones_prueba_H3.csv` | Predicción de cada modelo frente al valor real, mes a mes, en el periodo de prueba | `src/modelo.py` | §4 |
| `pronostico_mes_siguiente.csv` | Pronóstico de sustracciones del mes siguiente por operadora | `src/modelo.py` | §4 y tablero |
| `kpis_tablero.json` | Indicadores que muestra el tablero (sustraídos/mes, variación interanual, recuperación por operadora) | `src/tablero.py` | §4.4 Tablero |
| `tabla_departamentos_202303.csv` | Cobertura 4G y reclamos de calidad por departamento (J2) | `src/gold.py` | §4 |
| `optimizacion.json` | Antes y después de cada optimización (caché + broadcast: −26.5 %; particionado por año: −13.4 %), 6 repeticiones con AQE desactivado | `src/benchmark.py --optimizacion` | §3.6 |
| `benchmark/plan_*.txt` | Plan físico de Spark antes y después (muestra `BroadcastHashJoin` y `PartitionFilters`) | `src/benchmark.py --optimizacion` | §3.6 |
| `escalabilidad.json`, `benchmark/resultado_x*.json` | Tiempo y memoria de Spark de 1× a 300× (82.8 M filas) | `src/benchmark.py --escalabilidad` | §3.7 |
| `escalabilidad_pandas.json`, `benchmark/pandas_resultado_x*.json` | El mismo pipeline en pandas: completa 100× y falla con MemoryError a 150× (punto de quiebre) | `src/benchmark_pandas.py` | §3.7 |
| `benchmark/logs/memoria_x*.csv` | Muestreo de RAM y swap cada segundo durante cada factor | `src/benchmark.py` | §3.7 |
| `logs/*.log`, `benchmark/logs/*.log` | Salida completa de consola de cada corrida | redirección de la terminal | Anexos |
| `estimacion_aws.pdf` | Estimación en AWS Pricing Calculator: S3 + Glue + Athena = USD 0.22/mes | calculator.aws | §2 Costos |

## Cómo reproducirlas

```bash
python src/pipeline.py                       # manifiesto, calidad, corrida, modelo, pronóstico, tablero
python src/benchmark.py --optimizacion       # optimizacion.json y planes
export SPARK_DRIVER_MEMORY=3g
python src/benchmark.py --escalabilidad      # escalabilidad.json
python src/benchmark_pandas.py               # escalabilidad_pandas.json
```

Las réplicas de escalabilidad (`<lake>/_benchmark/`) solo se usan para medir rendimiento. Nunca alimentan el
modelo ni el tablero.
