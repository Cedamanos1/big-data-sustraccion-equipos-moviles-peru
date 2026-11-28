# Modelo de pronóstico

Resumen de `src/modelo.py` (informe §4.2 y §4.3).

**Problema:** pronosticar los equipos móviles sustraídos por operadora (Bitel, Claro, Entel, Movistar) para el mes siguiente (H = 1, principal) y a tres meses (H = 3, secundario).

**Formulación:** en lugar de predecir el nivel, se predice la **variación relativa** respecto del último mes conocido, `y = sustraídos_t / sustraídos_(t−H) − 1`, y luego se reconstruye el nivel. Los árboles de decisión no extrapolan niveles; predecir la variación evita ese problema.

**Variables:** variaciones de los meses previos (`var_lag`), la misma variación un año antes (`var_estacional`), el nivel respecto de la media de 12 meses, el mes del año y una marca del periodo COVID.

**Validación temporal (sin mezclar el futuro con el pasado)**

- Entrenamiento hasta junio de 2024, excluyendo el choque COVID (mar-2020 a mar-2021); prueba de julio de 2024 a enero de 2026.
- Los hiperparámetros se eligen con validación cruzada temporal (`TimeSeriesSplit`, 4 particiones).
- Se comparan dos modelos base (ingenuo e ingenuo estacional) contra Ridge, Random Forest y Gradient Boosting.

**Resultado:** el Gradient Boosting es el elegido. A un mes tiene un MAPE de 6.10 % (el ingenuo, 7.03 %, es decir −12.7 % de MAE); a tres meses, 7.67 %.

**Modelo exportado:** el Gradient Boosting entrenado con toda la historia (sin COVID) se guarda en `docs/evidencia/modelo_gb_h1.joblib`. Cuando llegan datos nuevos, el pipeline **no reentrena**: carga ese modelo y lo aplica. Para reentrenar y exportar de nuevo: `python src/pipeline.py --reentrenar`.

**Alternativa evaluada:** Spark MLlib (`src/modelo_spark.py`) con las mismas variables y validación dio un rendimiento equivalente (MAPE 6.25 % a un mes), pero tarda unos 10 minutos frente a unos 40 segundos; con unas 600 filas no hay ventaja que justifique reemplazar el modelo.
