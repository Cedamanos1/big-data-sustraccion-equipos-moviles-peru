# Guion de la demostración (≈ 12 minutos)

Objetivo: mostrar que **los datos nuevos entran al pipeline, se reflejan en el tablero y generan valor para OSIPTEL**, sin reprocesar lo anterior. Todo se hace sobre datos reales en una carpeta aislada (`demo/`), sin tocar el lake real.

## Antes de empezar (hacerlo con tiempo, no frente al profesor)

```bash
cd ~/big-data-sustraccion-equipos-moviles-peru
source ~/BigData_UPAO/bigdata_env/bin/activate      # el prompt debe empezar con (bigdata_env)
export SPARK_DRIVER_MEMORY=3g
git pull && python -m unittest discover -s tests 2>&1 | tail -3     # Ran 59 tests ... OK
export OSIPTEL_HOME="$PWD/demo"
python src/demo_incremental.py preparar               # datos recortados: robos hasta ago-2025, reclamos hasta jul-2025
python src/pipeline.py 2>&1 | grep -v '"meses":'      # carga inicial (≈ 2 min): entrena y exporta el modelo. Dejarla ya corrida.
```

Tener abiertos: `docs/index.html` real (o la URL de GitHub Pages), la Figura 1 (arquitectura) y la Figura 2 (despliegue) del informe.

## Pasos

| # | Minutos | Qué decir | Qué mostrar |
|---|---|---|---|
| 1 | 1 | **El problema.** Se sustraen unos 110 mil equipos móviles al mes entre las cuatro operadoras y se recuperan muy pocos (5 a 10 de cada 100, según operadora). OSIPTEL necesita saber a qué operadora supervisar primero. | Tablero (`docs/index.html`): tarjetas de prioridad, Movistar y Entel «Prioritaria» |
| 2 | 2 | **La arquitectura.** Fuentes → RAW/STAGING (Bronze) → SILVER y QUARANTINE (Silver) → GOLD y tablas analíticas (Gold) → modelo y tablero. Ruta **on-premise** (VM Debian, Spark local); equivalente en AWS estimado en USD 0.22 al mes. | Figuras 1 y 2 del informe; tabla de equivalencia Bronze/Silver/Gold |
| 3 | 1 | **Estado inicial.** El lake tiene datos hasta ago-2025 (robos) y jul-2025 (reclamos). La bitácora registra la carga inicial. | `python src/pipeline.py --estado 2>&1 \| tail -25` |
| 4 | 2 | **Llegan datos nuevos.** «OSIPTEL publicó los archivos completos.» Se corre el mismo comando, sin opciones. | `python src/demo_incremental.py llegan-datos`  y luego  `python src/pipeline.py 2>&1 \| grep -v '"meses":'` |
| 5 | 1 | **Qué pasó.** El modelo **ya estaba entrenado y exportado**: no se reentrena, se aplica a los datos nuevos que recorrieron todas las capas. Modo incremental: 71 filas nuevas de robos y 16 722 de reclamos; cobertura sin cambios (misma huella SHA-256, no se procesa); solo 6 meses de GOLD recalculados; SILVER y GOLD conservan la historia. | Líneas `[INCREMENTAL]`, `gold_meses_recalculados` y `[MODELO] modelo ya entrenado… se aplica a los datos nuevos, sin reentrenar` |
| 6 | 0.5 | **Idempotencia.** Si se vuelve a correr sin datos nuevos, no procesa nada. | `python src/pipeline.py 2>&1 \| grep -E "INCREMENTAL\|ANALÍTICA\|MODELO"`  (≈ 18 s) |
| 7 | 1 | **Se refleja en el tablero.** El corte pasó a enero de 2026 y el pronóstico, a febrero de 2026. | Abrir `demo/docs/index.html` |
| 8 | 2 | **Tablas analíticas.** GOLD alimenta tablas consultables con SQL; el tablero lee esas tablas y se reconcilia con el cálculo en pandas. | `python src/analitica.py --consultas 2>&1 \| grep -v WARN` |
| 9 | 1 | **Spark MLlib.** Se evaluó con las mismas variables y validación: rendimiento equivalente al de scikit-learn (MAPE 6.25 % frente a 6.10 % a un mes). Se mantiene scikit-learn en producción; MLlib queda listo si GOLD crece. | `docs/evidencia/modelo_mllib_metricas.json` (no correrlo: tarda ≈ 10 min) |
| 10 | 1.5 | **El valor.** Si Movistar (5.03) y Entel (6.41) alcanzaran la mediana de recuperación (7.65 por cada 100 sustraídos), serían unos **13 000 equipos más recuperados al año**. El modelo anticipa el mes siguiente con 6.1 % de error (−12.7 % frente al ingenuo). | Tarjeta de impacto del tablero |

## Al terminar

```bash
python src/demo_incremental.py limpiar     # borra demo/
unset OSIPTEL_HOME
```

## Preguntas probables

- **¿Por qué on-premise y no nube?** Decisión D-01 (detalle en §2.4): el costo no es el argumento (en la nube sería USD 0.22 al mes); el problema cabe en una sola máquina (Spark local aguantó 300× el volumen), el entorno es reproducible y el código es portable (se cambia `OSIPTEL_LAKE_URI`). Se acepta que la nube es más rápida de poner en marcha y escala sin armar un clúster; se migraría si el volumen, la disponibilidad o los usuarios lo exigieran.
- **¿Por qué MLlib no es el modelo final?** Rinde igual con unas 600 filas y tarda unos 10 minutos frente a unos 40 segundos. Con millones de filas sí convendría; el código ya está probado.
- **¿Reentrenan el modelo con cada dato nuevo?** No. El modelo se entrena una vez con toda la historia y se exporta (`docs/evidencia/modelo_gb_h1.joblib`); con datos nuevos se carga y se aplica (menos de 1 s). Reentrenar es una orden explícita: `python src/pipeline.py --reentrenar` (decisión D-20); se haría si el MAPE supera 8 %.
- **¿Cómo sabe qué es «nuevo»?** Compara los meses del archivo contra los que ya están en SILVER y usa una huella SHA-256 para descartar archivos idénticos. Detecta incluso un mes que llega tarde.
- **¿Qué no detecta?** Correcciones a meses ya cargados: en ese caso se usa `--completa`.
- **¿Cómo sé que lo incremental es correcto?** Se comparó con la reconstrucción total: los dos lakes quedaron idénticos (SILVER y GOLD con las mismas filas).
- **¿Por qué SILVER de robos tiene 1 569 filas si se aceptaron 1 698?** Se consolida por mes, operadora y estado al sumar razones sociales de una misma marca; no se descarta ningún registro y SILVER = GOLD en total de sustraídos.
- **¿Qué falta?** Ubicación geográfica de los robos (no hay fuente abierta), y una ejecución programada (propuesta: `cron` semanal).

## Opcional: ejecución programada

Una línea de `cron` (`crontab -e`) que revisa cada lunes si hay archivos nuevos en `data/raw/` y actualiza el tablero; si no hay cambios tarda unos 18 segundos:

```
0 7 * * 1  cd ~/big-data-sustraccion-equipos-moviles-peru && . ~/BigData_UPAO/bigdata_env/bin/activate && SPARK_DRIVER_MEMORY=3g python src/pipeline.py >> ~/pipeline_cron.log 2>&1
```

La descarga de los archivos nuevos desde datosabiertos.gob.pe sigue siendo manual, y la VM debe estar encendida a esa hora.
