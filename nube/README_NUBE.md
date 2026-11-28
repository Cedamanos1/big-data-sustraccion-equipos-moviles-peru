# Despliegue en AWS (us-east-2) — guía paso a paso

Estado: **código listo, pendiente de ejecutar en la cuenta**. Lo que se dibuje en el informe debe ser lo que aquí se ejecute y se capture.

Arquitectura: ver `docs/diagramas/despliegue_nube_borrador.png`. Un bucket S3 por zona (bronze, silver, gold), una EC2 `m7i-flex.large`
(2 vCPU, 8 GB; la más grande que permite el plan gratuito) con rol de IAM, Athena + Glue Data Catalog para consultar GOLD.

| Conexión | Intermediario | Script |
|---|---|---|
| Fuente → Bronze | **Spark Structured Streaming** (`readStream` sobre `raw/`, checkpoint en S3) + registro SHA-256 en `archivos_llegados`; la carga a S3 es manual (consola o AWS CLI) | `src/streaming_bronze.py`, `src/ingesta.py` |
| Bronze → Silver | PySpark en la EC2 por `s3a://` | `src/transform.py`, `src/incremental.py` |
| Silver → Gold | PySpark en la EC2 | `src/gold.py` |
| Gold → Consumo | Athena + Glue Data Catalog | `src/athena_ddl.py` |
| Gold → Modelo | scikit-learn, modelo exportado (`.joblib`) | `src/modelo.py` |
| Consumo → Tablero | HTML en S3 (URL prefirmada) | `src/tablero.py`, `src/nube.py url-tablero` |

## 1. Crear la infraestructura (CloudShell, una sola vez)
```bash
git clone https://github.com/Cedamanos1/big-data-sustraccion-equipos-moviles-peru.git
cd big-data-sustraccion-equipos-moviles-peru
bash nube/crear_infra.sh tu_correo@upao.edu.pe
```
Anota el **prefijo de buckets** (`osiptel-g11-<cuenta>`). Captura: consola S3 con los 3 buckets; IAM con el rol; EC2 en ejecución.

## 2. Subir los originales a Bronze
En la consola de S3, bucket `…-bronze`: crea la carpeta `raw/` con tres subcarpetas `robos/`, `reclamos/`, `cobertura/` y arrastra
a cada una su archivo original (el mismo que usas en `data/raw/`). Captura: contenido de `raw/`.

## 3. Entrar a la EC2 e instalar
Consola EC2 → la instancia `osiptel-g11` → **Conectar** → **Administrador de sesiones** → Conectar (sin claves ni puertos abiertos).
```bash
curl -O https://raw.githubusercontent.com/Cedamanos1/big-data-sustraccion-equipos-moviles-peru/main/nube/instalar_ec2.sh
bash instalar_ec2.sh osiptel-g11-<cuenta>
```
Cierra la sesión y abre otra (carga las variables). Comprueba: `ls ~/proyecto/docs/evidencia/*.joblib` (modelo ya entrenado).

## 4. Ejecutar el stream y el pipeline
```bash
cd ~/proyecto && . .venv/bin/activate
python src/nube.py estado          # ve qué hay en cada bucket
python src/streaming_bronze.py     # el stream detecta los originales en raw/ y los registra en BRONZE (archivos_llegados)
python src/streaming_bronze.py --procesar   # o: detecta y, si llegó algo, corre el pipeline incremental
python src/pipeline.py             # primera vez: carga inicial completa; después: incremental
python src/pipeline.py --estado    # bitácora _control/cargas
```
Captura: la salida del pipeline (`[NUBE] ...`), y los buckets con `silver/`, `gold/`, `_control/`, `evidencia/`, `consumo/`.

## 5. Consultar GOLD con Athena
```bash
python src/athena_ddl.py --ejecutar
```
Consola Athena → base `osiptel_gold` → `SELECT * FROM gold_operadora_mes LIMIT 10;`. Captura: resultado.

## 6. Ver el tablero
```bash
python src/nube.py url-tablero     # URL prefirmada (vence en 1 h); el bucket sigue privado
```

## 7. Demostrar el stream y la carga incremental
En una sesión de la EC2: `python src/streaming_bronze.py --continuo --procesar` (queda escuchando).
En la consola de S3, sube un archivo con meses nuevos a `bronze/raw/<fuente>/` **con otro nombre** (p. ej. `…_2026T1.xlsx`; el stream detecta por ruta y
la ingesta usa el más reciente). En unos 30 s aparece `[STREAM] micro-lote … 1 archivo(s) nuevo(s)`, luego solo se procesan los meses nuevos
y el modelo se aplica sin reentrenar. Prueba en Athena que el mes nuevo aparece en Gold.

## 8. Apagar
EC2 → Detener instancia al terminar cada sesión. S3 y Athena cobran solo por uso (centavos con este volumen).

## Límites conocidos (decirlos en la sustentación)
- La descarga desde el portal es manual: el portal de OSIPTEL publica una carpeta de SharePoint, no URLs directas.
- El incremental detecta archivos/meses nuevos, no correcciones a meses ya cargados.
- Sin orquestador: el pipeline se lanza a mano (cron o EventBridge quedan como evolución).
- El stream es de baja frecuencia (OSIPTEL publica por trimestre): se usa Structured Streaming por su checkpoint incremental, no por latencia. Kafka/Kinesis quedan como evolución si hubiera un productor continuo.
- Si se sube un archivo con el MISMO nombre que uno ya visto, el stream no lo vuelve a detectar (rastrea por ruta); usar un nombre nuevo. El SHA-256 de la ingesta sigue siendo la compuerta que decide si hay datos nuevos.
- Para reprocesar todo desde cero: borrar `s3://<bronze>/_checkpoints/llegadas`.
