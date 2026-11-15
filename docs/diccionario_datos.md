# Diccionario de datos

Describe cada tabla del data lake (capas SILVER y GOLD): columnas, tipo, regla de origen y ejemplo.
Las reglas de normalización están en `src/reglas.py` (Python puro, probadas en `tests/test_reglas.py`)
y se aplican con PySpark en `src/transform.py` y `src/gold.py`.

**Convenciones**
- `operadora`: marca comercial normalizada. Valores: `CLARO`, `MOVISTAR`, `ENTEL`, `BITEL`, `OMV_OTROS`.
  Telefónica del Perú e Integratel Perú (cambio de razón social en 2025) se consolidan en `MOVISTAR`.
- `departamento`: en mayúsculas y sin tildes. Lima y Callao se unifican en `LIMA Y CALLAO`, porque F2 los publica
  juntos. Así las 45 escrituras distintas de F3 quedan en 24 valores.
- Fechas mensuales: tipo `date`, primer día del mes (por ejemplo, `2025-03-01`).
- Las filas que no cumplen una regla de calidad van a `quarantine/<fuente>` con la columna `motivo_rechazo`.

---

## 1. SILVER

### 1.1 `silver/robos` (fuente F1, particionada por `operadora`)
Equipos móviles reportados como sustraídos, perdidos o recuperados. Grano: **operadora × mes × estado**.

| Columna | Tipo | Origen y regla | Ejemplo |
|---|---|---|---|
| `fecha_mes` | date | `mes` truncado al primer día del mes | 2025-03-01 |
| `operadora` | string | `empresa` → `reglas.operadora()` | MOVISTAR |
| `estado` | string | `estado` normalizado: SUSTRAIDO, PERDIDO o RECUPERADO | SUSTRAIDO |
| `cantidad` | long | suma de `cantidad` (entero ≥ 0) | 31 245 |
| `razones_sociales` | array<string> | razones sociales originales agrupadas en la marca | [Telefónica del Perú S.A.A., Integratel Perú S.A.A.] |

Reglas de rechazo, en orden: `fecha_invalida` · `anio_incoherente` (el año de `mes` no coincide con `anio`) ·
`estado_fuera_dominio` · `cantidad_invalida` (nula, negativa o decimal) · `duplicado`.

### 1.2 `silver/reclamos` (fuente F2, particionada por `anio`)
Reclamos presentados ante las operadoras. Grano: **operadora × mes × departamento × servicio × materia × forma**.

| Columna | Tipo | Origen y regla | Ejemplo |
|---|---|---|---|
| `fecha_mes` | date | `mes` truncado al primer día del mes | 2023-03-01 |
| `anio` | int | año de `fecha_mes` (columna de partición) | 2023 |
| `operadora` | string | `empresa` → `reglas.operadora()` | CLARO |
| `razon_social` | string | `empresa` sin cambios (trazabilidad) | América Movil Perú S.A.C. |
| `departamento` | string | `reglas.departamento()`; nulo si venía `-` | LIMA Y CALLAO |
| `servicio` | string | `servicio` normalizado | SERVICIOS MOVILES |
| `materia` | string | primera materia si vienen varias separadas por `\|` | FALTA DE SERVICIO |
| `forma` | string | canal del reclamo, sin cambios (Telefónico, Presencial, Página web, Escrito, Otros) | Telefónico |
| `reclamos` | long | número de reclamos (entero ≥ 0) | 412 |
| `es_movil` | boolean | `servicio = 'SERVICIOS MOVILES'` | true |

Reglas de rechazo: `empresa_nula` · `fecha_invalida` · `reclamos_invalido` (nulo o negativo).
Las 17 475 filas sin departamento **no se rechazan**: se conservan con `departamento` nulo y se cuentan en `calidad.json`.

### 1.3 `silver/cobertura` (fuente F3, particionada por `operadora`)
Cobertura móvil por centro poblado, marzo de 2023. Grano: **operadora × centro poblado**.

| Columna | Tipo | Origen y regla | Ejemplo |
|---|---|---|---|
| `periodo_fecha` | date | `PERIODO` (yyyyMM) → primer día del mes | 2023-03-01 |
| `operadora` | string | `EMPRESA_OPERADORA` → `reglas.operadora()` | ENTEL |
| `departamento` | string | `DEPARTAMENTO` → `reglas.departamento()` | CUSCO |
| `ubigeo_distrito` | string | `UBIGEO_DISTRITO`, 6 dígitos | 080101 |
| `ubigeo_ccpp` | string | `UBIGEO_CCPP`, 10 dígitos | 0801010001 |
| `tiene_2g` … `tiene_5g` | int | banderas `2G` … `5G`, valores 0 o 1 | 1 |
| `estaciones_4g` | int | `CANT_EB_4G`: estaciones base 4G | 3 |

Reglas de rechazo: `ubigeo_invalido` (no tiene 10 dígitos) · `bandera_fuera_dominio` (distinta de 0 o 1) ·
`coordenada_fuera_peru` (latitud fuera de −18.5 a 0.1 o longitud fuera de −81.5 a −68.5) · `duplicado`.

---

## 2. GOLD (capa de consumo)

### 2.1 `gold/gold_operadora_mes` (particionada por `fecha_mes`, una partición por mes): hechos de J1
`silver/robos` (pivotado por estado) **FULL OUTER JOIN** con los reclamos móviles agregados, por `operadora` + `fecha_mes`.
Grano: **operadora × mes**.

| Columna | Tipo | Definición | Ejemplo |
|---|---|---|---|
| `fecha_mes` | date | mes | 2025-12-01 |
| `anio` | int | año (partición) | 2025 |
| `operadora` | string | marca | BITEL |
| `sustraidos` | long | equipos reportados como sustraídos | 24 020 |
| `recuperados` | long | equipos recuperados | 1 870 |
| `perdidos` | long | equipos reportados como perdidos | 2 310 |
| `reclamos_moviles` | long | reclamos con `es_movil = true` | 18 540 |
| `tasa_recuperacion_pct` | double | `100 × recuperados / sustraidos`, 3 decimales | 7.785 |
| `origen_join` | string | `ambas` · `solo_robos` · `solo_reclamos` | ambas |

Control: la suma de `sustraidos` es la misma en SILVER y en GOLD (15 836 038), es decir, el JOIN no infla ni pierde filas.

### 2.2 `gold/gold_depto_operadora_202303`: hechos de J2
Reclamos móviles de marzo de 2023 **LEFT JOIN** con la cobertura agregada (broadcast), por `departamento` + `operadora`.
Grano: **departamento × operadora**.

| Columna | Tipo | Definición | Ejemplo |
|---|---|---|---|
| `departamento` | string | departamento normalizado | PUNO |
| `operadora` | string | marca | CLARO |
| `periodo` | date | 2023-03-01 | 2023-03-01 |
| `reclamos_moviles` | long | reclamos móviles del mes | 640 |
| `reclamos_calidad` | long | reclamos de las materias «Calidad e idoneidad…» y «Falta de servicio» | 72 |
| `ccpp` | long | centros poblados con cobertura | 3 105 |
| `ccpp_4g` | long | centros poblados con 4G | 1 410 |
| `estaciones_4g` | long | estaciones base 4G | 2 280 |
| `pct_ccpp_4g` | double | `100 × ccpp_4g / ccpp` | 45.411 |
| `origen_join` | string | `ambas` · `solo_reclamos` | ambas |

### 2.3 Bitácora de cargas `_control/cargas`
Una fila por fuente y por corrida del pipeline. Permite saber qué entró, cuándo y cuánto, y decide si una fuente cambió.

| Columna | Tipo | Definición | Ejemplo |
|---|---|---|---|
| `lote`, `fecha_carga` | string | marca de tiempo de la corrida | 2026-11-20T10:05:12 |
| `fuente` | string | `robos`, `reclamos` o `cobertura` | robos |
| `modo` | string | `completa` · `incremental` · `sin_cambios` | incremental |
| `sha256` | string | huella del archivo fuente en esa corrida | db9f686c… |
| `filas_fuente` | long | filas del archivo fuente | 1 698 |
| `filas_nuevas` | long | filas de meses nuevos que se procesaron | 71 |
| `aceptados`, `rechazados` | long | resultado de las reglas sobre las filas nuevas | 71 · 0 |
| `meses_nuevos` | long | cantidad de meses nuevos | 5 |
| `mes_min`, `mes_max` | string | rango de los meses nuevos | 2025-09-01 · 2026-01-01 |
| `meses_lista` | string | meses nuevos (AAAA-MM separados por coma) | 2025-09,2025-10,… |

### 2.4 Dimensiones

| Tabla | Clave | Columnas |
|---|---|---|
| `gold/dim_tiempo` | `fecha_mes` | `anio`, `mes`, `trimestre`, `periodo_covid` (true entre 2020-03 y 2021-03) |
| `gold/dim_operadora` | `operadora_id` | `razones_sociales_historicas` (array), `tipo` (Operador móvil de red / OMV y otras) |
| `gold/dim_departamento` | `departamento_id` | `ubigeos_dpto` (códigos de 2 dígitos del INEI) |

Los ejemplos numéricos son ilustrativos del formato. Las cifras reales están en `docs/evidencia/`.
