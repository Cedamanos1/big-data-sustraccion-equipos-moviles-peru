"""Pipeline oficial OSIPTEL: XLSX + ZIP históricos + CSV de cobertura -> RAW / SILVER / GOLD.

Requisitos: PySpark en el entorno JupyterLab y openpyxl para XLSX.
La adquisición de datos originales se realiza manualmente en data/raw/.
No construye hechos de robo por ubicación a partir de la cobertura.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import hmac
import io
import json
import os
import re
import secrets
import shutil
import sys
import time
import unicodedata
import zipfile
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RAW = ROOT / 'data' / 'raw'
STAGING = ROOT / 'data' / 'staging'
LOCAL_LAKE = ROOT / 'data' / 'lake'
OUTPUT = os.environ.get('OSIPTEL_LAKE_URI', str(LOCAL_LAKE)).rstrip('/')
EVIDENCE = ROOT / 'docs' / 'evidence'

SOURCE_DIRS = {
    'agregados': RAW / '01_agregados',
    'historicos': RAW / '02_historicos',
    'cobertura': RAW / '03_cobertura',
}
STAGING_COLUMNS = {
    'agregados': ['operadora', 'anio', 'mes', 'estado', 'cantidad', 'archivo_origen'],
    'historicos': ['id_transaccion', 'imei_hash', 'fechahora', 'reporte', 'pais', 'operadora', 'tecnologia', 'archivo_origen'],
    'cobertura': ['periodo', 'operadora', 'ubigeo_ccpp', 'departamento', 'provincia', 'distrito',
                  'centro_poblado', 'latitud', 'longitud', '2g', '3g', '4g', '5g',
                  'cant_eb_2g', 'cant_eb_3g', 'cant_eb_4g', 'cant_eb_5g', 'archivo_origen'],
}
ALIASES = {
 'agregados': {
  'operadora': ['empresa','empresa_operadora','operador','nombre_empresa','razon_social'],
  'anio': ['ano','anio','ano_del_reporte','a_o'],
  'mes': ['mes','mes_del_reporte'],
  'estado': ['estado_del_equipo_movil','estado_equipo_movil','estado','tipo_reporte','reporte'],
  'cantidad': ['cantidad_de_equipos_movil','cantidad_equipos_movil','cantidad_de_equipos_moviles',
               'cantidad_equipos_moviles','cantidad','total','numero_de_equipos_movil','n_de_equipos_movil'],
 },
 'historicos': {
  'id_transaccion':['id','transaccion','id_transaccion'],
  'imei':['ne','imei','numero_imei','n_imei'],
  'fechahora':['fechahora','fecha_hora','fecha_de_reporte','fecha_registro','fecha'],
  'reporte':['reporte','tipo_reporte','estado','tipo'],
  'pais':['pais','pais_origen'],
  'operadora':['operador','operadora','empresa','empresa_operadora'],
  'tecnologia':['tecnologia','tecnologia_red'],
 },
 'cobertura': {
  'periodo':['periodo','periodo_declarado','fecha_periodo'],
  'operadora':['empresa_operadora','operador','empresa','operadora'],
  'ubigeo_ccpp':['ubigeo_ccpp','ubigeo_centro_poblado','ubigeo','codigo_centro_poblado'],
  'departamento':['departamento','dpto'], 'provincia':['provincia'],
  'distrito':['distrito'], 'centro_poblado':['centro_poblado','centropoblado'],
  'latitud':['latitud','latitude'], 'longitud':['longitud','longitude'],
  '2g':['2g','cobertura_2g'],'3g':['3g','cobertura_3g'],
  '4g':['4g','cobertura_4g'],'5g':['5g','cobertura_5g'],
  'cant_eb_2g':['cant_eb_2g','cantidad_eb_2g'], 'cant_eb_3g':['cant_eb_3g','cantidad_eb_3g'],
  'cant_eb_4g':['cant_eb_4g','cantidad_eb_4g'], 'cant_eb_5g':['cant_eb_5g','cantidad_eb_5g'],
 },
}
REQUIRED = {
 'agregados': {'operadora','anio','mes','estado','cantidad'},
 'historicos': {'fechahora','reporte','operadora'},
 'cobertura': {'periodo','operadora','ubigeo_ccpp','departamento','2g','3g','4g','5g'},
}


def canonical(value):
    s = unicodedata.normalize('NFKD', str(value or '').replace('\ufeff',''))
    s = ''.join(c for c in s if not unicodedata.combining(c))
    return re.sub(r'_+', '_', re.sub(r'[^a-z0-9]+', '_', s.casefold())).strip('_')


def recognize_header(row, source):
    normalized = [canonical(x) for x in row]
    out = {}
    for idx, col in enumerate(normalized):
        for target, aliases in ALIASES[source].items():
            if col in aliases and target not in out:
                out[target] = idx
    return out if REQUIRED[source] <= set(out) else None


def key_for_imei():
    path = ROOT / '.secrets' / 'imei_hmac.key'
    if 'OSIPTEL_HMAC_KEY' in os.environ:
        return os.environ['OSIPTEL_HMAC_KEY'].encode('utf-8')
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return path.read_bytes()
    key = secrets.token_bytes(32)
    # Escritura exclusiva para impedir regeneraciones accidentales.
    with open(path, 'xb') as f:
        f.write(key)
    try: os.chmod(path, 0o600)
    except OSError: pass
    return key


def encode_record(row, mapcols, source, filename, hmac_key):
    def get(field):
        i = mapcols.get(field)
        if i is None or i >= len(row) or row[i] is None: return ''
        val = row[i]
        if isinstance(val, (date, datetime)): return val.isoformat(sep=' ') if isinstance(val, datetime) else val.isoformat()
        return str(val).strip()
    rec = {k: get(k) for k in STAGING_COLUMNS[source] if k != 'archivo_origen'}
    if source == 'historicos':
        imei = get('imei')
        rec.pop('imei', None)
        rec['imei_hash'] = hmac.new(hmac_key, imei.encode('utf-8'), hashlib.sha256).hexdigest() if imei else ''
    rec['archivo_origen'] = filename
    return [rec.get(k,'') for k in STAGING_COLUMNS[source]]


def sniff_stream(data, default=','):
    # Los CSV oficiales pueden venir codificados en ANSI, UTF-8 o UTF-8 con BOM.
    sample = data[:8192]
    enc = 'utf-8-sig'
    try: sample.decode(enc)
    except UnicodeDecodeError: enc = 'cp1252'
    decoded = sample.decode(enc, errors='replace')
    try: dialect = csv.Sniffer().sniff(decoded, delimiters=',;\t|'); sep = dialect.delimiter
    except csv.Error:
        first = decoded.splitlines()[0] if decoded else ''
        sep = max([',',';','\t','|'], key=lambda d: first.count(d)) if first else default
    return enc, sep


def read_csv_rows(binfile):
    """Detectar delimitador/codificación sin cargar el archivo completo en RAM."""
    # TextIOWrapper necesita un archivo binario de lectura y, para muestreo, peek.
    buffered = io.BufferedReader(binfile, buffer_size=65536)
    enc, sep = sniff_stream(buffered.peek(8192))
    txt = io.TextIOWrapper(buffered, encoding=enc, errors='replace', newline='')
    return csv.reader(txt, delimiter=sep, quotechar='"')


def convert_rows(rows, source, writer, filename, key):
    mapper = None
    count = 0
    preview = []
    for row in rows:
        if mapper is None:
            preview.append([str(v or '') for v in row][:35])
            mapper = recognize_header(row, source)
            if mapper is None:
                if len(preview) >= 50: break
                continue
            continue
        if not any(v is not None and str(v).strip() for v in row): continue
        writer.writerow(encode_record(row, mapper, source, filename, key))
        count += 1
    if mapper is None:
        raise ValueError(f'Sin cabecera compatible para {source}: {filename}. Filas iniciales: {preview[:3]}')
    return count


def convert_xlsx(path, source, target_dir, hmac_key):
    try: from openpyxl import load_workbook
    except ImportError as e: raise RuntimeError('Instala openpyxl en JupyterLab: %pip install openpyxl') from e
    wb = load_workbook(path, read_only=True, data_only=True)
    result = []
    for idx, sheet in enumerate(wb.worksheets):
        target = target_dir / f'{canonical(path.stem)}_{idx:02d}.csv'
        try:
            with target.open('w', encoding='utf-8', newline='') as out:
                writer = csv.writer(out)
                writer.writerow(STAGING_COLUMNS[source])
                n = convert_rows(sheet.iter_rows(values_only=True), source, writer,
                                 f'{path.name}:{sheet.title}', hmac_key)
            if n: result.append({'file':str(path),'sheet':sheet.title,'rows':n})
            else: target.unlink(missing_ok=True)
        except ValueError:
            target.unlink(missing_ok=True)
            continue  # Hojas de metadatos/portadas, no de datos.
    wb.close()
    if not result: raise ValueError(f'{path.name}: ninguna hoja contiene las columnas esperadas')
    return result


def convert_text(fileobj, source, target, source_name, hmac_key):
    with target.open('w', encoding='utf-8', newline='') as out:
        writer = csv.writer(out)
        writer.writerow(STAGING_COLUMNS[source])
        count = convert_rows(read_csv_rows(fileobj), source, writer, source_name, hmac_key)
    if not count: target.unlink(missing_ok=True)
    return count


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open('rb') as f:
        for part in iter(lambda: f.read(1024*1024), b''):
            digest.update(part)
    return digest.hexdigest()


def stage_raw_data():
    """RAW intacto; los CSV intermedios no contienen IMEI en claro."""
    STAGING.mkdir(parents=True, exist_ok=True)
    key = key_for_imei()
    report = {}
    for source, folder in SOURCE_DIRS.items():
        if not folder.is_dir(): raise FileNotFoundError(f'Crea la carpeta {folder}')
        items = sorted(x for x in folder.rglob('*') if x.is_file() and x.suffix.lower() in ('.csv','.txt','.zip','.xlsx'))
        if not items: raise FileNotFoundError(f'Sin archivos de {source} en {folder}. Descárgalos primero.')
        dest = STAGING / source
        if dest.exists(): shutil.rmtree(dest)
        dest.mkdir(parents=True)
        scanned = []
        errors = []
        seq = 0
        for entry in items:
            if entry.suffix.lower() == '.xlsx':
                scanned += convert_xlsx(entry, source, dest, key)
            elif entry.suffix.lower() == '.zip':
                with zipfile.ZipFile(entry) as z:
                    valid = [x for x in z.infolist() if not x.is_dir() and
                             Path(x.filename).suffix.lower() in ('.csv','.txt','.dat')]
                    if not valid: errors.append(f'{entry.name}: ZIP sin CSV/TXT/DAT legible')
                    for member in valid:
                        seq += 1
                        out = dest / f'{canonical(entry.stem)}_{seq:05d}.csv'
                        try:
                            with z.open(member) as f:
                                n = convert_text(f, source, out, f'{entry.name}:{member.filename}', key)
                            if n: scanned.append({'file':f'{entry.name}:{member.filename}','rows':n})
                        except (ValueError, UnicodeError, csv.Error) as exc:
                            out.unlink(missing_ok=True)
                            errors.append(f'{entry.name}:{member.filename}: {exc}')
            else:
                seq += 1
                out = dest / f'{canonical(entry.stem)}_{seq:05d}.csv'
                try:
                    with entry.open('rb') as f:
                        n = convert_text(f, source, out, entry.name, key)
                    if n: scanned.append({'file':entry.name,'rows':n})
                except (ValueError, UnicodeError, csv.Error) as exc:
                    out.unlink(missing_ok=True)
                    errors.append(f'{entry.name}: {exc}')
        if not scanned:
            raise ValueError(f'No se pudo leer {source}. Detalles: {errors[:5]}')
        report[source] = {'files':scanned, 'rows':sum(x['rows'] for x in scanned), 'skipped':errors,
                          'raw_provenance':[{'file':x.name,'bytes':x.stat().st_size,'sha256':sha256_file(x)} for x in items]}
        print(f'[STAGING] {source}: {report[source]["rows"]:,} registros, {len(scanned)} archivos/hojas')
        if errors: print(f'[AVISO] {source}: {len(errors)} recursos omitidos; revisar manifest.json')
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    (EVIDENCE/'manifest.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    return report


def get_spark():
    try: from pyspark.sql import SparkSession
    except ImportError as e:
        raise RuntimeError('PySpark no está disponible en el kernel seleccionado de JupyterLab.') from e
    b = SparkSession.builder.appName('BigData_OSIPTEL_Pipeline')
    if os.environ.get('SPARK_MASTER'): b = b.master(os.environ['SPARK_MASTER'])
    spark = b.config('spark.sql.shuffle.partitions','8')\
             .config('spark.sql.session.timeZone','America/Lima')\
             .config('spark.sql.ansi.enabled','false').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    return spark


def standard_operator(c):
    from pyspark.sql import functions as F
    s = F.upper(F.trim(c))
    s = F.translate(s, 'ÁÉÍÓÚÜÑ', 'AEIOUUN')
    return (F.when(s.rlike('AMERICA MOVIL|CLARO'), 'CLARO')
             .when(s.rlike('TELEFONICA|MOVISTAR'), 'MOVISTAR')
             .when(s.rlike('VIETTEL|BITEL'), 'BITEL')
             .when(s.rlike('NEXTEL'), 'NEXTEL')
             .when(s.rlike('ENTEL'), 'ENTEL')
             .otherwise(F.trim(s)))


def official_state(c):
    from pyspark.sql import functions as F
    s = F.translate(F.upper(F.trim(c)), 'ÁÉÍÓÚÜÑ', 'AEIOUUN')
    return (F.when(s.rlike('SUSTRA|ROBADO|ROBO|HURTADO|HURTO'), 'SUSTRAIDO')
             .when(s.rlike('RECUPER'), 'RECUPERADO')
             .when(s.rlike('PERDID'), 'PERDIDO')
             .otherwise('OTRO'))


def read_stage(spark, src):
    return spark.read.option('header',True).option('inferSchema',False).option('multiLine',False)\
                .option('mode','PERMISSIVE').csv(str(STAGING/src/'*.csv'))


def quality_split(df, requirements, table):
    """Devuelve válidos y rechazados con las reglas concretas auditables."""
    from pyspark.sql import functions as F
    reason_parts = [F.when(~F.coalesce(expr,F.lit(False)),F.lit(label)).otherwise(F.lit(None)) for label,expr in requirements]
    flagged = df.withColumn('motivo_rechazo',F.concat_ws('; ',*reason_parts))
    good = flagged.filter(F.col('motivo_rechazo')=='').drop('motivo_rechazo')
    bad = flagged.filter(F.col('motivo_rechazo')!='')
    # Spark acciones explícitas para evidenciar el control de calidad.
    total = flagged.count(); refused = bad.count()
    print(f'[CALIDAD] {table}: total={total:,}, aceptados={total-refused:,}, rechazados={refused:,}')
    return good,bad,{'total':total, 'aceptados':total-refused,'rechazados':refused,
                    'completitud_valida_pct':round(100*(total-refused)/total,2) if total else 0}


def silver_aggregated(spark, input_df=None):
    from pyspark.sql import functions as F
    d = input_df if input_df is not None else read_stage(spark,'agregados')
    d = (d.withColumn('operadora',standard_operator(F.col('operadora')))
           .withColumn('anio',F.regexp_replace(F.col('anio'),r'\.0+$','').cast('int'))
           .withColumn('mes',F.regexp_replace(F.col('mes'),r'\.0+$','').cast('int'))
           .withColumn('estado_original',F.col('estado'))
           .withColumn('estado',official_state(F.col('estado')))
           .withColumn('cantidad',F.regexp_replace(F.col('cantidad'),',','').cast('long')))
    d = d.withColumn('fecha_mes',F.make_date('anio','mes',F.lit(1)))
    checks = [('operadora_ausente',F.col('operadora').isNotNull() & (F.length(F.col('operadora'))>0)),
              ('anio_invalido',F.col('anio').between(2000,2100)),
              ('mes_invalido',F.col('mes').between(1,12)),
              ('fecha_invalida',F.col('fecha_mes').isNotNull()),
              ('cantidad_invalida',F.col('cantidad').isNotNull() & (F.col('cantidad')>=0)),
              ('estado_ausente',F.col('estado_original').isNotNull() & (F.length(F.trim('estado_original'))>0))]
    good,bad,q = quality_split(d,checks,'agregados')
    good = good.dropDuplicates(['operadora','anio','mes','estado_original','cantidad'] + (['benchmark_copy'] if 'benchmark_copy' in good.columns else []))
    return good,bad,q


def parse_datetime(c):
    from pyspark.sql import functions as F
    patterns = ['yyyy-MM-dd HH:mm:ss','yyyy-MM-dd HH:mm','yyyy-MM-dd\'T\'HH:mm:ss',
                'dd/MM/yyyy HH:mm:ss','dd/MM/yyyy HH:mm','d/M/yyyy H:mm:ss',
                'yyyy/MM/dd HH:mm:ss','yyyy-MM-dd','dd/MM/yyyy','yyyyMMddHHmmss']
    return F.coalesce(*[F.to_timestamp(c, p) for p in patterns])


def silver_historical(spark, input_df=None):
    from pyspark.sql import functions as F
    d = input_df if input_df is not None else read_stage(spark,'historicos')
    d = (d.withColumn('operadora',standard_operator(F.col('operadora')))
          .withColumn('pais_norm',F.translate(F.upper(F.trim('pais')), 'ÁÉÍÓÚÜÑ','AEIOUUN'))
          .withColumn('estado',official_state(F.col('reporte')))
          .withColumn('fecha_hora',parse_datetime(F.col('fechahora')))
          .withColumn('fecha_mes',F.trunc(F.col('fecha_hora'),'month')))
    checks = [('operadora_ausente',F.col('operadora').isNotNull() & (F.length(F.col('operadora'))>0)),
              ('fecha_invalida',F.col('fecha_hora').isNotNull()),
              ('pais_no_peru',F.col('pais_norm').isin('PERU','PE')),
              ('reporte_ausente',F.col('reporte').isNotNull() & (F.length(F.trim('reporte'))>0))]
    good,bad,q = quality_split(d,checks,'historicos')
    # No equiparamos reporte BLOQUEADO con sustracción. Puede repetirse un IMEI en estados diferentes.
    good = good.dropDuplicates(['id_transaccion','imei_hash','fecha_hora','reporte','operadora','pais_norm'] + (['benchmark_copy'] if 'benchmark_copy' in good.columns else []))
    return good,bad,q


def silver_coverage(spark, input_df=None):
    from pyspark.sql import functions as F
    d = input_df if input_df is not None else read_stage(spark,'cobertura')
    d = d.withColumn('operadora',standard_operator(F.col('operadora')))
    d = d.withColumn('periodo_str',F.regexp_replace(F.col('periodo'),r'\.0+$',''))
    d = d.withColumn('periodo_fecha',F.to_date(F.concat(F.col('periodo_str'),F.lit('01')),'yyyyMMdd'))
    for col in ('latitud','longitud'):
        d = d.withColumn(col,F.regexp_replace(F.col(col),',','.').cast('double'))
    for col in ('2g','3g','4g','5g','cant_eb_2g','cant_eb_3g','cant_eb_4g','cant_eb_5g'):
        d = d.withColumn(col,F.col('`'+col+'`').cast('int'))
    flags = ['2g','3g','4g','5g']
    valid_flag = F.lit(True)
    for c in flags: valid_flag = valid_flag & F.col('`'+c+'`').isin(0,1)
    checks = [('operadora_ausente',F.col('operadora').isNotNull() & (F.length('operadora')>0)),
              ('periodo_invalido',F.col('periodo_fecha').isNotNull()),
              ('ubigeo_invalido',F.col('ubigeo_ccpp').rlike(r'^\d{10}$')),
              ('departamento_ausente',F.col('departamento').isNotNull() & (F.length(F.trim('departamento'))>0)),
              ('coordenadas_invalidas',F.col('latitud').between(-18.5,1.0) & F.col('longitud').between(-82,-68)),
              ('banderas_tecnologia_invalidas',valid_flag)]
    good,bad,q = quality_split(d,checks,'cobertura')
    good = good.dropDuplicates(['periodo_str','operadora','ubigeo_ccpp'] + (['benchmark_copy'] if 'benchmark_copy' in good.columns else []))
    return good,bad,q


def make_gold(spark, agg, hist, cov, output_base=None):
    from pyspark.sql import functions as F
    output_base=output_base or OUTPUT
    def save(df,name,partition=None):
        w=df.write.mode('overwrite').format('parquet')
        if partition: w = w.partitionBy(*partition)
        w.save(f'{output_base}/gold/{name}')
        print(f'[GOLD] {name}')

    # Fuente A: equipos (conteos oficiales). Fuente B: EVENTOS (no equiparar a equipos únicos).
    a=(agg.groupBy('fecha_mes','operadora','estado').agg(F.sum('cantidad').alias('valor'))
          .withColumn('fuente',F.lit('AGREGADOS_OSIPTEL'))
          .withColumn('unidad',F.lit('EQUIPOS_REPORTADOS')))
    b=(hist.groupBy('fecha_mes','operadora','estado').agg(F.count('*').alias('valor'))
          .withColumn('fuente',F.lit('HISTORICOS_OSIPTEL'))
          .withColumn('unidad',F.lit('EVENTOS_REPORTE')))
    a=a.select('fecha_mes','operadora','estado','valor','fuente','unidad')
    b=b.select('fecha_mes','operadora','estado','valor','fuente','unidad')
    both=a.unionByName(b).withColumn('anio',F.year('fecha_mes'))
    save(both,'fact_reportes_por_fuente',['fuente','anio'])
    # Integración controlada entre fuentes: reconciliación solo por coincidencia EXACTA de mes/empresa/estado.
    # No se realiza unión de conteos: los conceptos son distintos.
    compared=(a.alias('a').join(b.alias('b'),['fecha_mes','operadora','estado'],'inner')
              .select('fecha_mes','operadora','estado',F.col('a.valor').alias('equipos_agregados'),
                      F.col('b.valor').alias('eventos_historicos')))
    save(compared,'comparacion_periodos_comunes')

    # La cobertura se mide por CENTROS POBLADOS, NO por robos ni por área o población cubierta.
    cc=(cov.groupBy('periodo_fecha','operadora','departamento')
          .agg(F.countDistinct('ubigeo_ccpp').alias('centros_poblados_con_registro'),
               *[F.countDistinct(F.when(F.col('`'+t+'`')==1,F.col('ubigeo_ccpp')))
                 .alias('centros_con_'+t) for t in ('2g','3g','4g','5g')]))
    save(cc,'cobertura_departamental')
    # Una vista por operadora para comprobar presencia interfuente SIN inferir causalidad o ubicación de delitos.
    totals=(both.groupBy('operadora','fuente','unidad')
             .agg(F.sum('valor').alias('total_historico'),F.min('fecha_mes').alias('desde'),
                  F.max('fecha_mes').alias('hasta')))
    coverage_profile=(cov.groupBy('operadora').agg(F.countDistinct('ubigeo_ccpp').alias('centros_cobertura'),
                  F.min('periodo_fecha').alias('periodo_cobertura_desde'),
                  F.max('periodo_fecha').alias('periodo_cobertura_hasta')))
    joined=(totals.join(coverage_profile,'operadora','left')
             .withColumn('comparacion_temporal_validada',F.lit(False))
             .withColumn('interpretacion',F.lit('Perfil descriptivo por operadora; NO geolocaliza robos, NO demuestra asociacion temporal')))
    save(joined,'perfil_operadora_interfuentes')
    return {'reportes':both,'reconciliacion':compared,'cobertura':cc,'perfil':joined}


def run_pipeline():
    start=time.perf_counter()
    raw_manifest=stage_raw_data()
    spark=get_spark()
    jobs = {'agregados':silver_aggregated,'historicos':silver_historical,'cobertura':silver_coverage}
    accepted={}; quality={}
    for key,fn in jobs.items():
        t0=time.perf_counter(); good,bad,summary=fn(spark)
        good.write.mode('overwrite').parquet(f'{OUTPUT}/silver/{key}')
        bad.write.mode('overwrite').parquet(f'{OUTPUT}/rejects/{key}')
        summary['tiempo_silver_s']=round(time.perf_counter()-t0,3)
        quality[key]=summary
        accepted[key]=spark.read.parquet(f'{OUTPUT}/silver/{key}')
    marts=make_gold(spark,accepted['agregados'],accepted['historicos'],accepted['cobertura'])
    quality['tiempo_total_s']=round(time.perf_counter()-start,3)
    quality['fuentes']={k:v['rows'] for k,v in raw_manifest.items()}
    quality['comentarios']=[
      'Los conteos de la fuente agregada no se suman con los eventos historicos.',
      'Las ubicaciones de cobertura NO son ubicaciones de robos.',
      'No se denomina tasa de recuperacion de cohortes al cociente entre recuperados y sustraidos.',
      'Comparacion_periodos_comunes puede ser vacia cuando las fuentes no se superponen.'
    ]
    EVIDENCE.mkdir(parents=True,exist_ok=True)
    (EVIDENCE/'calidad.json').write_text(json.dumps(quality,ensure_ascii=False,indent=2),encoding='utf-8')
    print('\n[COMPLETADO] Pipeline RAW -> SILVER -> GOLD')
    print('[EVIDENCIA]',EVIDENCE/'calidad.json')
    print('[GOLD] Visualiza desde Spark:',f'{OUTPUT}/gold/fact_reportes_por_fuente')
    return spark,quality,marts


def benchmark(spark=None, factors=(1,10,100)):
    """Réplicas reales SOLO para rendimiento; nunca se guardan como hechos ni alimentan modelos."""
    from pyspark.sql import functions as F
    spark=spark or get_spark()
    data=spark.read.parquet(f'{OUTPUT}/silver/agregados').select('operadora','estado','cantidad')
    rows=[]
    for factor in factors:
        start=time.perf_counter()
        replicated=data.crossJoin(F.broadcast(spark.range(int(factor)).select(F.col('id').alias('repeticion'))))
        result=replicated.groupBy('operadora','estado').agg(F.sum('cantidad').alias('total'))
        n=result.count()  # fuerza el shuffle; no persiste la réplica
        # count de salida es distinto del tamaño de entrada; se informa por separado.
        rows.append({'factor':int(factor),'grupos_salida':n,'tiempo_s':round(time.perf_counter()-start,3)})
        print('[ESCALABILIDAD]',rows[-1])
    EVIDENCE.mkdir(parents=True,exist_ok=True)
    (EVIDENCE/'escalabilidad.json').write_text(json.dumps(rows,indent=2),encoding='utf-8')
    return rows



def benchmark_full(spark=None, factors=(1,10,100)):
    """Prueba de escalabilidad RAW normalizada -> SILVER -> GOLD.

    Replica datos reales ÚNICAMENTE dentro de data/lake/_benchmark (se borra por factor).
    Los datos replicados NO ingresan a los análisis, predicciones o métricas oficiales.
    La etapa inicial XLSX/ZIP -> staging no se replica; se mide todo el ETL PySpark.
    """
    from pyspark.sql import functions as F
    spark=spark or get_spark()
    results=[]
    for factor in factors:
        if factor < 1: raise ValueError('factor debe ser >= 1')
        start=time.perf_counter()
        folder=f'{OUTPUT}/_benchmark/factor_{factor}'
        # Importante: puede consumir MUCHOS GB. No ejecutar 100x sin recursos suficientes.
        views={}; measurements={}
        for source,clean_fn in [('agregados',silver_aggregated),
                                ('historicos',silver_historical),('cobertura',silver_coverage)]:
            raw=read_stage(spark,source)
            repeated=raw.crossJoin(F.broadcast(spark.range(int(factor)).select(F.col('id').alias('benchmark_copy'))))
            cleaned,rejected,report=clean_fn(spark,repeated)
            cleaned.write.mode('overwrite').parquet(f'{folder}/silver/{source}')
            rejected.write.mode('overwrite').parquet(f'{folder}/rejects/{source}')
            measurements[source]=report
            views[source]=spark.read.parquet(f'{folder}/silver/{source}')
        make_gold(spark,views['agregados'],views['historicos'],views['cobertura'],output_base=folder)
        results.append({'factor':int(factor),'segundos':round(time.perf_counter()-start,2),
                        'mediciones':measurements,'nota':'SOLO ESCALABILIDAD; NO USAR EN ANALISIS'})
        print('[BENCHMARK COMPLETO]',factor,results[-1]['segundos'],'segundos')
        # No borramos el resultado HDFS automáticamente; revisa y elimina _benchmark al terminar.
    EVIDENCE.mkdir(parents=True,exist_ok=True)
    (EVIDENCE/'escalabilidad_pipeline.json').write_text(json.dumps(results,indent=2,ensure_ascii=False),encoding='utf-8')
    return results


def benchmark_partition(spark=None, year=None):
    """Compara misma consulta a fact SIN particiones vs fact particionada por fuente/año.
    Resultados pueden variar según hardware/cache; medir, no asumir mejora.
    """
    from pyspark.sql import functions as F
    spark=spark or get_spark()
    df=spark.read.parquet(f'{OUTPUT}/gold/fact_reportes_por_fuente')
    year=year or df.agg(F.max('anio')).first()[0]
    if year is None: raise ValueError('No hay años para medir')
    dest=f'{OUTPUT}/_benchmark/no_partition'
    df.write.mode('overwrite').parquet(dest)
    times={}
    for desc,path in [('sin_particionar',dest),('particionado',f'{OUTPUT}/gold/fact_reportes_por_fuente')]:
        start=time.perf_counter()
        result=(spark.read.parquet(path).filter(F.col('anio')==int(year))
                   .groupBy('operadora').agg(F.sum('valor').alias('n'))).collect()
        times[desc]={'segundos':round(time.perf_counter()-start,4),'grupos':len(result),'anio':int(year)}
    EVIDENCE.mkdir(parents=True,exist_ok=True)
    (EVIDENCE/'optimizacion.json').write_text(json.dumps(times,indent=2),encoding='utf-8')
    print('[OPTIMIZACIÓN]',times)
    return times


if __name__ == '__main__':
    p=argparse.ArgumentParser(description='ETL OSIPTEL en Spark')
    p.add_argument('--benchmark',action='store_true',help='Prueba parcial de agregación 1x,10x,100x')
    p.add_argument('--benchmark-full',action='store_true',help='ETL Spark completo sobre réplicas SOLO para rendimiento')
    p.add_argument('--optimize',action='store_true',help='Benchmark de particionado Parquet vs sin particionar')
    p.add_argument('--factors',nargs='+',type=int,default=[1,10,100])
    a=p.parse_args()
    if a.benchmark_full: benchmark_full(factors=a.factors)
    elif a.benchmark: benchmark(factors=a.factors)
    elif a.optimize: benchmark_partition()
    else: run_pipeline()
