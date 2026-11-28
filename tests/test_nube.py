"""Pruebas del soporte de nube (no requieren AWS ni Spark): rutas por zona y comandos de la AWS CLI.
    python -m unittest discover -s tests -v
"""
import importlib, os, sys, unittest
from unittest import mock
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

VARS = {"OSIPTEL_S3_BRONZE": "g11-bronze", "OSIPTEL_S3_SILVER": "g11-silver", "OSIPTEL_S3_GOLD": "g11-gold", "AWS_REGION": "us-east-2"}


def cargar(env):
    """Recarga config (y nube) con un entorno dado."""
    limpio = {k: v for k, v in os.environ.items() if not k.startswith("OSIPTEL_S3_") and k != "OSIPTEL_LAKE_URI"}
    limpio.update(env)
    with mock.patch.dict(os.environ, limpio, clear=True):
        import config, nube
        importlib.reload(config); importlib.reload(nube)
        return config, nube


class TestConfigLocal(unittest.TestCase):
    def test_sin_variables_todo_es_local(self):
        c, _ = cargar({})
        self.assertFalse(c.ES_NUBE)
        self.assertTrue(c.ruta("silver", "robos").startswith("file://"))
        self.assertTrue(c.ruta("gold", "gold_operadora_mes").startswith("file://"))

    def test_variables_incompletas_no_activan_la_nube(self):
        c, _ = cargar({"OSIPTEL_S3_BRONZE": "x"})
        self.assertFalse(c.ES_NUBE)


class TestConfigNube(unittest.TestCase):
    def setUp(self):
        self.c, self.n = cargar(VARS)

    def tearDown(self):
        cargar({})          # deja config en modo local para las demás pruebas

    def test_nube_activa(self):
        self.assertTrue(self.c.ES_NUBE)
        self.assertEqual(self.c.REGION, "us-east-2")

    def test_rutas_por_zona(self):
        r = self.c.ruta
        self.assertEqual(r("silver", "robos"), "s3a://g11-silver/silver/robos")
        self.assertEqual(r("quarantine", "robos"), "s3a://g11-silver/quarantine/robos")
        self.assertEqual(r("_control", "cargas"), "s3a://g11-silver/_control/cargas")
        self.assertEqual(r("gold", "gold_operadora_mes"), "s3a://g11-gold/gold/gold_operadora_mes")

    def test_subir_raw(self):
        cmd = self.n.subir_raw("/tmp/origen", simular=True)
        self.assertEqual(cmd[:3], ["aws", "s3", "sync"])
        self.assertEqual(cmd[3:5], ["/tmp/origen", "s3://g11-bronze/raw/"])
        self.assertIn("us-east-2", cmd)
        self.assertNotIn("--delete", cmd)       # los originales nunca se borran del bucket

    def test_bajar_raw_y_staging(self):
        self.assertEqual(self.n.bajar_raw(simular=True)[3], "s3://g11-bronze/raw/")
        self.assertEqual(self.n.subir_staging(simular=True)[4], "s3://g11-bronze/staging/")

    def test_resultados_van_al_bucket_gold(self):
        sync, cp = self.n.subir_resultados(simular=True)
        self.assertEqual(sync[4], "s3://g11-gold/evidencia/")
        self.assertEqual(cp[:3], ["aws", "s3", "cp"])
        self.assertEqual(cp[4], "s3://g11-gold/consumo/index.html")
        self.assertIn("text/html; charset=utf-8", cp)

    def test_evidencia_se_recupera_de_gold(self):
        self.assertEqual(self.n.bajar_evidencia(simular=True)[3], "s3://g11-gold/evidencia/")


if __name__ == "__main__":
    unittest.main()


class TestDDLAthena(unittest.TestCase):
    def setUp(self):
        self.c, self.n = cargar({})
        import athena_ddl
        importlib.reload(athena_ddl); self.d = athena_ddl

    def test_ddl_con_particion(self):
        sql = self.d.ddl_tabla("gold_operadora_mes", [("operadora", "string"), ("robos", "bigint"), ("fecha_mes", "date")],
                               ["fecha_mes"], "s3a://g11-gold/gold/gold_operadora_mes")
        self.assertIn("CREATE EXTERNAL TABLE IF NOT EXISTS osiptel_gold.gold_operadora_mes", sql)
        self.assertIn("`operadora` string", sql)
        self.assertNotIn("`fecha_mes` date,", sql)                    # la partición no va entre las columnas…
        self.assertIn("PARTITIONED BY (`fecha_mes` date)", sql)        # …sino en PARTITIONED BY
        self.assertIn("LOCATION 's3://g11-gold/gold/gold_operadora_mes/'", sql)
        self.assertIn("STORED AS PARQUET", sql)

    def test_ddl_sin_particion(self):
        sql = self.d.ddl_tabla("dim_operadora", [("operadora_id", "string"), ("tipo", "string")], [], "s3a://b/gold/dim_operadora")
        self.assertNotIn("PARTITIONED", sql)
        self.assertTrue(sql.endswith(";"))


class TestStreaming(unittest.TestCase):
    def test_fuente_segun_carpeta(self):
        import streaming_bronze as sb
        self.assertEqual(sb.fuente_de("s3a://g11-bronze/raw/robos/F1.xlsx"), "robos")
        self.assertEqual(sb.fuente_de("file:/home/u/proyecto/data/raw/cobertura/c.csv"), "cobertura")
        self.assertEqual(sb.fuente_de("file:/otra/ruta/x.csv"), "")

    def test_ingesta_usa_la_version_mas_reciente(self):
        try:
            import ingesta
        except ModuleNotFoundError:
            self.skipTest('requiere pandas')
        import tempfile, time
        from pathlib import Path
        with tempfile.TemporaryDirectory() as d:
            d = Path(d); a, b = d / "2025T4.xlsx", d / "2026T1.xlsx"
            a.write_text("a"); time.sleep(0.02); b.write_text("b")
            os.utime(a, (1_000_000, 1_000_000)); os.utime(b, (2_000_000, 2_000_000))
            self.assertEqual(ingesta._ultimo(d, "*.xlsx").name, "2026T1.xlsx")
            with self.assertRaises(FileNotFoundError):
                ingesta._ultimo(d, "*.csv")
