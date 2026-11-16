"""Pruebas de la capa analítica que no requieren Spark: consistencia de las consultas SQL y reconciliación.
    python -m unittest discover -s tests -v
"""
import json, os, re, sys, tempfile, unittest
from pathlib import Path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import analitica as an

CTES = {"base", "corte", "u12", "p12", "t", "m"}


class TestConsultas(unittest.TestCase):
    def test_solo_leen_tablas_gold_o_ctes(self):
        for nombre, sql in an.CONSULTAS.items():
            for tabla in re.findall(r"(?:FROM|JOIN)\s+(\w+)", sql):
                self.assertTrue(tabla in an.VISTAS or tabla in CTES, f"{nombre} lee «{tabla}», que no es una vista GOLD")

    def test_ejemplos_solo_leen_tablas_analiticas(self):
        for pregunta, sql in an.EJEMPLOS.items():
            for tabla in re.findall(r"(?:FROM|JOIN)\s+(\w+)", sql):
                self.assertIn(tabla, an.CONSULTAS, pregunta)

    def test_las_cuatro_tablas_analiticas(self):
        self.assertEqual(set(an.CONSULTAS), {"an_kpi_operadora", "an_serie_mensual", "an_resumen_anual", "an_depto_calidad"})


def _op(op, sus12=1000, rec12=100, tasa=10.0, var=5.0, brecha=1.5, pot=15, estado="Prioritaria"):
    return dict(op=op, sus12=sus12, rec12=rec12, tasa12=tasa, var_anual=var, brecha_vs_mediana=brecha, potencial=pot, estado=estado)


class TestReconciliar(unittest.TestCase):
    def test_iguales(self):
        a = [_op("CLARO"), _op("ENTEL", estado="En seguimiento")]
        self.assertTrue(an.reconciliar(a, [dict(o) for o in a])["ok"])

    def test_redondeo_tolerado(self):
        a, b = [_op("CLARO")], [_op("CLARO", tasa=10.005, var=5.1, pot=16)]
        self.assertTrue(an.reconciliar(a, b)["ok"])

    def test_detecta_diferencia_en_enteros(self):
        r = an.reconciliar([_op("CLARO")], [_op("CLARO", sus12=1001)])
        self.assertFalse(r["ok"])
        self.assertIn("CLARO.sus12", r["diferencias"])

    def test_detecta_cambio_de_estado(self):
        self.assertFalse(an.reconciliar([_op("CLARO")], [_op("CLARO", estado="Sobre la mediana")])["ok"])

    def test_detecta_operadora_faltante(self):
        self.assertFalse(an.reconciliar([_op("CLARO"), _op("ENTEL")], [_op("CLARO")])["ok"])


class TestTableroConTablasAnaliticas(unittest.TestCase):
    """El tablero debe producir lo mismo leyendo las tablas analíticas que calculando en pandas."""

    def test_flujo_con_an(self):
        try:
            import pandas as pd, numpy as np
            import tablero
        except ImportError:
            self.skipTest("pandas/numpy no instalados")
        meses = pd.date_range("2023-02-01", "2026-01-01", freq="MS")
        rng = np.random.default_rng(0)
        filas = [dict(fecha_mes=m, operadora=op, sustraidos=int(rng.integers(8000, 20000)),
                      recuperados=int(rng.integers(100, 900)), reclamos_moviles=int(rng.integers(1000, 5000)))
                 for m in meses for op in tablero.OPS]
        g = pd.DataFrame(filas)
        j = pd.DataFrame([dict(departamento=d, operadora="CLARO", ccpp=10, ccpp_4g=4, reclamos_moviles=500, reclamos_calidad=100)
                          for d in ["LIMA", "CUSCO"]])
        with tempfile.TemporaryDirectory() as t:
            ev = Path(t)
            pd.DataFrame([dict(fecha_mes="2026-02-01", operadora=op, base=1000, ultimo_observado=1000, pronostico=1100,
                               mae_prueba=50.0, variacion_pct=10.0) for op in tablero.OPS]).to_csv(ev / "pronostico_mes_siguiente.csv", index=False)
            m = dict(metricas={"gradient_boosting": dict(MAPE=6.1, MAE=1700), "ingenuo": dict(MAPE=7.0, MAE=1900)})
            (ev / "modelo_metricas.json").write_text(json.dumps({"H1": m}), encoding="utf-8")
            pd.DataFrame([dict(fecha_mes="2025-01-01", operadora="CLARO", sustraidos=10, gradient_boosting=11, ingenuo=12)]
                         ).to_csv(ev / "predicciones_prueba_H1.csv", index=False)
            base = tablero.construir_datos(g, j, ev)
            kpi = pd.DataFrame([dict(operadora=o["op"], sus12=o["sus12"], rec12=o["rec12"], tasa12=o["tasa12"],
                                     var_anual=o["var_anual"], brecha_vs_mediana=o["brecha_vs_mediana"],
                                     potencial=o["potencial"], estado=o["estado"]) for o in base["operadoras"]])
            dep = pd.DataFrame([dict(departamento="LIMA", reclamos_moviles=500, reclamos_calidad=100, pct_calidad=20.0, pct_4g=40.0)])
            con_an = tablero.construir_datos(g, j, ev, an={"an_kpi_operadora": kpi, "an_depto_calidad": dep})
        self.assertTrue(con_an["reconciliacion"]["ok"])
        self.assertEqual([o["op"] for o in con_an["operadoras"]], [o["op"] for o in base["operadoras"]])
        self.assertEqual([d["d"] for d in con_an["deptos"]], ["Lima"])      # sale de la tabla analítica
        self.assertIsNone(base["reconciliacion"])


if __name__ == "__main__":
    unittest.main()
