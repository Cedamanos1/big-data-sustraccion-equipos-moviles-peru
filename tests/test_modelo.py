"""Pruebas del modelo exportado: entrenar una vez, guardar y aplicar a datos nuevos sin reentrenar.
Usan una serie sintética pequeña; se omiten si faltan pandas o scikit-learn.
    python -m unittest discover -s tests -v
"""
import os, sys, tempfile, unittest
from pathlib import Path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
try:
    import numpy as np
    import pandas as pd
    import modelo as m
except ImportError:      # pragma: no cover
    m = None

INFO_H1 = {"hiperparametros": {"gradient_boosting": {"n_estimators": 20, "max_depth": 2, "learning_rate": 0.1}},
           "MAE_prueba_por_operadora": {"BITEL": 100.0, "CLARO": 200.0, "ENTEL": 150.0, "MOVISTAR": 300.0}}


def serie(meses):
    rng = np.random.default_rng(1)
    filas = []
    for op in ["BITEL", "CLARO", "ENTEL", "MOVISTAR"]:
        nivel = 1000.0
        for i, f in enumerate(pd.date_range("2018-01-01", periods=meses, freq="MS")):
            nivel *= 1 + 0.05 * np.sin(i / 3) + rng.normal(0, 0.02)
            filas.append({"fecha_mes": f, "operadora": op, "sustraidos": round(nivel)})
    return pd.DataFrame(filas)


@unittest.skipIf(m is None, "faltan pandas o scikit-learn")
class TestDebeReentrenar(unittest.TestCase):
    def test_sin_modelo_se_entrena(self):
        self.assertTrue(m.debe_reentrenar(False, False))

    def test_con_modelo_no_se_reentrena(self):
        self.assertFalse(m.debe_reentrenar(True, False, ["a", "b"], ["a", "b"]))

    def test_si_se_pide_se_reentrena(self):
        self.assertTrue(m.debe_reentrenar(True, True))

    def test_si_cambian_las_variables_se_reentrena(self):
        self.assertTrue(m.debe_reentrenar(True, False, ["a", "b"], ["a", "c"]))


@unittest.skipIf(m is None, "faltan pandas o scikit-learn")
class TestModeloExportado(unittest.TestCase):
    def test_guardar_cargar_da_el_mismo_pronostico(self):
        g = serie(60)
        paquete = m.entrenar_final(g, INFO_H1)
        with tempfile.TemporaryDirectory() as d:
            ruta = Path(d) / m.ARCHIVO_MODELO
            m.guardar_modelo(paquete, ruta)
            cargado = m.cargar_modelo(ruta)
        a = m.pronosticar(g, paquete, INFO_H1["MAE_prueba_por_operadora"])
        b = m.pronosticar(g, cargado, INFO_H1["MAE_prueba_por_operadora"])
        pd.testing.assert_frame_equal(a.reset_index(drop=True), b.reset_index(drop=True))

    def test_con_un_mes_nuevo_se_aplica_sin_reentrenar(self):
        g = serie(60)
        paquete = m.entrenar_final(g, INFO_H1)
        antes = m.pronosticar(g, paquete, INFO_H1["MAE_prueba_por_operadora"])
        g_nuevo = serie(61)                                  # llega un mes más
        despues = m.pronosticar(g_nuevo, paquete, INFO_H1["MAE_prueba_por_operadora"])
        self.assertEqual(antes.fecha_mes.max() + pd.DateOffset(months=1), despues.fecha_mes.max())
        self.assertEqual(len(despues), 4)                    # una fila por operadora
        self.assertEqual(paquete["entrenado_con_hasta"], str((g.fecha_mes.max()).date()))

    def test_el_pronostico_trae_las_columnas_del_tablero(self):
        g = serie(60)
        out = m.pronosticar(g, m.entrenar_final(g, INFO_H1), INFO_H1["MAE_prueba_por_operadora"])
        for c in ("fecha_mes", "operadora", "ultimo_observado", "pronostico", "mae_prueba", "variacion_pct"):
            self.assertIn(c, out.columns)


if __name__ == "__main__":
    unittest.main()
