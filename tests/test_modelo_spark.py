"""Pruebas de la parte pura del modelo con Spark MLlib (no requieren Spark).
    python -m unittest discover -s tests -v
"""
import os, sys, unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import modelo_spark as ms


class TestParticionesTemporales(unittest.TestCase):
    def test_ventana_creciente_sin_mirar_el_futuro(self):
        for fin_tr, ini_va, fin_va in ms.particiones_temporales(100, 4):
            self.assertEqual(fin_tr, ini_va)         # se valida justo después de lo entrenado
            self.assertLess(fin_tr, fin_va)

    def test_valores_conocidos(self):
        self.assertEqual(ms.particiones_temporales(100, 4), [(20, 20, 40), (40, 40, 60), (60, 60, 80), (80, 80, 100)])

    def test_cantidad_de_pliegues(self):
        self.assertEqual(len(ms.particiones_temporales(317, 4)), 4)

    def test_coincide_con_scikit_learn(self):
        try:
            from sklearn.model_selection import TimeSeriesSplit
            import numpy as np
        except ImportError:
            self.skipTest("scikit-learn no instalado")
        for n in (57, 100, 317):
            esperado = [(int(a.max()) + 1, int(b.min()), int(b.max()) + 1) for a, b in TimeSeriesSplit(4).split(np.arange(n))]
            self.assertEqual(ms.particiones_temporales(n, 4), esperado)


class TestCombinaciones(unittest.TestCase):
    def test_producto_de_la_malla(self):
        c = ms._combinaciones({"a": [1, 2], "b": [10, 20, 30]})
        self.assertEqual(len(c), 6)
        self.assertIn({"a": 2, "b": 30}, c)

    def test_malla_de_un_valor(self):
        self.assertEqual(ms._combinaciones({"a": [1]}), [{"a": 1}])


if __name__ == "__main__":
    unittest.main()
