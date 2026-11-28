"""Pruebas de preparar() con el mes futuro (requieren pandas y scikit-learn; se omiten si faltan).
    python -m unittest discover -s tests -v
"""
import os, sys, unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
try:
    import numpy as np
    import pandas as pd
    import modelo as m
except ImportError:      # pragma: no cover
    m = None


def serie(meses=36):
    filas = []
    for op in m.OPS:
        for i, f in enumerate(pd.date_range("2022-01-01", periods=meses, freq="MS")):
            filas.append({"fecha_mes": f, "operadora": op, "sustraidos": 1000 + 10 * i})
    return pd.DataFrame(filas)


@unittest.skipIf(m is None, "faltan pandas o scikit-learn")
class TestPrepararFuturo(unittest.TestCase):
    def test_una_fila_futura_por_operadora(self):
        futuro, _, _ = m.preparar(serie(), 1, futuro=True)
        self.assertEqual(len(futuro), len(m.OPS))
        self.assertEqual(set(futuro.operadora), set(m.OPS))

    def test_el_mes_futuro_es_el_siguiente_al_ultimo(self):
        g = serie()
        futuro, _, _ = m.preparar(g, 1, futuro=True)
        esperado = g.fecha_mes.max() + pd.DateOffset(months=1)
        self.assertTrue((futuro.fecha_mes == esperado).all())

    def test_el_mes_futuro_no_tiene_valor_real(self):
        futuro, _, _ = m.preparar(serie(), 1, futuro=True)
        self.assertTrue(futuro.sustraidos.isna().all())

    def test_la_base_es_el_ultimo_valor_conocido(self):
        g = serie()
        futuro, _, _ = m.preparar(g, 1, futuro=True)
        ultimo = g[g.fecha_mes == g.fecha_mes.max()].set_index("operadora").sustraidos
        for _, fila in futuro.iterrows():
            self.assertEqual(fila.base, ultimo[fila.operadora])


if __name__ == "__main__":
    unittest.main()
