"""Pruebas de la lógica de la carga incremental (no requieren Spark).
    python -m unittest discover -s tests -v
"""
import os, sys, unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import incremental as inc


class TestMesesNuevos(unittest.TestCase):
    def test_solo_los_que_faltan(self):
        fuente = ["2025-01-01", "2025-02-01", "2025-03-01", "2025-04-01"]
        self.assertEqual(inc.meses_nuevos(fuente, ["2025-01-01", "2025-02-01"]), ["2025-03-01", "2025-04-01"])

    def test_primera_carga_todo_es_nuevo(self):
        self.assertEqual(inc.meses_nuevos(["2025-02-01", "2025-01-01"], []), ["2025-01-01", "2025-02-01"])

    def test_sin_datos_nuevos(self):
        self.assertEqual(inc.meses_nuevos(["2025-01-01", "2025-02-01"], ["2025-01-01", "2025-02-01", "2025-03-01"]), [])

    def test_ignora_nulos_y_repetidos(self):
        self.assertEqual(inc.meses_nuevos([None, "2025-05-01", "2025-05-01", None], []), ["2025-05-01"])

    def test_detecta_un_hueco_historico(self):
        # un mes intermedio que llegó tarde se carga aunque haya meses posteriores (por eso no se usa «mayor que el último»)
        self.assertEqual(inc.meses_nuevos(["2025-01-01", "2025-02-01", "2025-03-01"], ["2025-01-01", "2025-03-01"]), ["2025-02-01"])

    def test_idempotente(self):
        # cargar dos veces el mismo lote: la segunda vez no queda nada por procesar
        fuente, cargados = ["2025-01-01", "2025-02-01"], []
        cargados += inc.meses_nuevos(fuente, cargados)
        self.assertEqual(inc.meses_nuevos(fuente, cargados), [])

    def test_cargas_por_tandas_equivalen_a_una_sola(self):
        # lo cargado en dos tandas es lo mismo que en una (propiedad que justifica el diseño por meses)
        todo = [f"2025-{m:02d}-01" for m in range(1, 13)]
        cargados = []
        for tanda in (todo[:5], todo[:9], todo):
            cargados += inc.meses_nuevos(tanda, cargados)
        self.assertEqual(sorted(cargados), todo)
        self.assertEqual(len(cargados), len(set(cargados)))  # ningún mes cargado dos veces


class TestDecidirModo(unittest.TestCase):
    def test_archivo_igual_no_se_procesa(self):
        self.assertEqual(inc.decidir_modo("abc", "abc"), "sin_cambios")

    def test_archivo_distinto_es_incremental(self):
        self.assertEqual(inc.decidir_modo("abc", "xyz"), "incremental")

    def test_fuente_sin_historial_es_incremental(self):
        self.assertEqual(inc.decidir_modo("abc", None), "incremental")


class TestPlanGold(unittest.TestCase):
    def test_sin_cambios_no_recalcula_nada(self):
        p = inc.plan_gold([], False, True, True)
        self.assertEqual(p, {"j1_meses": [], "recalcular_j2": False, "recalcular_dimensiones": False})

    def test_meses_desfasados_se_recalculan_sin_duplicar(self):
        p = inc.plan_gold(["2025-09-01", "2025-08-01", "2025-09-01"], False, True, True)
        self.assertEqual(p["j1_meses"], ["2025-08-01", "2025-09-01"])
        self.assertTrue(p["recalcular_dimensiones"])
        self.assertFalse(p["recalcular_j2"])

    def test_j2_solo_si_llega_cobertura_o_cambia_marzo_2023(self):
        self.assertTrue(inc.plan_gold([], True, True, True)["recalcular_j2"])
        self.assertTrue(inc.plan_gold(["2023-03-01"], False, True, True)["recalcular_j2"])
        self.assertFalse(inc.plan_gold(["2025-01-01"], False, True, True)["recalcular_j2"])

    def test_tablas_ausentes_se_construyen(self):
        p = inc.plan_gold([], False, False, False)
        self.assertTrue(p["recalcular_j2"] and p["recalcular_dimensiones"])


class TestResumenMeses(unittest.TestCase):
    def test_resumen(self):
        self.assertEqual(inc.resumen_meses(["2025-10-01", "2025-08-01"]), ("2025-08-01", "2025-10-01", "2025-08,2025-10"))

    def test_vacio(self):
        self.assertEqual(inc.resumen_meses([]), (None, None, ""))


if __name__ == "__main__":
    unittest.main()
