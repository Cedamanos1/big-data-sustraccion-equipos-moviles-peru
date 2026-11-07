"""Pruebas de las reglas de normalización (no requieren Spark).
    python -m unittest discover -s tests -v
"""
import os, sys, unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import reglas


class TestOperadora(unittest.TestCase):
    def test_marcas(self):
        casos = {"América Movil Perú S.A.C.": "CLARO", "AMÉRICA MÓVIL PERÚ S.A.C.": "CLARO",
                 "Telefónica del Perú S.A.A.": "MOVISTAR", "Integratel Perú S.A.A.": "MOVISTAR",
                 "Integratel Perú S.A.A. (antes Telefónica del Perú S.A.A.)": "MOVISTAR",
                 "Entel Perú S.A.": "ENTEL", "Viettel Perú S.A.C.": "BITEL",
                 "Flash Servicios Perú S.R.L.": "OMV_OTROS", "Guinea Mobile S.A.C": "OMV_OTROS"}
        for raw, esperado in casos.items():
            self.assertEqual(reglas.operadora(raw), esperado, raw)

    def test_nulo(self):
        self.assertEqual(reglas.operadora(None), "OMV_OTROS")


class TestDepartamento(unittest.TestCase):
    def test_lima_callao_unificados(self):
        for v in ("LIMA", "Lima", "CALLAO", "LIMA Y CALLAO"):
            self.assertEqual(reglas.departamento(v), "LIMA Y CALLAO")

    def test_tildes_y_mayusculas(self):
        self.assertEqual(reglas.departamento("Junín"), "JUNIN")
        self.assertEqual(reglas.departamento("ÁNCASH"), "ANCASH")
        self.assertEqual(reglas.departamento("Ancash"), "ANCASH")

    def test_sin_departamento(self):
        self.assertIsNone(reglas.departamento("-"))
        self.assertIsNone(reglas.departamento(None))


class TestEstadoMateria(unittest.TestCase):
    def test_estado(self):
        self.assertEqual(reglas.estado("Sustraídos"), "SUSTRAIDO")
        self.assertEqual(reglas.estado("Recuperados"), "RECUPERADO")
        self.assertIsNone(reglas.estado("Bloqueados"))

    def test_materia_multiple(self):
        self.assertEqual(reglas.materia("Facturación y cobro|Falta de servicio"), "FACTURACION Y COBRO")
        self.assertIsNone(reglas.materia("-"))


if __name__ == "__main__":
    unittest.main()
