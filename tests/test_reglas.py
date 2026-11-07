"""Pruebas de las reglas de normalización (no requieren Spark).
    python -m unittest discover -s tests -v
"""
import os, sys, unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import reglas

MARCAS_VALIDAS = {"CLARO", "MOVISTAR", "ENTEL", "BITEL", "OMV_OTROS"}


class TestNormalizarTexto(unittest.TestCase):
    def test_mayusculas_tildes_espacios(self):
        self.assertEqual(reglas.normalizar_texto("  San   Martín "), "SAN MARTIN")
        self.assertEqual(reglas.normalizar_texto("Huánuco"), "HUANUCO")

    def test_vacios_son_nulos(self):
        for v in (None, "", "   ", "nan", "NaN"):
            self.assertIsNone(reglas.normalizar_texto(v), repr(v))

    def test_numero_como_texto(self):
        self.assertEqual(reglas.normalizar_texto(2023), "2023")


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

    def test_espacios_y_minusculas(self):
        self.assertEqual(reglas.operadora("   entel   perú s.a."), "ENTEL")
        self.assertEqual(reglas.operadora("viettel peru s.a.c."), "BITEL")

    def test_cambio_razon_social_no_parte_la_serie(self):
        # Telefónica pasó a llamarse Integratel en 2025: ambas deben caer en la misma marca.
        self.assertEqual(reglas.operadora("Telefónica del Perú S.A.A."),
                         reglas.operadora("Integratel Perú S.A.A."))

    def test_siempre_devuelve_marca_valida(self):
        for v in ("", "-", "Americatel Perú S.A.", "Dolphin Mobile S.A.C.", "123", None):
            self.assertIn(reglas.operadora(v), MARCAS_VALIDAS, repr(v))


class TestDepartamento(unittest.TestCase):
    def test_lima_callao_unificados(self):
        for v in ("LIMA", "Lima", "CALLAO", "LIMA Y CALLAO"):
            self.assertEqual(reglas.departamento(v), "LIMA Y CALLAO")

    def test_lima_callao_con_espacios_y_minusculas(self):
        for v in (" lima ", "callao", "Lima  y  Callao"):
            self.assertEqual(reglas.departamento(v), "LIMA Y CALLAO", repr(v))

    def test_tildes_y_mayusculas(self):
        self.assertEqual(reglas.departamento("Junín"), "JUNIN")
        self.assertEqual(reglas.departamento("ÁNCASH"), "ANCASH")
        self.assertEqual(reglas.departamento("Ancash"), "ANCASH")

    def test_nombres_compuestos(self):
        self.assertEqual(reglas.departamento("Madre de Dios"), "MADRE DE DIOS")
        self.assertEqual(reglas.departamento("San Martín"), "SAN MARTIN")

    def test_sin_departamento(self):
        self.assertIsNone(reglas.departamento("-"))
        self.assertIsNone(reglas.departamento(None))
        self.assertIsNone(reglas.departamento(""))

    def test_idempotente(self):
        # Aplicar la regla dos veces no cambia el resultado (SILVER se puede reprocesar).
        for v in ("Lima", "Junín", "Callao", "Madre de Dios"):
            una = reglas.departamento(v)
            self.assertEqual(reglas.departamento(una), una, v)


class TestEstadoMateria(unittest.TestCase):
    def test_estado(self):
        self.assertEqual(reglas.estado("Sustraídos"), "SUSTRAIDO")
        self.assertEqual(reglas.estado("Recuperados"), "RECUPERADO")
        self.assertIsNone(reglas.estado("Bloqueados"))

    def test_estado_perdidos_y_variantes(self):
        self.assertEqual(reglas.estado("Perdidos"), "PERDIDO")
        self.assertEqual(reglas.estado(" SUSTRAIDOS "), "SUSTRAIDO")
        self.assertIsNone(reglas.estado(None))

    def test_materia_multiple(self):
        self.assertEqual(reglas.materia("Facturación y cobro|Falta de servicio"), "FACTURACION Y COBRO")
        self.assertIsNone(reglas.materia("-"))

    def test_materia_simple_y_nula(self):
        self.assertEqual(reglas.materia("Falta de servicio"), "FALTA DE SERVICIO")
        self.assertIsNone(reglas.materia(None))


if __name__ == "__main__":
    unittest.main()
