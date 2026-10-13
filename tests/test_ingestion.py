import unittest
from io import BytesIO
import osiptel_pipeline as p

class PipelineTest(unittest.TestCase):
    def test_header_aggregate(self):
        self.assertEqual(set(p.recognize_header(['Empresa','Año','Mes','Estado del equipo móvil','Cantidad de equipos móvil'],'agregados')),{'operadora','anio','mes','estado','cantidad'})

    def test_header_historic(self):
        names='ID NE ARCHIVO FECHAHORA REPORTE PAIS OPERADOR TECNOLOGIA'.split()
        self.assertTrue(p.recognize_header(names,'historicos'))

    def test_header_coverage(self):
        names='NUM;FECHA_CORTE;PERIODO;EMPRESA_OPERADORA;UBIGEO_CCPP;UBIGEO_DISTRITO;DEPARTAMENTO;PROVINCIA;DISTRITO;CENTRO_POBLADO;LATITUD;LONGITUD;2G;3G;4G;5G'.split(';')
        self.assertTrue(p.recognize_header(names,'cobertura'))

    def test_csv_semicolon(self):
        data=BytesIO(b'PERIODO;EMPRESA_OPERADORA;UBIGEO_CCPP;DEPARTAMENTO;2G;3G;4G;5G\n202303;BITEL;0000010001;LIMA;1;0;1;0\n')
        r=p.read_csv_rows(data)
        self.assertEqual(next(r)[0],'PERIODO')
        self.assertEqual(next(r)[2],'0000010001')

    def test_imei_hash_not_plain(self):
        row=['1','123456789012345','2020-01-01 12:00:00','ROBADO','PERU','CLARO']
        headers=['id','ne','fechahora','reporte','pais','operador']
        mapping=p.recognize_header(headers,'historicos')
        value=p.encode_record(row,mapping,'historicos','archivo.csv',b'a'*32)
        self.assertNotIn('123456789012345',value)
        self.assertEqual(len(value[1]),64)

if __name__=='__main__': unittest.main()
