# Dibuja los dos diagramas del informe en SVG (control total del tamaño de texto) y los exporta a PNG.
import sys
sys.path.insert(0, "/home/claude/entrega/diagramas")
C = dict(fuente=("#eaf2fd", "#2a78d6"), dato=("#fff4e5", "#eb6834"), proc=("#f1ecfa", "#7a5bbf"),
         uso=("#eaf7ee", "#2f9e5b"), nube=("#f4f4f2", "#52514e"))
DEFS = ('<defs><marker id="ah" markerWidth="9" markerHeight="9" refX="8" refY="4.5" orient="auto"><path d="M0,0 L9,4.5 L0,9 z" fill="#3b3b38"/></marker>'
        '<marker id="ah2" markerWidth="9" markerHeight="9" refX="1" refY="4.5" orient="auto-start-reverse"><path d="M0,0 L9,4.5 L0,9 z" fill="#3b3b38"/></marker></defs>')


class Lienzo:
    def __init__(self, W, H):
        self.W, self.H, self.o, self.n = W, H, [], {}

    def caja(self, x, y, w, h, titulo, dashed=False, fill="#fafaf8", stroke="#c9c9c4", fs=19):
        d = ' stroke-dasharray="7 5"' if dashed else ""
        self.o.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="{fill}" stroke="{stroke}" stroke-width="1.6"{d}/>')
        if titulo:
            self.o.append(f'<text x="{x + w/2}" y="{y + 27}" text-anchor="middle" font-size="{fs}" font-weight="700" fill="#1f3a5f">{titulo}</text>')

    def nodo(self, k, x, y, w, h, lineas, tipo, dashed=False, fs=19):
        f, s = C[tipo]
        d = ' stroke-dasharray="6 4"' if dashed else ""
        self.o.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="8" fill="{f}" stroke="{s}" stroke-width="1.8"{d}/>')
        n = len(lineas); lh = fs + 5; y0 = y + h/2 - (n - 1) * lh/2 + fs*0.34
        for i, t in enumerate(lineas):
            self.o.append(f'<text x="{x + w/2}" y="{y0 + i*lh}" text-anchor="middle" font-size="{fs}" font-weight="{700 if i == 0 else 400}" fill="#0b0b0b">{t}</text>')
        self.n[k] = (x, y, w, h)

    def flecha(self, a, b, lado=("r", "l"), dashed=False, both=False, label=None, curva=True):
        def pt(n, s):
            x, y, w, h = self.n[n]
            return {"r": (x + w, y + h/2), "l": (x, y + h/2), "b": (x + w/2, y + h), "t": (x + w/2, y)}[s]
        (x1, y1), (x2, y2) = pt(a, lado[0]), pt(b, lado[1])
        if lado[0] in "rl" and curva and abs(y1 - y2) > 1:
            mx = (x1 + x2)/2; p = f"M{x1},{y1} C{mx},{y1} {mx},{y2} {x2},{y2}"
        else:
            p = f"M{x1},{y1} L{x2},{y2}"
        d = ' stroke-dasharray="6 4"' if dashed else ""
        st = ' marker-start="url(#ah2)"' if both else ""
        self.o.append(f'<path d="{p}" fill="none" stroke="#3b3b38" stroke-width="1.8"{d} marker-end="url(#ah)"{st}/>')
        if label:
            self.o.append(f'<text x="{(x1 + x2)/2 + 6}" y="{(y1 + y2)/2 - 6}" font-size="16" fill="#52514e">{label}</text>')

    def texto(self, x, y, t, fs=17, anchor="start", color="#333", bold=False):
        self.o.append(f'<text x="{x}" y="{y}" text-anchor="{anchor}" font-size="{fs}" fill="{color}" font-weight="{700 if bold else 400}">{t}</text>')

    def guardar(self, nombre):
        svg = (f'<svg xmlns="http://www.w3.org/2000/svg" width="{self.W}" height="{self.H}" viewBox="0 0 {self.W} {self.H}" '
               f'font-family="Calibri, Carlito, Arial, sans-serif">{DEFS}<rect width="{self.W}" height="{self.H}" fill="white"/>' + "".join(self.o) + "</svg>")
        open(f"/home/claude/entrega/diagramas/{nombre}.svg", "w", encoding="utf-8").write(svg)
        open(f"/home/claude/entrega/diagramas/{nombre}.html", "w", encoding="utf-8").write(f'<html><body style="margin:0">{svg}</body></html>')
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            b = p.chromium.launch(); pg = b.new_page(viewport={"width": self.W, "height": self.H}, device_scale_factor=2)
            pg.goto(f"file:///home/claude/entrega/diagramas/{nombre}.html"); pg.screenshot(path=f"/home/claude/entrega/diagramas/{nombre}.png", full_page=True); b.close()


def arquitectura():
    CW, GAP, X0, NW, NH, ST, Y0 = 188, 26, 16, 164, 84, 112, 62
    cx = [X0 + i*(CW + GAP) for i in range(6)]
    W = cx[-1] + CW + X0
    L = Lienzo(W, 640)
    titulos = ["1 · Fuentes", "2 · BRONZE", "3 · Spark 3.5.1", "4 · SILVER", "5 · GOLD", "6 · Consumo"]
    filas = [4, 2, 3, 3, 3, 3]
    for i, (t, n) in enumerate(zip(titulos, filas)):
        L.caja(cx[i], 14, CW, 56 + n*ST + 6, t)
    nx = lambda i: cx[i] + (CW - NW)/2
    ny = lambda r: Y0 + 14 + r*ST
    L.nodo("f1", nx(0), ny(0), NW, NH, ["F1 Sustraídos", "XLSX"], "fuente")
    L.nodo("f2", nx(0), ny(1), NW, NH, ["F2 Reclamos", "XLSX"], "fuente")
    L.nodo("f3", nx(0), ny(2), NW, NH, ["F3 Cobertura", "CSV"], "fuente")
    L.nodo("f4", nx(0), ny(3), NW, NH, ["F4 Históricos", "(escalabilidad)"], "fuente")
    L.nodo("raw", nx(1), ny(0), NW, NH, ["RAW", "originales", "+ SHA-256"], "dato")
    L.nodo("stg", nx(1), ny(1), NW, NH, ["STAGING", "CSV UTF-8"], "dato")
    L.nodo("tr", nx(2), ny(0), NW, NH, ["transform.py", "reglas de calidad"], "proc")
    L.nodo("inc", nx(2), ny(2), NW, NH, ["incremental.py", "solo meses nuevos"], "proc")
    L.nodo("si", nx(3), ny(0), NW, NH, ["silver/", "datos limpios"], "dato")
    L.nodo("q", nx(3), ny(1), NW, NH, ["quarantine/", "rechazos"], "dato")
    L.nodo("ct", nx(3), ny(2), NW, NH, ["_control/cargas", "bitácora"], "proc")
    L.nodo("gj", nx(4), ny(0), NW, NH, ["gold.py", "JOIN"], "proc")
    L.nodo("go", nx(4), ny(1), NW, NH, ["gold/", "hechos + dim."], "dato")
    L.nodo("an", nx(4), ny(2), NW, NH, ["an_* tablas", "Spark SQL"], "dato")
    L.nodo("mo", nx(5), ny(0), NW, NH, ["modelo.py", "scikit-learn"], "uso")
    L.nodo("db", nx(5), ny(1), NW, NH, ["Tablero", "HTML estático"], "uso")
    L.nodo("ml", nx(5), ny(2), NW, NH, ["modelo_spark.py", "MLlib", "(alternativa)"], "uso", dashed=True)
    for f in ("f1", "f2", "f3", "f4"): L.flecha(f, "raw")
    L.flecha("raw", "stg", ("b", "t")); L.flecha("stg", "tr")
    L.flecha("tr", "si"); L.flecha("tr", "q"); L.flecha("tr", "inc", ("b", "t"))
    L.flecha("inc", "ct", both=True); L.flecha("si", "gj")
    L.flecha("gj", "go", ("b", "t")); L.flecha("go", "an", ("b", "t"))
    L.flecha("go", "mo"); L.flecha("mo", "db", ("b", "t")); L.flecha("an", "db"); L.flecha("go", "ml", dashed=True)
    ly = 14 + 56 + 4*ST + 6 + 34
    x = 16
    for k, t in [("fuente", "Fuente externa"), ("dato", "Datos (Parquet + Snappy)"), ("proc", "Proceso / control"), ("uso", "Consumo")]:
        f, s = C[k]
        L.o.append(f'<rect x="{x}" y="{ly}" width="26" height="18" rx="3" fill="{f}" stroke="{s}" stroke-width="1.8"/>')
        L.texto(x + 34, ly + 15, t, 17); x += 36 + len(t)*9 + 26
    L.texto(x, ly + 15, "Trazo discontinuo: alternativa evaluada", 17)
    L.H = ly + 40
    L.guardar("arquitectura_lr")


def despliegue():
    CW, GAP, X0, NW, NH = 188, 26, 16, 164, 92
    cx = [X0 + i*(CW + GAP) for i in range(6)]
    W = cx[-1] + CW + X0
    L = Lienzo(W, 560)
    L.caja(X0, 14, W - 2*X0, 190, "RUTA ELEGIDA · ON-PREMISE · máquina virtual VirtualBox · Debian 12 · 4 vCPU · 6.7 GB RAM", fill="#f7f7f5", stroke="#9a9a94")
    L.caja(X0, 292, W - 2*X0, 190, None, dashed=True, fill="#fbfbfa", stroke="#52514e")
    L.texto(W/2, 470, "EQUIVALENTE EN LA NUBE · AWS us-east-1 · USD 0.22 al mes (estimado, no desplegado)", 19, anchor="middle", color="#1f3a5f", bold=True)
    nx = lambda i: cx[i] + (CW - NW)/2
    y1, y2 = 62, 340
    L.nodo("o", nx(0), y1, NW, NH, ["datosabiertos", "gob.pe (OSIPTEL)", "XLSX / CSV"], "fuente")
    L.nodo("sp", nx(1), y1, NW, NH, ["Apache Spark 3.5.1", "local · Python 3.11", "OpenJDK 17"], "proc")
    L.nodo("dl", nx(2), y1, NW, NH, ["Data lake en disco", "Parquet + Snappy", "RAW · SILVER · GOLD"], "dato")
    L.nodo("sq", nx(3), y1, NW, NH, ["Spark SQL y MLlib", "tablas an_*", "modelo"], "proc")
    L.nodo("gp", nx(4), y1, NW, NH, ["GitHub Pages", "tablero HTML", "(repo + Actions)"], "uso")
    L.nodo("u", nx(5), y1, NW, NH, ["Usuario: DFI", "OSIPTEL", "navegador web"], "fuente")
    L.nodo("gl", nx(1), y2, NW, NH, ["AWS Glue", "ETL Spark", "sin servidor"], "nube", dashed=True)
    L.nodo("s3", nx(2), y2, NW, NH, ["Amazon S3", "data lake", "+ Glue Data Catalog"], "nube", dashed=True)
    L.nodo("at", nx(3), y2, NW, NH, ["Amazon Athena", "consultas SQL", "sobre S3"], "nube", dashed=True)
    L.nodo("sw", nx(4), y2, NW, NH, ["Amazon S3", "sitio web estático", "(tablero)"], "nube", dashed=True)
    for a, b in [("o", "sp"), ("sp", "dl"), ("dl", "sq"), ("sq", "gp"), ("gp", "u")]:
        L.flecha(a, b)
    L.flecha("gl", "s3"); L.flecha("s3", "at"); L.flecha("at", "sw")
    for a, b in [("sp", "gl"), ("dl", "s3"), ("sq", "at"), ("gp", "sw")]:
        L.flecha(a, b, ("b", "t"), dashed=True, both=True)
    for i, t in enumerate(["Equivalencia:", "el mismo código;", "solo cambia", "OSIPTEL_LAKE_URI", "(disco → s3a://)"]):
        L.texto(cx[0] + CW/2, 332 + i*24, t, 18, anchor="middle", color="#1f3a5f", bold=(i == 0))
    L.texto(W/2, 530, "Costos y supuestos de la nube: §2.4 (AWS Pricing Calculator, Figura 3).", 16, anchor="middle")
    L.H = 548
    L.guardar("despliegue")


arquitectura(); despliegue()
