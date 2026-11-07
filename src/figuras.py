"""Figuras del EDA (informe §4.1), del modelo (§4.3) y maqueta del tablero (§4.4).

Lee SOLO tablas GOLD (datos limpios) y las métricas del modelo; nunca RAW ni las réplicas de benchmark.
    python src/figuras.py           -> escribe PNG en docs/figuras/
"""
import json
from pathlib import Path
import numpy as np, pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

FIG = Path(__file__).resolve().parents[1] / "docs" / "figuras"
OUT = None  # carpeta con modelo_metricas.json y predicciones (docs/evidencia)
COL = {"BITEL": "#2a78d6", "CLARO": "#eb6834", "ENTEL": "#1baf7a", "MOVISTAR": "#eda100"}
OPS = list(COL)
INK, INK2, GRID, SURF = "#0b0b0b", "#52514e", "#e4e3df", "#ffffff"
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10, "axes.edgecolor": GRID, "axes.labelcolor": INK2,
                     "xtick.color": INK2, "ytick.color": INK2, "axes.grid": True, "grid.color": GRID, "grid.linewidth": .6,
                     "axes.spines.top": False, "axes.spines.right": False, "axes.titlesize": 12, "axes.titleweight": "bold",
                     "axes.titlecolor": INK, "axes.titlelocation": "left", "figure.facecolor": SURF, "axes.facecolor": SURF,
                     "lines.linewidth": 2, "savefig.dpi": 200})
miles = matplotlib.ticker.FuncFormatter(lambda x, _: f"{x/1000:,.0f} k")


def guardar(fig, n):
    fig.tight_layout()
    fig.savefig(FIG / n, bbox_inches="tight")
    plt.close(fig)


def etiquetas_finales(ax, series, fmt, dx=20):
    """Etiqueta directa al final de cada línea, separando colisiones verticales."""
    pts = sorted([(s.iloc[-1], op, s.index[-1]) for op, s in series.items()])
    ymin, ymax = ax.get_ylim(); gap = (ymax - ymin) * 0.06; last = -1e18
    for y, op, x in pts:
        y2 = max(y, last + gap); last = y2
        ax.annotate(f"{op} {fmt(y)}", (x, y), xytext=(x + pd.Timedelta(days=dx), y2), color=INK, fontsize=9, va="center")


def main(g, j, out_dir, fig_dir=None):
    """g = gold_operadora_mes, j = gold_depto_operadora_202303 (pandas); out_dir = carpeta de evidencia."""
    global OUT, FIG
    OUT, FIG = Path(out_dir), Path(fig_dir) if fig_dir else FIG
    FIG.mkdir(parents=True, exist_ok=True)
    g = g.copy(); g["fecha_mes"] = pd.to_datetime(g["fecha_mes"])
    g4 = g[g.operadora.isin(OPS)]
    # ---- Fig 1: serie total de sustraídos
    tot = g[g.sustraidos.notna()].groupby("fecha_mes").sustraidos.sum()
    fig, ax = plt.subplots(figsize=(9, 3.6))
    ax.plot(tot.index, tot.values, color=COL["BITEL"])
    ax.axvspan(pd.Timestamp("2020-03-15"), pd.Timestamp("2020-07-01"), color="#e4e3df", alpha=.6, lw=0)
    ax.annotate("Inmovilización\nsocial obligatoria\n(abr-2020: %s)" % f"{tot['2020-04-01']:,.0f}".replace(",", " "),
                (pd.Timestamp("2020-04-01"), tot["2020-04-01"]), xytext=(pd.Timestamp("2020-09-01"), 8000), fontsize=8.5, color=INK2,
                arrowprops=dict(arrowstyle="-", color=INK2, lw=.8))
    m25 = tot["2025"].mean()
    ax.axhline(m25, color=INK2, lw=.8, ls="--")
    ax.text(pd.Timestamp("2017-02-01"), m25 + 4000, f"Promedio 2025: {m25:,.0f} / mes".replace(",", " "), fontsize=8.5, color=INK2)
    ax.yaxis.set_major_formatter(miles); ax.set_ylim(0, 230000)
    ax.set_title("Equipos móviles reportados como sustraídos por mes (todas las operadoras)")
    ax.set_ylabel("Equipos / mes")
    guardar(fig, "fig1_sustraidos_total.png")

    # ---- Fig 2: sustraídos por operadora
    fig, ax = plt.subplots(figsize=(9, 3.8))
    series = {}
    for op in OPS:
        s = g4[g4.operadora == op].set_index("fecha_mes").sustraidos.dropna().rolling(3).mean().dropna()
        ax.plot(s.index, s.values, color=COL[op], lw=1.8); series[op] = s
    ax.yaxis.set_major_formatter(miles); ax.set_xlim(pd.Timestamp("2017-01-01"), pd.Timestamp("2027-03-01"))
    etiquetas_finales(ax, series, lambda v: f"{v/1000:.0f} k")
    ax.set_title("Sustraídos por operadora (media móvil de 3 meses)"); ax.set_ylabel("Equipos / mes")
    guardar(fig, "fig2_sustraidos_operadora.png")

    # ---- Fig 3: tasa de recuperación anual por operadora
    a = g4[g4.fecha_mes < "2026-01-01"].assign(anio=lambda d: d.fecha_mes.dt.year).groupby(["anio", "operadora"])[["recuperados", "sustraidos"]].sum()
    a["tasa"] = 100 * a.recuperados / a.sustraidos
    fig, ax = plt.subplots(figsize=(9, 3.8))
    series = {}
    for op in OPS:
        s = a.xs(op, level="operadora").tasa
        s.index = pd.to_datetime(s.index.astype(str) + "-07-01")
        ax.plot(s.index, s.values, color=COL[op], marker="o", ms=4); series[op] = s
    ax.set_ylim(0, 16); ax.set_xlim(pd.Timestamp("2016-10-01"), pd.Timestamp("2026-12-01"))
    ax.xaxis.set_major_locator(mdates.YearLocator()); ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    etiquetas_finales(ax, series, lambda v: f"{v:.1f} %", dx=60)
    ax.set_title("Recuperados por cada 100 sustraídos, por operadora y año"); ax.set_ylabel("%")
    guardar(fig, "fig3_tasa_recuperacion.png")

    # ---- Fig 4: sustraídos vs reclamos móviles (índice 2017 = 100, un solo eje)
    b = g4[g4.origen_join.isin(["both", "ambas"])].groupby("fecha_mes")[["sustraidos", "reclamos_moviles"]].sum()
    b = b.rolling(3).mean().dropna(); idx = 100 * b / b.loc["2017-03-01":"2017-12-01"].mean()
    fig, ax = plt.subplots(figsize=(9, 3.6))
    ax.plot(idx.index, idx.sustraidos, color=COL["BITEL"], label="Equipos sustraídos")
    ax.plot(idx.index, idx.reclamos_moviles, color=COL["CLARO"], label="Reclamos de servicio móvil")
    ax.axhline(100, color=INK2, lw=.8)
    ax.legend(frameon=False, loc="upper right")
    ax.set_title("Sustracciones y reclamos móviles, índice (2017 = 100), media móvil 3 meses"); ax.set_ylabel("Índice")
    guardar(fig, "fig4_robos_vs_reclamos.png")

    # ---- Fig 5: cruce territorial cobertura 4G vs reclamos de calidad (mar-2023)
    d = j.groupby("departamento")[["ccpp", "ccpp_4g", "reclamos_moviles", "reclamos_calidad"]].sum()
    d["pct4g"] = 100 * d.ccpp_4g / d.ccpp; d["pct_cal"] = 100 * d.reclamos_calidad / d.reclamos_moviles
    fig, ax = plt.subplots(figsize=(9, 4))
    ax.scatter(d.pct4g, d.pct_cal, s=36, color=COL["BITEL"], edgecolor="white", lw=1.5, zorder=3)
    for n in ["LIMA Y CALLAO", "AMAZONAS", "LORETO", "MADRE DE DIOS", "ANCASH", "ICA", "HUANCAVELICA", "LA LIBERTAD"]:
        ax.annotate(n.title(), (d.pct4g[n], d.pct_cal[n]), xytext=(5, 4), textcoords="offset points", fontsize=8.5, color=INK)
    rho = d[["pct4g", "pct_cal"]].corr(method="spearman").iloc[0, 1]
    ax.set_xlabel("% de centros poblados con registro que tienen 4G"); ax.set_ylabel("% de reclamos móviles por calidad o falta de servicio")
    ax.set_title(f"Cobertura 4G vs reclamos de calidad por departamento, marzo 2023 (Spearman ρ = {rho:.2f})")
    guardar(fig, "fig5_cobertura_vs_reclamos.png")
    d.round(2).to_csv(OUT / "tabla_departamentos_202303.csv")

    # ---- Fig 6: modelo, prueba H=1
    p = pd.read_csv(OUT / "predicciones_prueba_H1.csv", parse_dates=["fecha_mes"])
    fig, axs = plt.subplots(2, 2, figsize=(9, 5), sharex=True)
    for ax, op in zip(axs.flat, OPS):
        x = p[p.operadora == op]
        ax.plot(x.fecha_mes, x.sustraidos, color=INK, lw=1.6, label="Observado")
        ax.plot(x.fecha_mes, x.gradient_boosting, color=COL[op], lw=1.8, label="Gradient Boosting")
        ax.plot(x.fecha_mes, x.ingenuo, color=INK2, lw=1.2, ls="--", label="Ingenuo (mes anterior)")
        ax.set_title(op, fontsize=10); ax.yaxis.set_major_formatter(miles)
        ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[1, 7])); ax.xaxis.set_major_formatter(mdates.DateFormatter("%b-%y"))
    h, l = axs[0, 0].get_legend_handles_labels()
    fig.legend(h, l, frameon=False, fontsize=8.5, loc="upper right", ncol=3, bbox_to_anchor=(1, 1.0))
    fig.suptitle("Pronóstico a 1 mes en el periodo de prueba (jul-2024 a ene-2026)", x=0.01, y=1.06, ha="left", fontweight="bold", fontsize=12)
    guardar(fig, "fig6_modelo_prueba.png")

    # ---- Fig 7: tablero (capa de consumo) — maqueta generada desde GOLD
    ult = g4.fecha_mes.max(); u12 = g4[g4.fecha_mes > ult - pd.DateOffset(months=12)]
    k_sus = u12.sustraidos.sum() / 12; k_rec = 100 * u12.recuperados.sum() / u12.sustraidos.sum()
    prev = g4[(g4.fecha_mes <= ult - pd.DateOffset(months=12)) & (g4.fecha_mes > ult - pd.DateOffset(months=24))]
    var = 100 * (u12.sustraidos.sum() / prev.sustraidos.sum() - 1)
    met = json.load(open(OUT / "modelo_metricas.json", encoding="utf-8"))["H1"]["metricas"]["gradient_boosting"]
    fig = plt.figure(figsize=(10, 5.6)); fig.patch.set_facecolor("#f4f3ef")
    fig.text(.02, .95, f"Tablero RENTESEG · Sustracción y recuperación de equipos · corte {ult:%m/%Y}", fontsize=13, fontweight="bold", color=INK)
    tiles = [("Sustraídos / mes (últ. 12 m)", f"{k_sus:,.0f}".replace(",", " ")),
             ("Variación interanual", f"{var:+.1f} %"),
             ("Recuperados por 100 sustraídos", f"{k_rec:.1f}"),
             ("Error del pronóstico (MAPE, 1 mes)", f"{met['MAPE']:.1f} %")]
    for i, (t, v) in enumerate(tiles):
        axk = fig.add_axes([.02 + i * .245, .70, .23, .2]); axk.set_facecolor("white"); axk.set_xticks([]); axk.set_yticks([]); axk.grid(False)
        for s in axk.spines.values(): s.set_visible(False)
        axk.text(.06, .68, t, fontsize=9, color=INK2, transform=axk.transAxes)
        axk.text(.06, .22, v, fontsize=20, fontweight="bold", color=INK, transform=axk.transAxes)
    ax1 = fig.add_axes([.06, .08, .42, .52]); ax2 = fig.add_axes([.56, .08, .42, .52])
    for op in OPS:
        s = g4[(g4.operadora == op) & (g4.fecha_mes >= "2024-01-01")].set_index("fecha_mes").sustraidos
        ax1.plot(s.index, s.values, color=COL[op], lw=1.6)
        ax1.annotate(op, (s.index[-1], s.iloc[-1]), xytext=(4, {"MOVISTAR": 5, "BITEL": -5}.get(op, 0)), textcoords="offset points", fontsize=8, color=INK, va="center")
    ax1.yaxis.set_major_formatter(miles); ax1.set_title("Sustraídos por operadora", fontsize=10)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%m-%y")); ax1.set_xlim(right=ult + pd.Timedelta(days=75))
    r = u12.groupby("operadora")[["recuperados", "sustraidos"]].sum(); r = (100 * r.recuperados / r.sustraidos).sort_values()
    bars = ax2.barh(r.index, r.values, color=[COL[o] for o in r.index], height=.55)
    ax2.bar_label(bars, labels=[f"{v:.1f}" for v in r.values], padding=4, fontsize=9, color=INK)
    ax2.axvline(k_rec, color=INK2, lw=.8, ls="--"); ax2.set_title("Recuperados por 100 sustraídos (últ. 12 m)", fontsize=10)
    ax2.set_xlim(0, max(r) * 1.25); ax2.grid(axis="y", visible=False)
    fig.savefig(FIG / "fig7_tablero.png", bbox_inches="tight"); plt.close(fig)
    json.dump(dict(corte=str(ult.date()), sustraidos_mes_u12=k_sus, var_interanual=var, recuperacion_u12=k_rec,
                   recuperacion_u12_operadora=r.round(2).to_dict()), open(OUT / "kpis_tablero.json", "w"), indent=2)


if __name__ == "__main__":
    import os, sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from config import ruta, EVIDENCIA
    from pipeline import spark_session
    spark = spark_session("OSIPTEL_figuras")
    main(spark.read.parquet(ruta("gold", "gold_operadora_mes")).toPandas(),
         spark.read.parquet(ruta("gold", "gold_depto_operadora_202303")).toPandas(), EVIDENCIA)
