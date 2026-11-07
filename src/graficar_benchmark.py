"""Figuras de rendimiento para el informe (§3.6 y §3.7), a partir de docs/evidencia/.

    python src/graficar_benchmark.py
Genera docs/figuras/fig9_optimizacion.png y docs/figuras/fig10_escalabilidad.png.
"""
import csv, json
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

RAIZ = Path(__file__).resolve().parents[1]
EVID, FIG = RAIZ / "docs" / "evidencia", RAIZ / "docs" / "figuras"
AZUL, NARANJA, GRIS, ROJO, TINTA = "#2a78d6", "#eb6834", "#52514e", "#d03b3b", "#0b0b0b"
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.grid": True, "grid.color": "#e4e3df", "axes.edgecolor": "#cfcfca", "axes.titlelocation": "left",
                     "axes.titleweight": "bold", "savefig.dpi": 200})


def fig_optimizacion():
    p = EVID / "optimizacion.json"
    if not p.exists():
        print("Falta optimizacion.json: ejecute src/benchmark.py --optimizacion"); return
    o = json.loads(p.read_text(encoding="utf-8"))
    pares = [("A · Construcción de GOLD", o["A_antes_sin_cache_ni_broadcast"], o["A_despues_con_cache_y_broadcast"], "sin caché ni broadcast", "con caché y broadcast", o["A_mejora_pct"]),
             ("B · Consulta de un año", o["B_antes_sin_particion"], o["B_despues_particionado_anio"], "sin partición", "particionada por año", o["B_mejora_pct"])]
    fig, axs = plt.subplots(1, 2, figsize=(9, 3.4))
    for ax, (tit, a, d, la, ld, mej) in zip(axs, pares):
        vals, errs = [a["promedio_s"], d["promedio_s"]], [a["desv_estandar_s"], d["desv_estandar_s"]]
        bars = ax.bar([la, ld], vals, yerr=errs, color=[GRIS, AZUL], width=0.55, capsize=4, error_kw={"elinewidth": 1})
        ax.bar_label(bars, labels=[f"{v:.2f} s" for v in vals], padding=6, fontsize=9)
        ax.set_title(f"{tit}: −{mej:.0f} %", fontsize=10.5); ax.set_ylabel("Segundos (promedio ± desv. est.)")
        ax.set_ylim(0, max(v + e for v, e in zip(vals, errs)) * 1.3); ax.grid(axis="x", visible=False)
    fig.suptitle(f"Optimización medida: {o['repeticiones']} repeticiones por variante, AQE desactivado", x=0.01, ha="left", fontweight="bold")
    fig.tight_layout(); fig.savefig(FIG / "fig9_optimizacion.png", bbox_inches="tight"); plt.close(fig)
    print("OK fig9_optimizacion.png")


def fig_escalabilidad():
    p = EVID / "escalabilidad.json"
    if not p.exists():
        print("Falta escalabilidad.json: ejecute src/benchmark.py --escalabilidad"); return
    e = json.loads(p.read_text(encoding="utf-8"))
    fs = e["factores"]; ok = [f for f in fs if f["estado"] == "completado"]; mal = [f for f in fs if f["estado"] != "completado"]
    ram_total = e["maquina"].get("ram_total_gb", 0) * 1000
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(9.5, 3.6))
    if ok:
        x = [f["factor"] for f in ok]; t = [f["tiempo_total_s"] for f in ok]
        a1.plot(x, t, "-o", color=AZUL, label="Tiempo medido")
        a1.plot(x, [t[0] * xi / x[0] for xi in x], "--", color=GRIS, lw=1, label="Crecimiento proporcional")
        for xi, ti in zip(x, t):
            a1.annotate(f"{ti:,.0f} s".replace(",", " "), (xi, ti), xytext=(5, -12), textcoords="offset points", fontsize=8.5)
    a1.set_xscale("log"); a1.set_yscale("log")
    for f in mal:
        a1.axvline(f["factor"], color=ROJO, lw=1, ls=":")
        a1.annotate(f"{f['factor']}×: falla", (f["factor"], a1.get_ylim()[0] * 1.3), rotation=90, fontsize=8.5, color=ROJO, ha="right", va="bottom")
    a1.set_xlabel("Factor de réplica (escala log)"); a1.set_ylabel("Segundos (escala log)")
    a1.set_title("Tiempo del pipeline completo"); a1.legend(frameon=False, fontsize=8.5)
    x2 = [f["factor"] for f in fs]; r2 = [f["ram_sistema_pico_mb"] or 0 for f in fs]; s2 = [f["swap_pico_mb"] or 0 for f in fs]
    cols = [AZUL if f["estado"] == "completado" else ROJO for f in fs]
    pos = range(len(x2))
    a2.bar(pos, r2, color=cols, width=0.55, label="RAM pico del sistema")
    a2.bar(pos, s2, bottom=r2, color=NARANJA, width=0.55, label="Swap pico")
    if ram_total:
        a2.axhline(ram_total, color=TINTA, lw=1, ls="--")
        a2.text(-0.45, ram_total * 1.03, f"RAM total {ram_total/1000:.1f} GB", fontsize=8.5, ha="left")
    a2.set_ylim(0, max([ram_total] + [r + s for r, s in zip(r2, s2)]) * 1.35)
    a2.set_xticks(list(pos), [f"{x}×" for x in x2]); a2.set_ylabel("MB"); a2.set_title("Memoria máxima por factor (rojo = falla)")
    a2.legend(frameon=False, fontsize=8.5, loc="upper left", ncol=2); a2.grid(axis="x", visible=False); a2.set_axisbelow(True)
    fig.tight_layout(); fig.savefig(FIG / "fig10_escalabilidad.png", bbox_inches="tight"); plt.close(fig)
    print("OK fig10_escalabilidad.png")


if __name__ == "__main__":
    FIG.mkdir(parents=True, exist_ok=True)
    fig_optimizacion(); fig_escalabilidad()
