"""Variant B charts: matplotlib PNGs for the install->paid email.

iA-style: no gridlines, no chart borders, direct labels instead of legends. Colour
carries meaning only (same palette as ip_render_email). Rendered at 2x for retina on
an opaque card background so dark-mode clients cannot turn them into dark-on-dark.
"""
from __future__ import annotations

from pathlib import Path

import matplotlib
import matplotlib.font_manager

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

import ip_model as M  # noqa: E402
import ip_render_email as R  # noqa: E402

BG = "#fefefe"
INK, MUTED = "#222222", "#777777"
_HAVE = {f.name for f in matplotlib.font_manager.fontManager.ttflist}
plt.rcParams.update({
    "font.family": [f for f in ("Inter", "Helvetica Neue", "Helvetica") if f in _HAVE] + ["DejaVu Sans"],
    "font.size": 9, "axes.edgecolor": "#dddddd", "axes.labelcolor": MUTED,
    "xtick.color": MUTED, "ytick.color": INK, "figure.facecolor": BG, "axes.facecolor": BG,
})
W_IN, DPI = 5.2, 200


def _clean(ax):
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)
    ax.tick_params(axis="y", length=0)
    ax.tick_params(axis="x", length=3, color="#dddddd")


def _save(fig, out_dir, name):
    path = Path(out_dir) / name
    fig.savefig(path, dpi=DPI, facecolor=BG, bbox_inches="tight", pad_inches=0.12)
    plt.close(fig)
    return {"name": name, "path": str(path)}


def funnel_png(t, model, out_dir):
    """Dot plot per step: today's rate with its 95% interval, coloured by the verdict vs
    Mar 2026, and the two benchmarks as markers. Paid gets its own panel and scale."""
    low = [s for s in R.steps_for(t) if s in ("paid", "pc")]
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(W_IN, 2.1 + 0.4 * len(low)),
                                   gridspec_kw={"height_ratios": [3, 0.45 + 0.7 * len(low)], "hspace": 0.55})
    for ax, steps in ((ax1, ("pw", "pv", "ex")), (ax2, low)):
        ys = list(range(len(steps)))[::-1]
        for y, s in zip(ys, steps):
            p, lo, hi = t["rates"][s]
            col = R.verdict_color(t["cmp"]["mar26"][s], t["thin"])
            ax.plot([lo, hi], [y, y], color=col, lw=2.2, alpha=0.35, solid_capstyle="round", zorder=2)
            ax.scatter([p], [y], s=46, color=col, zorder=4)
            for bk, colr in (("aug25", R.C_AUG), ("mar26", R.C_MAR)):
                bv = R.bench_for(t, model, bk, s)      # None: no like-for-like figure, no mark
                if bv is not None:
                    ax.scatter([bv], [y], marker="|", s=170, lw=2.2, color=colr, zorder=3)
            ax.annotate(R.pct(p), (p, y), xytext=(0, 8), textcoords="offset points", ha="center",
                        fontsize=8.5, fontweight="bold", color=INK)
        ax.set_yticks(ys)
        ax.set_yticklabels([R.STEP_NAMES[s] for s in steps], fontsize=8.5)
        ax.set_ylim(-0.6, len(steps) - 0.3)
        _clean(ax)
    ax1.set_xlim(0, 1)
    ax1.xaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1, decimals=0))
    sc = R._scale_for([t["rates"][s][2] for s in low] +
                      [R.bench_for(t, model, bk, s) for s in low for bk in ("aug25", "mar26")])
    step = sc / 4 if round(sc / 4 * 1000) % 5 == 0 else sc / 5
    ax2.set_xlim(0, sc)
    ax2.xaxis.set_major_locator(matplotlib.ticker.MultipleLocator(step))
    ax2.xaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1, decimals=0 if (step * 100) % 1 == 0 else 1))
    ax2.set_title("Paid has its own scale" + (" · † in-app event, like for like with Aug 2025, directional"
                                              if "pc" in low else ""), fontsize=7.5, color=MUTED, loc="left", pad=3)
    fig.text(0.0, 0.97, "dot = today, line = its 95% range", fontsize=7.5, color=MUTED, transform=fig.transFigure)
    fig.text(0.42, 0.97, "| Aug 2025", fontsize=7.5, color=R.C_AUG, fontweight="bold", transform=fig.transFigure)
    fig.text(0.56, 0.97, "| Mar 2026", fontsize=7.5, color=R.C_MAR, fontweight="bold", transform=fig.transFigure)
    return _save(fig, out_dir, f"ip-{t['key']}-funnel.png")


def trend_png(t, model, out_dir):
    plat = M.plat_of(t["key"])
    pts = t["trend"]
    mar = model["bench"]["mar26"][plat]["rates"]["paid"][0]
    pooled = {b["v"] for b in t["builds"]}
    fig, ax = plt.subplots(figsize=(W_IN, 1.7))
    xs = list(range(len(pts)))
    for x, p in zip(xs, pts):
        r, lo, hi = p["rate"]
        col = R.ACCENT if p["v"] in pooled else "#b9c9d6"
        ax.bar([x], [r], width=0.6, color=col, zorder=2)
        ax.plot([x, x], [lo, hi], color=MUTED, lw=0.8, zorder=3)
        ax.annotate(R.pct(r), (x, hi), xytext=(0, 3), textcoords="offset points", ha="center", fontsize=7.5, color=INK)
    ax.axhline(mar, color=R.C_MAR, lw=1, ls=(0, (3, 2)), zorder=1)
    ax.set_xticks(xs)
    ax.set_xticklabels([f"{p['v']}\nn={p['n']:,}" for p in pts], fontsize=7, color=MUTED)
    ax.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1, decimals=0 if mar > 0.03 else 1))
    ax.set_ylim(0, max([p["rate"][2] for p in pts] + [mar]) * 1.18)
    ax.tick_params(axis="y", labelsize=7, colors=MUTED)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.set_title(f"Paid within a day, by build · dark = in today's pool · dashed = Mar 2026 ({R.pct(mar)})",
                 fontsize=8, color=MUTED, loc="left")
    return _save(fig, out_dir, f"ip-{t['key']}-trend.png")
