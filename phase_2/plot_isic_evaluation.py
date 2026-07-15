"""
Generate publication-quality figures from the ISIC evaluation CSV reports.

Reads only the CSVs already produced by evaluate_isic_results.py and, where
present, compare_models.py / the adaptive-concurrency history written by
run_isic_classification.py. Never opens the database and never calls an API —
every number plotted here already exists on disk.

Inputs (reports/):
    isic_evaluation_summary.csv       required
    isic_division_distribution.csv    required
    isic_confidence_distribution.csv  required
    isic_model_statistics.csv         required (read for context; not directly plotted)
    model_comparison_summary.csv      optional
    isic_concurrency_history.csv      optional

Outputs (reports/figures/, each as .png at 300+ DPI and .svg):
    isic_top20_divisions
    isic_confidence_distribution
    classification_coverage
    model_agreement
    concurrency_throughput            only if enough concurrency history exists

Usage:
    python phase_2/plot_isic_evaluation.py [--reports-dir reports]
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.ticker import FuncFormatter, MaxNLocator

REPORTS_DIR_DEFAULT = "reports"
FIGURES_SUBDIR = "figures"
DPI = 300
MIN_CONCURRENCY_ROWS = 2

ACCEPTED_METHODS = ["openai:gpt-4o-mini", "openai:gpt-4.1-mini"]
METHOD_LABELS = {"openai:gpt-4o-mini": "GPT-4o-mini", "openai:gpt-4.1-mini": "GPT-4.1-mini"}

# --- Palette: fixed categorical roles, one hue per entity, never cycled. ---
SURFACE = "#fcfcfb"
INK_PRIMARY = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRIDLINE = "#e1e0d9"
BASELINE = "#c3c2b7"

BLUE = "#2a78d6"    # GPT-4o-mini identity, everywhere it appears
AQUA = "#1baf7a"    # GPT-4.1-mini identity, everywhere it appears
VIOLET = "#4a3aa7"  # "classified by both models" overlap
GREEN = "#008300"   # cross-model agreement
RED = "#e34948"     # cross-model disagreement / errors
GREY = INK_MUTED    # remaining / not-yet-classified (an absence, not a hue)

SEQUENTIAL_BLUE = LinearSegmentedColormap.from_list("seq_blue", ["#cde2fb", "#0d366b"])


plt.rcParams.update({
    "figure.facecolor": SURFACE,
    "axes.facecolor": SURFACE,
    "savefig.facecolor": SURFACE,
    "text.color": INK_PRIMARY,
    "axes.edgecolor": BASELINE,
    "axes.labelcolor": INK_SECONDARY,
    "xtick.color": INK_SECONDARY,
    "ytick.color": INK_SECONDARY,
    "font.size": 11,
    "font.family": ["Georgia", "DejaVu Serif", "serif"],
    "axes.titlesize": 14,
    "axes.titleweight": "bold",
    "axes.grid": False,
})


# ---------------------------------------------------------------------------
# CSV loading (read-only; missing optional files are handled by the caller)
# ---------------------------------------------------------------------------

def _read_rows(path: Path) -> list[dict] | None:
    if not path.exists():
        return None
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _read_metric_value_csv(path: Path) -> dict[str, str] | None:
    """Read a `metric,value` CSV, stopping at the first blank line so files
    like model_comparison_summary.csv (which append an unrelated table after
    a blank line) don't get misread."""
    if not path.exists():
        return None
    metrics: dict[str, str] = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header != ["metric", "value"]:
            return None
        for row in reader:
            if not row or not row[0]:
                break
            metrics[row[0]] = row[1]
    return metrics


def _as_int(value: str) -> int:
    return int(float(value.replace(",", "")))


def _as_float(value: str) -> float:
    return float(value.replace(",", ""))


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _save_figure(fig, figures_dir: Path, name: str) -> tuple[Path, Path]:
    figures_dir.mkdir(parents=True, exist_ok=True)
    png_path = figures_dir / f"{name}.png"
    svg_path = figures_dir / f"{name}.svg"
    fig.savefig(png_path, dpi=DPI, bbox_inches="tight", facecolor=SURFACE)
    fig.savefig(svg_path, bbox_inches="tight", facecolor=SURFACE)
    plt.close(fig)
    return png_path, svg_path


def _style_axes(ax, y_grid: bool = True) -> None:
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color(BASELINE)
    if y_grid:
        ax.yaxis.grid(True, color=GRIDLINE, linewidth=1, zorder=0)
        ax.set_axisbelow(True)


def _pct_formatter():
    return FuncFormatter(lambda v, _pos: f"{v:.0f}%")


def _truncate(text: str, max_len: int) -> str:
    return text if len(text) <= max_len else text[: max_len - 1].rstrip() + "…"


def _label_luminance_ink(hex_color: str) -> str:
    """White text on a dark fill, dark ink on a light fill."""
    r, g, b = (int(hex_color.lstrip("#")[i:i + 2], 16) for i in (0, 2, 4))
    luminance = 0.2126 * r + 0.7152 * g + 0.0722 * b
    return "#ffffff" if luminance < 140 else INK_PRIMARY


# ---------------------------------------------------------------------------
# Figure 1: Top 20 ISIC divisions
# ---------------------------------------------------------------------------

def build_top20_divisions(reports_dir: Path, figures_dir: Path) -> dict:
    name = "isic_top20_divisions"
    rows = _read_rows(reports_dir / "isic_division_distribution.csv")
    if not rows:
        return {"name": name, "status": "skipped", "reason": "isic_division_distribution.csv not found"}

    top20 = sorted(rows, key=lambda r: int(r["rank"]))[:20]
    if not top20 or all(int(r["count"]) == 0 for r in top20):
        return {"name": name, "status": "skipped", "reason": "no classified divisions to plot"}

    # Largest at top: reverse so rank 1 is drawn last (top of a horizontal bar chart).
    plotted = list(reversed(top20))
    labels = [f"{r['code']} — {_truncate(r['title'], 42)}" for r in plotted]
    counts = [int(r["count"]) for r in plotted]
    pcts = [float(r["percentage"]) for r in plotted]

    max_count = max(counts) or 1
    colors = [SEQUENTIAL_BLUE(0.25 + 0.75 * (c / max_count)) for c in counts]

    fig, ax = plt.subplots(figsize=(10, 9))
    bars = ax.barh(labels, counts, color=colors, height=0.68, zorder=3)
    _style_axes(ax, y_grid=False)
    ax.xaxis.grid(True, color=GRIDLINE, linewidth=1, zorder=0)
    ax.set_axisbelow(True)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _pos: f"{int(v):,}"))
    ax.set_xlabel("Classified projects")
    ax.set_title(
        "Accepted production models combined (GPT-4o-mini + GPT-4.1-mini)",
        fontsize=9.5, fontweight="normal", color=INK_MUTED, pad=12, loc="left",
    )
    fig.suptitle("Top 20 ISIC Divisions by Classified Project Count", fontsize=14, fontweight="bold", y=1.0)

    max_x = max(counts)
    for bar, count, pct in zip(bars, counts, pcts):
        ax.text(
            bar.get_width() + max_x * 0.012, bar.get_y() + bar.get_height() / 2,
            f"{count:,} ({pct:g}%)", va="center", ha="left", fontsize=9, color=INK_SECONDARY,
        )
    ax.set_xlim(0, max_x * 1.18)

    png_path, svg_path = _save_figure(fig, figures_dir, name)
    return {"name": name, "status": "created", "png": png_path, "svg": svg_path}


# ---------------------------------------------------------------------------
# Figure 2: Confidence distribution
# ---------------------------------------------------------------------------

def build_confidence_distribution(reports_dir: Path, figures_dir: Path) -> dict:
    name = "isic_confidence_distribution"
    rows = _read_rows(reports_dir / "isic_confidence_distribution.csv")
    if not rows:
        return {"name": name, "status": "skipped", "reason": "isic_confidence_distribution.csv not found"}

    by_method: dict[str, dict[str, float]] = {m: {} for m in ACCEPTED_METHODS}
    buckets: list[str] = []
    for row in rows:
        method = row["method"]
        if method not in by_method:
            continue
        if row["bucket"] not in buckets:
            buckets.append(row["bucket"])
        by_method[method][row["bucket"]] = float(row["percentage"])

    if not buckets or not any(by_method[m] for m in ACCEPTED_METHODS):
        return {"name": name, "status": "skipped", "reason": "no per-method confidence buckets found"}

    n = len(buckets)
    x = list(range(n))
    width = 0.38
    colors = {"openai:gpt-4o-mini": BLUE, "openai:gpt-4.1-mini": AQUA}

    fig, ax = plt.subplots(figsize=(10, 5.5))
    for i, method in enumerate(ACCEPTED_METHODS):
        offset = (i - 0.5) * width
        values = [by_method[method].get(b, 0.0) for b in buckets]
        bars = ax.bar(
            [xi + offset for xi in x], values, width=width * 0.94,
            color=colors[method], label=METHOD_LABELS[method], zorder=3,
        )
        if values and max(values) > 0:
            peak_idx = max(range(n), key=lambda j: values[j])
            ax.text(
                peak_idx + offset, values[peak_idx] + 1.5, f"{values[peak_idx]:.0f}%",
                ha="center", va="bottom", fontsize=9, color=INK_SECONDARY,
            )

    _style_axes(ax)
    ax.set_xticks(x)
    ax.set_xticklabels(buckets, rotation=40, ha="right")
    ax.yaxis.set_major_formatter(_pct_formatter())
    ax.set_ylabel("Share of classifications")
    ax.set_xlabel("Confidence bucket")
    ax.set_title("Confidence Score Distribution by Model")
    ax.legend(frameon=False, loc="upper left")

    png_path, svg_path = _save_figure(fig, figures_dir, name)
    return {"name": name, "status": "created", "png": png_path, "svg": svg_path}


# ---------------------------------------------------------------------------
# Figure 3: Classification coverage
# ---------------------------------------------------------------------------

def build_classification_coverage(reports_dir: Path, figures_dir: Path) -> dict:
    name = "classification_coverage"
    metrics = _read_metric_value_csv(reports_dir / "isic_evaluation_summary.csv")
    if not metrics:
        return {"name": name, "status": "skipped", "reason": "isic_evaluation_summary.csv not found"}

    required = [
        "total_project_inputs", "classified_by_openai:gpt-4o-mini",
        "classified_by_openai:gpt-4.1-mini", "projects_classified_by_both_models",
        "remaining_unclassified",
    ]
    if any(k not in metrics for k in required):
        return {"name": name, "status": "skipped", "reason": "required coverage metrics missing from summary"}

    total = _as_int(metrics["total_project_inputs"])
    gpt4o = _as_int(metrics["classified_by_openai:gpt-4o-mini"])
    gpt41 = _as_int(metrics["classified_by_openai:gpt-4.1-mini"])
    both = _as_int(metrics["projects_classified_by_both_models"])
    remaining = _as_int(metrics["remaining_unclassified"])
    if total <= 0:
        return {"name": name, "status": "skipped", "reason": "total_project_inputs is zero"}

    segments = [
        ("GPT-4o-mini only", gpt4o - both, BLUE),
        ("GPT-4.1-mini only", gpt41 - both, AQUA),
        ("Both models", both, VIOLET),
        ("Remaining unclassified", remaining, GREY),
    ]

    fig, ax = plt.subplots(figsize=(10, 2.6))
    left = 0.0
    for label, count, color in segments:
        pct = count / total * 100
        ax.barh(
            [0], [pct], left=left, height=0.5, color=color, zorder=3,
            edgecolor=SURFACE, linewidth=2,
        )
        if pct >= 5:
            ax.text(
                left + pct / 2, 0, f"{pct:.1f}%", ha="center", va="center",
                fontsize=10.5, color=_label_luminance_ink(color), fontweight="bold",
            )
        left += pct

    ax.set_xlim(0, 100)
    ax.set_ylim(-0.6, 0.6)
    ax.set_yticks([])
    ax.set_xlabel("Percentage of PROJECT inputs")
    ax.xaxis.set_major_formatter(_pct_formatter())
    ax.set_title(
        f"n = {total:,} PROJECT inputs · accepted production models",
        fontsize=9.5, fontweight="normal", color=INK_MUTED, pad=12, loc="left",
    )
    fig.suptitle("PROJECT Classification Coverage", fontsize=14, fontweight="bold", y=1.12)
    for spine in ("top", "right", "left"):
        ax.spines[spine].set_visible(False)
    ax.spines["bottom"].set_color(BASELINE)

    handles = [
        plt.Rectangle((0, 0), 1, 1, color=color) for _, _, color in segments
    ]
    legend_labels = [f"{label} ({count:,})" for label, count, _ in segments]
    ax.legend(
        handles, legend_labels, loc="upper center", bbox_to_anchor=(0.5, -0.55),
        ncol=2, frameon=False, fontsize=9.5,
    )

    png_path, svg_path = _save_figure(fig, figures_dir, name)
    return {"name": name, "status": "created", "png": png_path, "svg": svg_path}


# ---------------------------------------------------------------------------
# Figure 4: Cross-model agreement
# ---------------------------------------------------------------------------

def build_model_agreement(reports_dir: Path, figures_dir: Path) -> dict:
    name = "model_agreement"
    metrics = _read_metric_value_csv(reports_dir / "isic_evaluation_summary.csv")
    if not metrics:
        return {"name": name, "status": "skipped", "reason": "isic_evaluation_summary.csv not found"}

    required = ["projects_classified_by_both_models", "agreement_count", "agreement_percent", "disagreement_count"]
    if any(k not in metrics for k in required):
        return {"name": name, "status": "skipped", "reason": "required agreement metrics missing from summary"}

    groups = []
    both_n = _as_int(metrics["projects_classified_by_both_models"])
    if both_n > 0:
        agree_pct = _as_float(metrics["agreement_percent"])
        groups.append({
            "label": f"Production overlap\n(n={both_n:,})",
            "agree_pct": agree_pct,
            "agree_n": _as_int(metrics["agreement_count"]),
            "disagree_n": _as_int(metrics["disagreement_count"]),
        })

    comparison = _read_metric_value_csv(reports_dir / "model_comparison_summary.csv")
    if comparison and "both_models_available" in comparison and "exact_code_agreement_percent" in comparison:
        sample_n = _as_int(comparison["both_models_available"])
        if sample_n > 0:
            groups.append({
                "label": f"Comparison sample\n(n={sample_n:,})",
                "agree_pct": _as_float(comparison["exact_code_agreement_percent"]),
                "agree_n": _as_int(comparison["exact_code_agreement_count"]),
                "disagree_n": _as_int(comparison.get("disagreement_count", "0")),
            })

    if not groups:
        return {"name": name, "status": "skipped", "reason": "no cross-model comparison with overlap > 0"}

    fig, ax = plt.subplots(figsize=(6.5 + 2 * (len(groups) - 1), 5.5))
    x = list(range(len(groups)))
    bar_width = 0.5

    for xi, g in enumerate(groups):
        disagree_pct = 100 - g["agree_pct"]
        ax.bar(xi, g["agree_pct"], width=bar_width, color=GREEN, zorder=3, edgecolor=SURFACE, linewidth=2)
        ax.bar(
            xi, disagree_pct, width=bar_width, bottom=g["agree_pct"], color=RED, zorder=3,
            edgecolor=SURFACE, linewidth=2,
        )
        ax.text(
            xi, g["agree_pct"] / 2, f"{g['agree_pct']:.1f}%\n({g['agree_n']:,})",
            ha="center", va="center", fontsize=10, color=_label_luminance_ink(GREEN), fontweight="bold",
        )
        if disagree_pct >= 8:
            ax.text(
                xi, g["agree_pct"] + disagree_pct / 2, f"{disagree_pct:.1f}%\n({g['disagree_n']:,})",
                ha="center", va="center", fontsize=10, color=_label_luminance_ink(RED), fontweight="bold",
            )

    ax.set_xticks(x)
    ax.set_xticklabels([g["label"] for g in groups])
    ax.set_xlim(-0.6, len(groups) - 0.4)
    ax.set_ylim(0, 100)
    ax.yaxis.set_major_formatter(_pct_formatter())
    ax.set_ylabel("Share of overlapping classifications")
    ax.set_title("Cross-Model Agreement: GPT-4o-mini vs GPT-4.1-mini")
    _style_axes(ax)
    ax.set_axisbelow(True)

    handles = [plt.Rectangle((0, 0), 1, 1, color=GREEN), plt.Rectangle((0, 0), 1, 1, color=RED)]
    ax.legend(handles, ["Agreement", "Disagreement"], frameon=False, loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=2)

    png_path, svg_path = _save_figure(fig, figures_dir, name)
    return {"name": name, "status": "created", "png": png_path, "svg": svg_path}


# ---------------------------------------------------------------------------
# Figure 5: Adaptive concurrency vs throughput (optional)
# ---------------------------------------------------------------------------

def build_concurrency_throughput(reports_dir: Path, figures_dir: Path) -> dict:
    name = "concurrency_throughput"
    rows = _read_rows(reports_dir / "isic_concurrency_history.csv")
    if not rows:
        return {"name": name, "status": "skipped", "reason": "isic_concurrency_history.csv not found"}
    if len(rows) < MIN_CONCURRENCY_ROWS:
        return {
            "name": name, "status": "skipped",
            "reason": f"only {len(rows)} concurrency window(s) recorded (need >= {MIN_CONCURRENCY_ROWS})",
        }

    # Windows are numbered per run_id; runs are appended over time, so replay
    # them in file order and use a running index for the x-axis.
    windows = list(range(1, len(rows) + 1))
    concurrency = [int(r["new_concurrency"]) for r in rows]
    completed = [int(r["completed"]) for r in rows]
    errors = [int(r["errors"]) for r in rows]
    inserted_ok = [c - e for c, e in zip(completed, errors)]
    run_ids = [r["run_id"] for r in rows]
    multiple_runs = len(set(run_ids)) > 1

    fig, (ax_top, ax_bottom) = plt.subplots(
        2, 1, figsize=(9.5, 7), sharex=True, gridspec_kw={"height_ratios": [1, 1.3], "hspace": 0.12},
    )

    ax_top.step(windows, concurrency, where="post", color=VIOLET, linewidth=2, zorder=3)
    ax_top.scatter(windows, concurrency, color=VIOLET, s=36, zorder=4)
    ax_top.set_ylabel("Concurrency level")
    ax_top.set_title("Adaptive Concurrency: Level and Throughput per Window")
    ax_top.yaxis.set_major_locator(MaxNLocator(integer=True))
    _style_axes(ax_top)
    ax_top.set_axisbelow(True)
    if multiple_runs:
        ax_top.text(
            0.0, 1.18, f"{len(set(run_ids))} runs shown consecutively by window index",
            transform=ax_top.transAxes, fontsize=9, color=INK_MUTED,
        )

    ax_bottom.bar(windows, inserted_ok, width=0.6, color=AQUA, label="Completed successfully", zorder=3)
    ax_bottom.bar(windows, errors, width=0.6, bottom=inserted_ok, color=RED, label="Errors", zorder=3)
    ax_bottom.set_ylabel("Requests per window")
    ax_bottom.set_xlabel("Window number (file order)")
    ax_bottom.xaxis.set_major_locator(MaxNLocator(integer=True))
    _style_axes(ax_bottom)
    ax_bottom.set_axisbelow(True)
    ax_bottom.legend(frameon=False, loc="upper center", bbox_to_anchor=(0.5, -0.22), ncol=2)

    png_path, svg_path = _save_figure(fig, figures_dir, name)
    return {"name": name, "status": "created", "png": png_path, "svg": svg_path}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _print_result(result: dict) -> None:
    if result["status"] == "created":
        print(f"  [created] {result['name']}: {result['png']}, {result['svg']}")
    else:
        print(f"  [skipped] {result['name']}: {result['reason']}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate publication-quality figures from the ISIC evaluation CSV reports "
                    "(read-only, no database access, no API calls)."
    )
    parser.add_argument("--reports-dir", default=REPORTS_DIR_DEFAULT)
    args = parser.parse_args()

    reports_dir = Path(args.reports_dir)
    figures_dir = reports_dir / FIGURES_SUBDIR

    if not reports_dir.exists():
        print(f"ERROR: reports directory not found: {reports_dir}", file=sys.stderr)
        sys.exit(1)

    print("=" * 64)
    print("ISIC Evaluation Figures")
    print("=" * 64)
    print(f"  reports dir : {reports_dir}")
    print(f"  figures dir : {figures_dir}")
    print()

    builders = [
        build_top20_divisions,
        build_confidence_distribution,
        build_classification_coverage,
        build_model_agreement,
        build_concurrency_throughput,
    ]

    results = []
    for build in builders:
        result = build(reports_dir, figures_dir)
        results.append(result)
        _print_result(result)

    created = [r for r in results if r["status"] == "created"]
    skipped = [r for r in results if r["status"] == "skipped"]

    print()
    print("=" * 64)
    print(f"Summary: {len(created)} created, {len(skipped)} skipped")
    print("=" * 64)


if __name__ == "__main__":
    main()
