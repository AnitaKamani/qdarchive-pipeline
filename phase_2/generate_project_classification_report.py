"""
Generate the per-repository project classification report as a vector PDF.

Read-only: opens the database via a SQLite read-only URI, makes no API calls,
and does not touch Phase 1, the classification pipeline, or the schema. Uses
the same cross-model selection rule as export_project_classification_table.py
(see project_classification_data.py): prefer --preferred-method, fall back to
--fallback-method, ignore local-dry-run/model_error.

Unlike the XLSX export (which defaults to classified-only), this report
always covers the full eligible population per repository — coverage and
unclassified counts are reported honestly regardless of how complete
production classification currently is.

A note on "repository_id": it is assigned locally by each contributing
student's own harvesting configuration (see config.py's REPOS list) and is
not a globally unique identifier — the same numeric ID can denote different
real-world source repositories across students. Repository 99 was inspected
specifically (see project history) and found to be genuine, correctly
classified data (a Zenodo-sourced project), not an artifact, so it is kept
in every output like any other repository. This caveat is stated explicitly
in the report's Limitations section rather than silently omitted.

Usage:
    python phase_2/generate_project_classification_report.py [options]

Options:
    --db                PATH   default: 23727550-sq26-combined.db
    --output            PATH   default: reports/23727550-sq26-project-classification-report.pdf
    --preferred-method  METHOD default: openai:gpt-4.1-mini
    --fallback-method   METHOD default: openai:gpt-4o-mini
    --top-n             N      default: 20 (table rows; charts show a smaller
                                CHART_TOP_N for label readability at reduced size)
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
import tempfile
import textwrap
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

_here = Path(__file__).parent
if str(_here) not in sys.path:
    sys.path.insert(0, str(_here))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.ticker import FuncFormatter

from project_classification_data import (
    DEFAULT_FALLBACK_METHOD,
    DEFAULT_PREFERRED_METHOD,
    connect_readonly,
    fetch_project_rows,
    isic_label,
    load_isic_titles,
    print_schema_decisions,
)

DB_DEFAULT = "23727550-sq26-combined.db"
OUTPUT_DEFAULT = "reports/23727550-sq26-project-classification-report.pdf"
VALIDATION_REPORT = "reports/project_classification_report_validation.csv"
TOP_N_DEFAULT = 20
CHART_TOP_N = 12  # bar charts show fewer classes than the table so wrapped labels stay readable at reduced size
STUDENT_ID = "23727550"
AUTHOR = "Fatemeh Kamani"
SUPERVISOR = "Prof. Riehle"
REPORT_DATE = "July 2026"

# A4 portrait, inches, with standard 1-inch margins on every page.
PAGE_SIZE = (8.27, 11.69)
MARGIN_IN = 1.0
CONTENT_LEFT = MARGIN_IN / PAGE_SIZE[0]
CONTENT_RIGHT = 1 - MARGIN_IN / PAGE_SIZE[0]
CONTENT_BOTTOM = MARGIN_IN / PAGE_SIZE[1]
CONTENT_TOP = 1 - MARGIN_IN / PAGE_SIZE[1]
CONTENT_WIDTH = CONTENT_RIGHT - CONTENT_LEFT
CONTENT_HEIGHT = CONTENT_TOP - CONTENT_BOTTOM
FOOTER_Y = 0.4 / PAGE_SIZE[1]  # 0.4in from the bottom edge, inside the margin band

# --- Palette: a single consistent blue family throughout the report ---
SURFACE = "#fcfcfb"
INK_PRIMARY = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRIDLINE = "#e1e0d9"
BASELINE = "#c3c2b7"
BLUE_DARK = "#0d366b"    # table/section headers, darkest chart bars, "classified" pie slice
BLUE_LIGHT = "#9ec5f4"   # "remaining" pie slice, light chart bars
SEQUENTIAL_BLUE = LinearSegmentedColormap.from_list("seq_blue", ["#cde2fb", BLUE_DARK])

# Calibri is the preferred typeface; where it isn't installed, matplotlib's
# font-family fallback chain resolves to the next available name (Helvetica,
# then Arial, then the bundled DejaVu Sans) without raising an error. The
# "Font family not found" notice this produces for the first (unavailable)
# name is expected and not a fault — silence it rather than let it clutter
# console output on every run.
logging.getLogger("matplotlib.font_manager").setLevel(logging.ERROR)

plt.rcParams.update({
    "figure.facecolor": SURFACE,
    "axes.facecolor": SURFACE,
    "savefig.facecolor": SURFACE,
    "text.color": INK_PRIMARY,
    "font.family": ["Calibri", "Helvetica", "Arial", "DejaVu Sans"],
    # All pages are built in memory before any is saved (so the footer can
    # print a real "Page X of Y"), so it's normal to have dozens of figures
    # open at once here — not a leak, so the default open-figure warning
    # would just be noise on every run.
    "figure.max_open_warning": 0,
})


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _wrap_label(text: str, width: int, max_lines: int = 2) -> str:
    """Word-wrap to at most `max_lines` lines instead of truncating with an
    ellipsis mid-word. Only the small number of ISIC titles long enough to
    exceed max_lines at this width fall back to a soft ellipsis on the last
    line, so the common case is always a full, readable label."""
    lines = textwrap.wrap(text, width=width) or [""]
    if len(lines) <= max_lines:
        return "\n".join(lines)
    kept = lines[:max_lines]
    last = kept[-1]
    if len(last) > width - 1:
        last = last[: width - 1].rstrip()
    kept[-1] = last + "…"
    return "\n".join(kept)


def _label_ink(hex_color: str) -> str:
    """White text on a dark fill, dark ink on a light fill."""
    r, g, b = (int(hex_color.lstrip("#")[i:i + 2], 16) for i in (0, 2, 4))
    luminance = 0.2126 * r + 0.7152 * g + 0.0722 * b
    return "#ffffff" if luminance < 140 else INK_PRIMARY


def _leading(axes_height_fraction: float, points: float) -> float:
    """Axes-fraction vertical step corresponding to `points` (1/72 inch) of
    actual page space. Text is drawn with `transform=ax.transAxes`, so a
    fixed axes-fraction step means a different absolute line height on every
    differently-sized sub-axes unless it's rescaled by that axes' own height
    — this converts a desired physical line height into the right fraction
    for whichever axes it's drawn into."""
    axes_height_in = axes_height_fraction * PAGE_SIZE[1]
    return (points / 72.0) / axes_height_in


def _style_axes(ax) -> None:
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color(BASELINE)


def _new_page() -> tuple[plt.Figure, plt.Axes]:
    """A blank A4 page with one axes spanning the standard 1-inch content box."""
    fig = plt.figure(figsize=PAGE_SIZE)
    ax = fig.add_axes([CONTENT_LEFT, CONTENT_BOTTOM, CONTENT_WIDTH, CONTENT_HEIGHT])
    ax.axis("off")
    return fig, ax


def _add_footer(fig: plt.Figure, page_num: int, total_pages: int) -> None:
    fig.text(
        0.5, FOOTER_Y, f"{AUTHOR} | Project Classification Report | Page {page_num} of {total_pages}",
        ha="center", va="center", fontsize=8, color=INK_MUTED,
    )


def _draw_lines(ax, x: float, y: float, lines: list[str], fontsize: float = 11,
                 line_spacing: float = 0.032) -> float:
    """Render pre-built short lines verbatim, one per row, with no wrapping —
    for label: value stats lines that are already known to be short."""
    for line in lines:
        ax.text(x, y, line, transform=ax.transAxes, fontsize=fontsize, color=INK_PRIMARY, va="top", ha="left")
        y -= line_spacing
    return y


def _draw_paragraphs(ax, x: float, y: float, paragraphs: list[str], fontsize: float = 10.5,
                      wrap_width: int = 92, line_spacing: float = 0.026, para_gap: float = 0.016) -> float:
    """Word-wrap each paragraph to `wrap_width` characters (tuned for the
    default axes width and font) and render one line per matplotlib text
    call, so long sentences never run off the page edge."""
    for para in paragraphs:
        for line in textwrap.wrap(para, width=wrap_width) or [""]:
            ax.text(x, y, line, transform=ax.transAxes, fontsize=fontsize, color=INK_PRIMARY, va="top", ha="left")
            y -= line_spacing
        y -= para_gap
    return y


def _draw_bullets(ax, x: float, y: float, items: list[str], fontsize: float = 10.5,
                   wrap_width: int = 88, line_spacing: float = 0.026, bullet_gap: float = 0.016) -> float:
    """Word-wrap each bullet item, indenting continuation lines under the
    bullet marker rather than restarting at the margin."""
    continuation_indent = "   "
    for item in items:
        wrapped = textwrap.wrap(item, width=wrap_width) or [""]
        for i, line in enumerate(wrapped):
            prefix = "•  " if i == 0 else continuation_indent
            ax.text(x, y, prefix + line, transform=ax.transAxes, fontsize=fontsize, color=INK_PRIMARY,
                     va="top", ha="left")
            y -= line_spacing
        y -= bullet_gap
    return y


# ---------------------------------------------------------------------------
# Data aggregation
# ---------------------------------------------------------------------------

def aggregate(rows: list[dict]) -> dict:
    """Group the full eligible-project population by repository_id and
    compute the stats every section of the report needs."""
    by_repo: dict[int, list[dict]] = defaultdict(list)
    for row in rows:
        by_repo[row["repository_id"]].append(row)

    repo_stats = {}
    for repo_id, repo_rows in by_repo.items():
        classified = [r for r in repo_rows if r["primary_class_code"] is not None]
        class_counts = Counter(r["primary_class_code"] for r in classified)
        type_counts = Counter(r["project_type"] for r in repo_rows)
        repo_stats[repo_id] = {
            "repository_id": repo_id,
            "total": len(repo_rows),
            "classified_count": len(classified),
            "unclassified_count": len(repo_rows) - len(classified),
            "type_counts": type_counts,
            "class_counts": class_counts,
        }

    global_classified = [r for r in rows if r["primary_class_code"] is not None]
    global_stats = {
        "total": len(rows),
        "classified_count": len(global_classified),
        "unclassified_count": len(rows) - len(global_classified),
        "type_counts": Counter(r["project_type"] for r in rows),
        "class_counts": Counter(r["primary_class_code"] for r in global_classified),
        "num_repositories": len(by_repo),
    }
    return {"by_repo": repo_stats, "global": global_stats}


def _concentration_label(top_share_pct: float) -> str:
    if top_share_pct >= 40:
        return "concentrated — a single class accounts for a large share of classified projects"
    if top_share_pct <= 15:
        return "diverse — no single class dominates"
    return "moderately concentrated"


def generate_comments(stats: dict, titles: dict[str, str]) -> list[str]:
    """Factual, conservative observations derived only from the counts
    already computed — no inference about *why* a distribution looks the
    way it does."""
    total = stats["total"]
    classified = stats["classified_count"]
    coverage_pct = (classified / total * 100) if total else 0.0
    comments = [f"Classification coverage: {classified:,} of {total:,} eligible projects ({coverage_pct:.1f}%)."]

    class_counts = stats["class_counts"]
    if not class_counts:
        comments.append("No classified projects are available for class-distribution analysis.")
        return comments

    distinct = len(class_counts)
    comments.append(
        f"{distinct} distinct ISIC {'division is' if distinct == 1 else 'divisions are'} "
        f"represented among classified projects."
    )
    top_code, top_count = class_counts.most_common(1)[0]
    top_share = top_count / classified * 100 if classified else 0.0
    comments.append(
        f"The most common primary class is {isic_label(top_code, titles)}, accounting for "
        f"{top_share:.1f}% of classified projects ({top_count:,})."
    )
    comments.append(f"Overall distribution: {_concentration_label(top_share)} (top class share {top_share:.1f}%).")
    return comments


# ---------------------------------------------------------------------------
# Page builders
# ---------------------------------------------------------------------------

def build_title_page(db_path: str, generated_at: str, global_stats: dict,
                      preferred_method: str, fallback_method: str) -> plt.Figure:
    """Title block plus a compact executive-summary table sharing the same
    page, so the report leads with the key figures without a near-empty page."""
    fig = plt.figure(figsize=PAGE_SIZE)

    title_h = CONTENT_HEIGHT * 0.46
    title_ax = fig.add_axes([CONTENT_LEFT, CONTENT_TOP - title_h, CONTENT_WIDTH, title_h])
    title_ax.axis("off")

    rounded_thousands = (global_stats["total"] // 1000) * 1000
    lines_top = [
        (0.86, "QDArchive Project Classification Report", 20, "bold", INK_PRIMARY),
        (0.76, "ISIC Rev. 5 classification of QDA/QD projects", 12.5, "normal", INK_SECONDARY),
    ]
    for y, text, fontsize, weight, color in lines_top:
        title_ax.text(0.5, y, text, transform=title_ax.transAxes, fontsize=fontsize, fontweight=weight,
                       ha="center", color=color)

    meta_lines = [
        f"Author: {AUTHOR}",
        f"Supervisor: {SUPERVISOR}",
        f"Date: {REPORT_DATE}",
        "",
        f"Student ID: {STUDENT_ID}",
        f"Database: {Path(db_path).name}",
        f"Generated: {generated_at}",
    ]
    y = 0.60
    for line in meta_lines:
        if line:
            title_ax.text(0.5, y, line, transform=title_ax.transAxes, fontsize=11, ha="center", color=INK_SECONDARY)
        y -= 0.075

    title_ax.text(
        0.5, 0.04,
        f"More than {rounded_thousands:,} eligible projects across "
        f"{global_stats['num_repositories']} successfully processed repositories",
        transform=title_ax.transAxes, fontsize=10.5, ha="center", color=INK_MUTED,
    )

    # --- Executive summary table, sharing the page rather than a near-empty one ---
    summary_h = CONTENT_HEIGHT * 0.30
    summary_top = CONTENT_TOP - title_h - CONTENT_HEIGHT * 0.06
    heading_ax = fig.add_axes([CONTENT_LEFT, summary_top - 0.03, CONTENT_WIDTH, 0.03])
    heading_ax.axis("off")
    heading_ax.text(0.0, 1.0, "Executive Summary", transform=heading_ax.transAxes, fontsize=14,
                     fontweight="bold", va="top", color=INK_PRIMARY)

    table_ax = fig.add_axes([CONTENT_LEFT, summary_top - 0.06 - summary_h, CONTENT_WIDTH, summary_h])
    table_ax.axis("off")

    coverage_pct = (global_stats["classified_count"] / global_stats["total"] * 100) if global_stats["total"] else 0.0
    rows = [
        ["Eligible projects", f"{global_stats['total']:,}"],
        ["Classified projects", f"{global_stats['classified_count']:,}"],
        ["Coverage", f"{coverage_pct:.1f}%"],
        ["Successfully processed repositories", f"{global_stats['num_repositories']}"],
        ["Primary classification models", f"{preferred_method} (preferred), {fallback_method} (fallback)"],
    ]
    table = table_ax.table(
        cellText=rows, colLabels=["Metric", "Value"], loc="upper center", cellLoc="left",
        colWidths=[0.45, 0.55],
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9.5)
    table.scale(1, 1.9)
    for (row, _col), cell in table.get_celld().items():
        cell.set_edgecolor(GRIDLINE)
        if row == 0:
            cell.set_text_props(fontweight="bold", color="white")
            cell.set_facecolor(BLUE_DARK)
        else:
            cell.set_facecolor(SURFACE)

    return fig


def build_methodology_page(preferred_method: str, fallback_method: str) -> plt.Figure:
    fig, ax = _new_page()
    ax.text(0.0, 0.98, "Methodology", transform=ax.transAxes, fontsize=17, fontweight="bold", color=INK_PRIMARY)

    paragraphs = [
        "Project type (QDA_PROJECT / QD_PROJECT / OTHER_PROJECT / NOT_A_PROJECT) was assigned in "
        "Phase 1 by a deterministic, rule-based classifier operating on project metadata. Only "
        "QDA_PROJECT and QD_PROJECT records are in scope for this report.",

        "ISIC Rev. 5 division classification was performed in Phase 2 by a model-based classifier. "
        f"Two OpenAI production models were used over the course of the project: {fallback_method} and "
        f"{preferred_method}. Where a project was successfully classified by both, this report and the "
        f"accompanying classification table prefer {preferred_method}; otherwise the {fallback_method} "
        "result is used. Exploratory or comparison runs (e.g. local-dry-run) are never used as a final "
        "classification.",

        "Every model response was required to conform to a structured JSON schema constraining the "
        "returned class codes to the 87 valid ISIC Rev. 5 divisions (Structured Outputs). Responses "
        "were additionally validated against the isic_divisions reference table; an invalid code was "
        "corrected only when the model's own stated reason named exactly one division title verbatim, "
        "and was otherwise kept as an error rather than guessed.",

        "Production classification is ongoing and, at the time this report was generated, does not yet "
        "cover every eligible project. Unclassified projects are not hidden or excluded from the "
        "statistics below: coverage and remaining-unclassified counts are reported for every repository "
        "and for the archive as a whole.",
    ]
    _draw_paragraphs(ax, 0.0, 0.94, paragraphs, fontsize=10.5, wrap_width=92,
                      line_spacing=_leading(CONTENT_HEIGHT, 15), para_gap=_leading(CONTENT_HEIGHT, 9))
    return fig


def _stats_lines(stats: dict, extra_lines: list[str] | None = None) -> list[str]:
    lines = [
        f"Total eligible projects: {stats['total']:,}",
        f"Classified: {stats['classified_count']:,}      Unclassified: {stats['unclassified_count']:,}",
        f"Coverage: {(stats['classified_count'] / stats['total'] * 100) if stats['total'] else 0:.1f}%",
        "",
        "Counts by project type:",
    ]
    for ptype, count in sorted(stats["type_counts"].items(), key=lambda kv: -kv[1]):
        lines.append(f"  {ptype}: {count:,}")
    if extra_lines:
        lines.append("")
        lines.extend(extra_lines)
    return lines


def _build_pie_chart(ax, classified: int, unclassified: int) -> None:
    total = classified + unclassified
    ax.set_title("Classified vs Remaining", fontsize=11.5, fontweight="bold", color=INK_PRIMARY, pad=10)
    if total == 0:
        ax.axis("off")
        ax.text(0.5, 0.5, "No eligible projects.", ha="center", va="center", transform=ax.transAxes, color=INK_MUTED)
        return

    values = [classified, unclassified]
    colors = [BLUE_DARK, BLUE_LIGHT]
    wedges, _texts, autotexts = ax.pie(
        values, colors=colors, startangle=90, counterclock=False,
        autopct=lambda pct: f"{pct:.1f}%",
        pctdistance=0.7, wedgeprops={"edgecolor": SURFACE, "linewidth": 2},
        textprops={"fontsize": 9.5, "fontweight": "bold"},
    )
    for at, color in zip(autotexts, colors):
        at.set_color(_label_ink(color))
    ax.legend(
        wedges, [f"Classified ({classified:,})", f"Remaining ({unclassified:,})"],
        loc="upper center", bbox_to_anchor=(0.5, -0.02), ncol=1, frameon=False, fontsize=8.8,
    )
    ax.set_aspect("equal")


def build_global_summary_pages(global_stats: dict, titles: dict[str, str]) -> list[plt.Figure]:
    fig = plt.figure(figsize=PAGE_SIZE)
    head_h = 0.05
    head_ax = fig.add_axes([CONTENT_LEFT, CONTENT_TOP - head_h, CONTENT_WIDTH, head_h])
    head_ax.axis("off")
    head_ax.text(0.0, 1.0, "Global Summary", transform=head_ax.transAxes, fontsize=17, fontweight="bold",
                 va="top", color=INK_PRIMARY)

    body_top = CONTENT_TOP - head_h - 0.02
    body_h = body_top - CONTENT_BOTTOM
    stats_w = CONTENT_WIDTH * 0.56
    stats_ax = fig.add_axes([CONTENT_LEFT, body_top - body_h, stats_w, body_h])
    stats_ax.axis("off")
    extra = [f"Distinct ISIC classes observed: {len(global_stats['class_counts'])}"] if global_stats["class_counts"] else []
    lines = _stats_lines(global_stats, extra_lines=extra)
    _draw_lines(stats_ax, 0.0, 0.97, lines, fontsize=11, line_spacing=_leading(body_h, 20))

    pie_w = CONTENT_WIDTH * 0.38
    pie_left = CONTENT_LEFT + CONTENT_WIDTH * 0.62
    pie_ax = fig.add_axes([pie_left, body_top - body_h * 0.62, pie_w, body_h * 0.55])
    _build_pie_chart(pie_ax, global_stats["classified_count"], global_stats["unclassified_count"])

    pages = [fig]
    chart = _build_bar_chart_page(
        global_stats["class_counts"], titles, top_n=CHART_TOP_N,
        chart_title=f"Top {min(CHART_TOP_N, len(global_stats['class_counts']))} Primary ISIC Classes — All Repositories",
        subtitle=f"{global_stats['classified_count']:,} classified projects across "
                 f"{global_stats['num_repositories']} repositories",
    )
    if chart is not None:
        pages.append(chart)
    return pages


def _build_bar_chart_axes(ax, class_counts: Counter, titles: dict[str, str], top_n: int, label_width: int = 32) -> None:
    top = class_counts.most_common(top_n)
    total_classified = sum(class_counts.values())
    plotted = list(reversed(top))
    labels = [_wrap_label(isic_label(code, titles), label_width) for code, _ in plotted]
    counts = [c for _, c in plotted]
    pcts = [c / total_classified * 100 for c in counts]

    max_count = max(counts) or 1
    colors = [SEQUENTIAL_BLUE(0.25 + 0.75 * (c / max_count)) for c in counts]
    bars = ax.barh(labels, counts, color=colors, height=0.58, zorder=3)
    _style_axes(ax)
    ax.xaxis.grid(True, color=GRIDLINE, linewidth=1, zorder=0)
    ax.set_axisbelow(True)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _pos: f"{int(v):,}"))
    ax.set_xlabel("Classified projects", fontsize=9.5)
    ax.tick_params(axis="y", labelsize=8.7)
    ax.tick_params(axis="x", labelsize=8.7)

    max_x = max(counts)
    for bar, count, pct in zip(bars, counts, pcts):
        ax.text(bar.get_width() + max_x * 0.02, bar.get_y() + bar.get_height() / 2,
                f"{count:,} ({pct:.1f}%)", va="center", ha="left", fontsize=8.2, color=INK_SECONDARY)
    ax.set_xlim(0, max_x * 1.28)


def _build_bar_chart_page(class_counts: Counter, titles: dict[str, str], top_n: int,
                           chart_title: str, subtitle: str) -> plt.Figure | None:
    """A standalone chart page, sized to roughly 70-75% of the full previous
    full-page chart footprint (reduced ~25-30%) while remaining vector."""
    if not class_counts:
        return None

    fig = plt.figure(figsize=PAGE_SIZE)
    chart_left = CONTENT_LEFT + CONTENT_WIDTH * 0.30
    chart_width = CONTENT_RIGHT - chart_left
    chart_height = CONTENT_HEIGHT * 0.62  # ~25-30% smaller than the prior full-content-height chart
    chart_top = CONTENT_TOP - 0.08  # anchored just below the title/subtitle, not centered in the page
    chart_bottom = chart_top - chart_height
    ax = fig.add_axes([chart_left, chart_bottom, chart_width, chart_height])
    _build_bar_chart_axes(ax, class_counts, titles, top_n, label_width=30)

    fig.text(0.5, CONTENT_TOP - 0.01, chart_title, fontsize=13, fontweight="bold", ha="center", color=INK_PRIMARY)
    fig.text(0.5, CONTENT_TOP - 0.035, subtitle, fontsize=9, ha="center", color=INK_MUTED)
    return fig


def _build_table_and_comments_page(page_title: str, class_counts: Counter, titles: dict[str, str],
                                    top_n: int, comments: list[str]) -> plt.Figure:
    fig = plt.figure(figsize=PAGE_SIZE)
    fig.text(CONTENT_LEFT, CONTENT_TOP, page_title, fontsize=15, fontweight="bold", va="top", color=INK_PRIMARY)

    total_classified = sum(class_counts.values())
    top = class_counts.most_common(top_n)

    table_h = CONTENT_HEIGHT * 0.74
    table_top = CONTENT_TOP - 0.04
    table_ax = fig.add_axes([CONTENT_LEFT, table_top - table_h, CONTENT_WIDTH, table_h])
    table_ax.axis("off")

    col_labels = ["Rank", "ISIC code", "ISIC division title", "Count", "%"]
    cell_text = [
        [str(rank), code, _wrap_label(titles.get(code, code), 44), f"{count:,}",
         f"{(count / total_classified * 100) if total_classified else 0:.1f}%"]
        for rank, (code, count) in enumerate(top, start=1)
    ]
    table = table_ax.table(
        cellText=cell_text, colLabels=col_labels, loc="upper center", cellLoc="left",
        colWidths=[0.08, 0.14, 0.56, 0.11, 0.11],
    )
    table.auto_set_font_size(False)
    table.set_fontsize(7.8)
    table.scale(1, 1.55)  # tall enough for up to 2 wrapped lines per title, without overflowing table_h
    for (row, _col), cell in table.get_celld().items():
        cell.set_edgecolor(GRIDLINE)
        if row == 0:
            cell.set_text_props(fontweight="bold", color="white")
            cell.set_facecolor(BLUE_DARK)
        else:
            cell.set_facecolor(SURFACE)

    comments_h = CONTENT_HEIGHT * 0.13
    comments_ax = fig.add_axes([CONTENT_LEFT, CONTENT_BOTTOM, CONTENT_WIDTH, comments_h])
    comments_ax.axis("off")
    comments_ax.text(0.0, 1.0, "Findings", transform=comments_ax.transAxes, fontsize=11.5,
                      fontweight="bold", va="top", color=INK_PRIMARY)
    _draw_bullets(comments_ax, 0.0, 0.80, comments, fontsize=9.3, wrap_width=104,
                  line_spacing=_leading(comments_h, 12), bullet_gap=_leading(comments_h, 6))
    return fig


def build_repository_pages(repo_id: int, stats: dict, titles: dict[str, str], top_n: int) -> list[plt.Figure]:
    fig = plt.figure(figsize=PAGE_SIZE)
    text_h = CONTENT_HEIGHT * 0.30
    text_ax = fig.add_axes([CONTENT_LEFT, CONTENT_TOP - text_h, CONTENT_WIDTH, text_h])
    text_ax.axis("off")
    text_ax.text(0.0, 0.97, f"Repository {repo_id}", transform=text_ax.transAxes, fontsize=16,
                 fontweight="bold", va="top", color=INK_PRIMARY)
    _draw_lines(text_ax, 0.0, 0.72, _stats_lines(stats), fontsize=10.5, line_spacing=_leading(text_h, 16))

    pages = [fig]
    has_chart = bool(stats["class_counts"])
    if has_chart:
        chart_top = CONTENT_TOP - text_h - 0.03
        chart_height = chart_top - CONTENT_BOTTOM
        chart_left = CONTENT_LEFT + CONTENT_WIDTH * 0.32
        chart_ax = fig.add_axes([chart_left, CONTENT_BOTTOM, CONTENT_RIGHT - chart_left, chart_height])
        _build_bar_chart_axes(chart_ax, stats["class_counts"], titles, top_n=CHART_TOP_N, label_width=28)
        fig.text(
            0.5, chart_top + 0.012,
            f"Top {min(CHART_TOP_N, len(stats['class_counts']))} Primary ISIC Classes — Repository {repo_id}",
            fontsize=11.5, fontweight="bold", ha="center", color=INK_PRIMARY,
        )

    if has_chart:
        comments = generate_comments(stats, titles)
        pages.append(_build_table_and_comments_page(
            f"Repository {repo_id} — Top {min(top_n, len(stats['class_counts']))} ISIC Classes",
            stats["class_counts"], titles, top_n, comments,
        ))
    else:
        no_data_ax = fig.add_axes([CONTENT_LEFT, CONTENT_BOTTOM, CONTENT_WIDTH, CONTENT_TOP - text_h - CONTENT_BOTTOM])
        no_data_ax.axis("off")
        no_data_ax.text(0.0, 1.0, "Findings", transform=no_data_ax.transAxes, fontsize=13, fontweight="bold",
                         va="top", color=INK_PRIMARY)
        comments = generate_comments(stats, titles)
        _draw_bullets(no_data_ax, 0.0, 0.90, comments, fontsize=10.5, wrap_width=92,
                      line_spacing=_leading(CONTENT_TOP - text_h - CONTENT_BOTTOM, 16),
                      bullet_gap=_leading(CONTENT_TOP - text_h - CONTENT_BOTTOM, 9))

    return pages


def build_limitations_page() -> plt.Figure:
    fig, ax = _new_page()
    ax.text(0.0, 0.98, "Limitations", transform=ax.transAxes, fontsize=17, fontweight="bold", color=INK_PRIMARY)
    points = [
        "No external gold-standard labels were available for this archive: reported agreement and "
        "coverage figures describe the production models' output, not verified accuracy against a "
        "human-annotated ground truth.",
        "Some projects can plausibly map to more than one ISIC division; a single primary_class is "
        "reported per project (with an optional secondary_class in the classification table), so a "
        "close second-best division is not always visible in the histograms in this report.",
        "Where the two production models disagree on a project, this does not necessarily imply a "
        "clear classification error on either side — ISIC assignment for interdisciplinary research "
        "projects is not always unambiguous.",
        "Coverage figures in this report reflect the state of the production database at generation "
        "time. If production inference is still running or was interrupted, coverage may be "
        "incomplete and will increase on a subsequent run of this report.",
        "The repository_id used to group projects in this report is assigned locally by each "
        "contributing student's own harvesting configuration; it is not a globally unique repository "
        "identifier, and the same numeric ID can denote different real-world source repositories "
        "across students. Repository sections here should be read as processing groupings rather than "
        "a canonical list of distinct external archives.",
    ]
    _draw_bullets(ax, 0.0, 0.90, points, fontsize=10.5, wrap_width=92,
                  line_spacing=_leading(CONTENT_HEIGHT, 15), bullet_gap=_leading(CONTENT_HEIGHT, 9))
    return fig


# ---------------------------------------------------------------------------
# Assembly + validation
# ---------------------------------------------------------------------------

def generate_report(
    db_path: str,
    output_path: str,
    preferred_method: str,
    fallback_method: str,
    top_n: int,
) -> dict:
    conn = connect_readonly(db_path)
    print_schema_decisions(preferred_method, fallback_method, include_unclassified=True)
    print("(the PDF report always covers the full eligible population, classified or not)")

    titles = load_isic_titles(conn)
    rows = fetch_project_rows(
        conn, preferred_method=preferred_method, fallback_method=fallback_method, include_unclassified=True,
    )
    conn.close()

    aggregated = aggregate(rows)
    global_stats = aggregated["global"]
    by_repo = aggregated["by_repo"]
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Build every page in memory first so the footer can print "Page X of Y"
    # with a real total, then save them all in a second pass.
    pages: list[plt.Figure] = []
    pages.append(build_title_page(db_path, generated_at, global_stats, preferred_method, fallback_method))
    pages.append(build_methodology_page(preferred_method, fallback_method))
    pages.extend(build_global_summary_pages(global_stats, titles))

    repo_ids_processed: list[int] = []
    for repo_id in sorted(by_repo.keys()):
        pages.extend(build_repository_pages(repo_id, by_repo[repo_id], titles, top_n))
        repo_ids_processed.append(repo_id)

    pages.append(build_limitations_page())
    total_pages = len(pages)

    with tempfile.TemporaryDirectory() as _tmp_dir:
        # Any ad-hoc chart preview during generation would be written under
        # _tmp_dir and discarded when this block exits; the PDF pages below
        # are written directly (vector) into the final PdfPages output, so no
        # intermediate raster files are produced in the normal path.
        with PdfPages(out_path) as pdf:
            for i, fig in enumerate(pages, start=1):
                _add_footer(fig, i, total_pages)
                pdf.savefig(fig)
                plt.close(fig)

            info = pdf.infodict()
            info["Title"] = "QDArchive Project Classification Report"
            info["Subject"] = f"Student {STUDENT_ID} — ISIC Rev. 5 classification statistics"

    return {
        "output_path": out_path,
        "page_count": total_pages,
        "repo_ids_processed": repo_ids_processed,
        "global_stats": global_stats,
        "by_repo": by_repo,
        "titles": titles,
    }


def validate(result: dict, valid_codes: set[str]) -> list[dict]:
    checks: list[dict] = []

    def add(name: str, passed: bool, detail: str = "") -> None:
        checks.append({"check": name, "status": "PASS" if passed else "FAIL", "detail": detail})

    out_path: Path = result["output_path"]
    add("PDF file exists and is non-empty", out_path.exists() and out_path.stat().st_size > 0,
        f"{out_path} ({out_path.stat().st_size if out_path.exists() else 0:,} bytes)")

    try:
        from pypdf import PdfReader
        reader = PdfReader(str(out_path))
        actual_pages = len(reader.pages)
    except Exception as exc:  # defensive: validation must not crash the run
        actual_pages = None
        add("PDF reopens without error", False, str(exc))
    else:
        add("PDF reopens without error", True, f"{actual_pages} pages")

    if actual_pages is not None:
        add("page count matches generated pages", actual_pages == result["page_count"],
            f"{actual_pages} in file vs {result['page_count']} generated")

    expected_repos = set(result["by_repo"].keys())
    add(
        "one section generated per repository",
        set(result["repo_ids_processed"]) == expected_repos,
        f"{len(result['repo_ids_processed'])} sections for {len(expected_repos)} repositories",
    )

    invalid_codes = set()
    for stats in result["by_repo"].values():
        invalid_codes |= (set(stats["class_counts"].keys()) - valid_codes)
    invalid_codes |= (set(result["global_stats"]["class_counts"].keys()) - valid_codes)
    add("all charted/tabled classes map to isic_divisions", len(invalid_codes) == 0,
        f"unmapped codes: {sorted(invalid_codes)}" if invalid_codes else "none")

    total_from_repos = sum(s["total"] for s in result["by_repo"].values())
    add(
        "per-repository totals sum to the global total",
        total_from_repos == result["global_stats"]["total"],
        f"{total_from_repos:,} vs {result['global_stats']['total']:,}",
    )

    return checks


def write_validation_report(checks: list[dict]) -> None:
    Path("reports").mkdir(parents=True, exist_ok=True)
    with open(VALIDATION_REPORT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["check", "status", "detail"])
        w.writeheader()
        w.writerows(checks)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the per-repository project classification PDF report.")
    parser.add_argument("--db", default=DB_DEFAULT)
    parser.add_argument("--output", default=OUTPUT_DEFAULT)
    parser.add_argument("--preferred-method", default=DEFAULT_PREFERRED_METHOD)
    parser.add_argument("--fallback-method", default=DEFAULT_FALLBACK_METHOD)
    parser.add_argument("--top-n", type=int, default=TOP_N_DEFAULT)
    args = parser.parse_args()

    print("=" * 64)
    print("Project Classification Report Generation")
    print("=" * 64)

    result = generate_report(
        db_path=args.db,
        output_path=args.output,
        preferred_method=args.preferred_method,
        fallback_method=args.fallback_method,
        top_n=args.top_n,
    )
    print(f"\nPDF written to {result['output_path']} ({result['page_count']} pages, "
          f"{len(result['repo_ids_processed'])} repository sections)")

    checks = validate(result, set(result["titles"].keys()))
    write_validation_report(checks)

    print()
    print("Validation:")
    all_pass = True
    for c in checks:
        detail = f" ({c['detail']})" if c["detail"] else ""
        print(f"  [{c['status']}] {c['check']}{detail}")
        if c["status"] != "PASS":
            all_pass = False
    print(f"\n  Report: {VALIDATION_REPORT}")
    print("=" * 64)

    if not all_pass:
        sys.exit(1)


if __name__ == "__main__":
    main()
