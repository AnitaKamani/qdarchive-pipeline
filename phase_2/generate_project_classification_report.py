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
real-world source repositories across students. This caveat is stated
explicitly in the report's Limitations section rather than silently omitted.

A fixed set of source repositories is excluded from every final-output
statistic in this report — see project_classification_data.EXCLUDED_REPOSITORY_IDS,
which fetch_project_rows() applies once so this and every other final-output
script stay consistent without a separate presentation-layer check.

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
import re
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
import matplotlib.font_manager as font_manager
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.ticker import FuncFormatter

from project_classification_data import (
    DEFAULT_FALLBACK_METHOD,
    DEFAULT_PREFERRED_METHOD,
    EXCLUDED_REPOSITORY_IDS,
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

# Table-of-contents layout: readable body-text size (never a tiny footnote
# size) and a comfortable leading, both in points so the row budget below
# scales correctly regardless of page geometry.
TOC_FONT_SIZE = 11
TOC_ROW_LEADING_PTS = 22
TOC_HEADING_RESERVE_IN = 0.75  # space reserved for the page's own heading + gap before the first row


def _repo_anchor(repo_id: int) -> str:
    """Stable named-destination/bookmark anchor for a repository section.
    repository_id is always a plain integer column, but this sanitizes
    defensively so the anchor is never built from anything containing
    characters a PDF name/destination can't safely carry."""
    return "repository_" + re.sub(r"[^0-9a-zA-Z_]+", "_", str(repo_id))

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

# Font selection: one shared resolver, one shared result, consumed by every
# text-producing part of this file (title page, headings, body text, tables,
# chart text, footers, page numbers) so nothing independently re-resolves its
# own fallback. Calibri and Aptos ship with Microsoft Office/365, not with
# plain macOS, so on a stock Mac this normally resolves to Helvetica Neue;
# nothing here raises if a preferred name isn't installed.
FONT_FAMILY_CANDIDATES = ["Calibri", "Aptos", "Helvetica Neue", "Helvetica"]


def _best_variant_entry(entries: list, want_italic: bool, want_bold: bool):
    """Pick the installed face closest to the requested weight/style out of
    every face matplotlib's font manager found for one family (a .ttc/.ttf
    can bundle several weights as separate faces)."""
    target_weight = 700 if want_bold else 400
    candidates = [e for e in entries if (e.style == "italic") == want_italic]
    if not candidates:
        return None
    return min(candidates, key=lambda e: abs((e.weight if isinstance(e.weight, (int, float)) else 400) - target_weight))


def _find_font_variants(family: str) -> dict[str, str] | None:
    """Locate actual font FILES on this machine for `family`'s regular, bold,
    italic, and bold-italic faces, using matplotlib's font manager index
    (which itself scans the standard macOS font directories: /System/Library/
    Fonts, /Library/Fonts, ~/Library/Fonts). Returns None if the family isn't
    installed at all; falls back to the regular face for any single missing
    style variant rather than failing."""
    entries = [f for f in font_manager.fontManager.ttflist if f.name == family]
    if not entries:
        return None
    regular = _best_variant_entry(entries, want_italic=False, want_bold=False)
    if regular is None:
        return None
    bold = _best_variant_entry(entries, want_italic=False, want_bold=True) or regular
    italic = _best_variant_entry(entries, want_italic=True, want_bold=False) or regular
    bold_italic = _best_variant_entry(entries, want_italic=True, want_bold=True) or bold or italic or regular
    return {"regular": regular.fname, "bold": bold.fname, "italic": italic.fname, "bolditalic": bold_italic.fname}


def _resolve_font() -> tuple[str, dict[str, str]]:
    """The one shared resolver: walks FONT_FAMILY_CANDIDATES in order and
    returns the first family with real font files installed, plus the file
    paths for each of its four style variants. If none of the preferred
    external fonts are installed, falls back to DejaVu Sans — matplotlib's
    own bundled font, always present with no system dependency, playing the
    same role here that ReportLab's built-in base-14 Helvetica would in a
    ReportLab pipeline."""
    for name in FONT_FAMILY_CANDIDATES:
        variants = _find_font_variants(name)
        if variants is not None:
            return name, variants
    return "DejaVu Sans", _find_font_variants("DejaVu Sans") or {}


RESOLVED_FONT_FAMILY, RESOLVED_FONT_VARIANTS = _resolve_font()

# Explicitly (re-)register each resolved variant file with matplotlib's font
# manager. These are typically already auto-discovered (that's how
# _find_font_variants located them), but registering them by path here is
# idempotent and makes the dependency on these specific files explicit rather
# than incidental.
for _variant_path in set(RESOLVED_FONT_VARIANTS.values()):
    font_manager.fontManager.addfont(_variant_path)

# Silence the "Font family not found" notice matplotlib would otherwise log
# for each unavailable candidate name _resolve_font() already handled above —
# not a fault, just expected fallback resolution.
logging.getLogger("matplotlib.font_manager").setLevel(logging.ERROR)

# Applied via plt.rc_context(REPORT_RC) around page building/saving in
# generate_report() rather than a bare plt.rcParams.update() at import time.
# plt.rcParams is process-global mutable state shared with every other
# matplotlib-using module in this pipeline (e.g. plot_isic_evaluation.py sets
# its own font.family for its own charts); when regenerate_all_outputs.py
# imports both, whichever import happens to run last would otherwise win and
# silently override this module's font for the rest of the process. A scoped
# rc_context sidesteps import order entirely: this module's settings apply
# only while it is actually building and saving its own pages, and are
# restored afterward regardless of what else runs before or after it.
REPORT_RC = {
    "figure.facecolor": SURFACE,
    "axes.facecolor": SURFACE,
    "savefig.facecolor": SURFACE,
    "text.color": INK_PRIMARY,
    "font.family": RESOLVED_FONT_FAMILY,
    # Embed real TrueType font programs (each referenced by its actual
    # PostScript/family name in the PDF's font resources) instead of
    # matplotlib's default Type 3 bitmap-outline fonts, which have no
    # meaningful family name to verify — this is what makes the "is the
    # resolved family actually embedded" validation check possible at all.
    "pdf.fonttype": 42,
    # All pages are built in memory before any is saved (so the footer can
    # print a real "Page X of Y"), so it's normal to have dozens of figures
    # open at once here — not a leak, so the default open-figure warning
    # would just be noise on every run.
    "figure.max_open_warning": 0,
}


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


def _wrap_paragraph_balanced(text: str, width: int) -> list[str]:
    """Word-wrap like textwrap.wrap, but avoid leaving a short "widow" line
    (e.g. one lone short word) at the end of a paragraph. textwrap's greedy
    fill picks break points purely by character count, which can strand a
    short remainder on its own final line; trying slightly narrower widths
    that still produce the same number of lines often finds a break point
    where the last line fills a reasonable share of the column instead."""
    lines = textwrap.wrap(text, width=width) or [""]
    if len(lines) <= 1 or len(lines[-1]) >= width * 0.55:
        return lines
    for w in range(width - 1, max(width - 12, 20), -1):
        candidate = textwrap.wrap(text, width=w) or [""]
        if len(candidate) == len(lines) and len(candidate[-1]) >= width * 0.55:
            return candidate
    return lines


def _draw_paragraphs(ax, x: float, y: float, paragraphs: list[str], fontsize: float = 10.5,
                      wrap_width: int = 92, line_spacing: float = 0.026, para_gap: float = 0.016) -> float:
    """Word-wrap each paragraph to `wrap_width` characters (tuned for the
    default axes width and font), rebalanced to avoid short widow lines, and
    render one line per matplotlib text call so long sentences never run off
    the page edge. Left-aligned throughout — never centered."""
    for para in paragraphs:
        for line in _wrap_paragraph_balanced(para, wrap_width):
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

def build_title_page(db_path: str, generated_at: str, global_stats: dict) -> plt.Figure:
    """Title, author/supervisor/date metadata, and the top-line population
    figure. The numeric summary lives on the Global Summary page instead of
    a separate executive-summary page."""
    fig = plt.figure(figsize=PAGE_SIZE)
    ax = fig.add_axes([CONTENT_LEFT, CONTENT_BOTTOM, CONTENT_WIDTH, CONTENT_HEIGHT])
    ax.axis("off")

    lines_top = [
        (0.88, "QDArchive Project Classification Report", 22, "bold", INK_PRIMARY),
        (0.815, "ISIC Rev. 5 classification of QDA/QD projects", 13, "normal", INK_SECONDARY),
    ]
    for y, text, fontsize, weight, color in lines_top:
        ax.text(0.5, y, text, transform=ax.transAxes, fontsize=fontsize, fontweight=weight,
                ha="center", color=color)

    meta_lines = [
        f"{AUTHOR}",
        f"Matrikelnummer: {STUDENT_ID}",
        "",
        "",
        f"Supervisor: {SUPERVISOR}",
        f"{REPORT_DATE}",
    ]
    y = 0.62
    for line in meta_lines:
        if line:
            ax.text(0.5, y, line, transform=ax.transAxes, fontsize=11.5, ha="center", color=INK_SECONDARY)
        y -= 0.045

    rounded_thousands = (global_stats["total"] // 1000) * 1000
    ax.text(
        0.5, 0.18,
        f"More than {rounded_thousands:,} eligible projects across "
        f"{global_stats['num_repositories']} successfully processed repositories",
        transform=ax.transAxes, fontsize=11, ha="center", color=INK_MUTED,
    )
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


def _build_project_type_pie_chart(ax, type_counts: Counter) -> None:
    """QD_PROJECT vs QDA_PROJECT share of the full eligible population.
    Deliberately not classified-vs-remaining: that split changes as
    production classification proceeds and is expected to reach 100%
    coverage, whereas the project-type split is a stable property of the
    eligible population itself."""
    order = ["QDA_PROJECT", "QD_PROJECT"]
    ax.set_title("Eligible Projects by Project Type", fontsize=11.5, fontweight="bold", color=INK_PRIMARY, pad=10)

    present = [(t, type_counts.get(t, 0)) for t in order if type_counts.get(t, 0) > 0]
    total = sum(c for _, c in present)
    if total == 0:
        ax.axis("off")
        ax.text(0.5, 0.5, "No eligible projects.", ha="center", va="center", transform=ax.transAxes, color=INK_MUTED)
        return

    values = [c for _, c in present]
    colors = [BLUE_DARK, BLUE_LIGHT][: len(present)]
    # Percentages are shown in the legend rather than as in-wedge autopct
    # labels: with a split this lopsided (QDA_PROJECT is ~1% of the total),
    # an in-wedge label has no room to sit inside its own sliver and either
    # overlaps the neighboring wedge or renders unreadably off-center.
    wedges, _texts = ax.pie(
        values, colors=colors, startangle=90, counterclock=False,
        wedgeprops={"edgecolor": SURFACE, "linewidth": 2},
    )
    legend_labels = [f"{t} ({c:,}, {c / total * 100:.1f}%)" for t, c in present]
    ax.legend(
        wedges, legend_labels,
        loc="upper center", bbox_to_anchor=(0.5, -0.02), ncol=1, frameon=False, fontsize=8.8,
    )
    ax.set_aspect("equal")


def build_global_summary_pages(global_stats: dict, titles: dict[str, str],
                                preferred_method: str, fallback_method: str) -> list[plt.Figure]:
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
    extra = []
    if global_stats["class_counts"]:
        extra.append(f"Distinct ISIC classes observed: {len(global_stats['class_counts'])}")
    extra.append(f"Successfully processed repositories: {global_stats['num_repositories']}")
    extra.append("Production models:")
    extra.append(f"  {preferred_method} (preferred)")
    extra.append(f"  {fallback_method} (fallback)")
    lines = _stats_lines(global_stats, extra_lines=extra)
    _draw_lines(stats_ax, 0.0, 0.97, lines, fontsize=11, line_spacing=_leading(body_h, 20))

    pie_w = CONTENT_WIDTH * 0.38
    pie_left = CONTENT_LEFT + CONTENT_WIDTH * 0.62
    pie_ax = fig.add_axes([pie_left, body_top - body_h * 0.62, pie_w, body_h * 0.55])
    _build_project_type_pie_chart(pie_ax, global_stats["type_counts"])

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


# Modest, consistent gap between the bottom of a repository table and its
# Findings heading, expressed as a page-relative fraction (see _leading).
FINDINGS_GAP = 14  # points


def _build_table_and_comments_page(page_title: str, class_counts: Counter, titles: dict[str, str],
                                    top_n: int, comments: list[str]) -> list[plt.Figure]:
    """Table and Findings are flowed as one unit: the table renders at its
    natural content height (not stretched to fill a fixed box), Findings is
    placed directly beneath it with a small fixed gap, and only spills to a
    second page if it genuinely would not fit below the table."""
    fig = plt.figure(figsize=PAGE_SIZE)
    fig.text(CONTENT_LEFT, CONTENT_TOP, page_title, fontsize=15, fontweight="bold", va="top", color=INK_PRIMARY)

    total_classified = sum(class_counts.values())
    top = class_counts.most_common(top_n)

    table_top = CONTENT_TOP - 0.04
    table_ax = fig.add_axes([CONTENT_LEFT, CONTENT_BOTTOM, CONTENT_WIDTH, table_top - CONTENT_BOTTOM])
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
    table.scale(1, 1.55)  # tall enough for up to 2 wrapped lines per title
    for (row, _col), cell in table.get_celld().items():
        cell.set_edgecolor(GRIDLINE)
        if row == 0:
            cell.set_text_props(fontweight="bold", color="white")
            cell.set_facecolor(BLUE_DARK)
        else:
            cell.set_facecolor(SURFACE)

    # The table sizes itself to its actual content (rows x scale), anchored
    # to the top of table_ax — measure where its real bottom edge landed so
    # Findings can start right below it instead of at a guessed fixed offset.
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    table_bottom_fig_y = table.get_window_extent(renderer).y0 / fig.bbox.height

    gap = _leading(1.0, FINDINGS_GAP)
    findings_top = table_bottom_fig_y - gap
    findings_height = findings_top - CONTENT_BOTTOM

    fits = False
    comments_ax = None
    if findings_height > 0.05:
        comments_ax = fig.add_axes([CONTENT_LEFT, CONTENT_BOTTOM, CONTENT_WIDTH, findings_height])
        comments_ax.axis("off")
        comments_ax.text(0.0, 1.0, "Findings", transform=comments_ax.transAxes, fontsize=11.5,
                          fontweight="bold", va="top", color=INK_PRIMARY)
        # The heading-to-bullet gap must stay a small fixed physical size
        # regardless of how tall this axes happens to be (a short table
        # leaves a much taller findings_height than a full one) — computed
        # via _leading against this axes' own height, not a fixed fraction
        # of it, which would otherwise stretch into a large blank gap here.
        bullets_start_y = 1.0 - _leading(findings_height, 18)
        end_y = _draw_bullets(comments_ax, 0.0, bullets_start_y, comments, fontsize=9.3, wrap_width=104,
                              line_spacing=_leading(findings_height, 12), bullet_gap=_leading(findings_height, 6))
        fits = end_y >= -0.02

    if fits:
        return [fig]

    # Findings genuinely doesn't fit below the table on this page: keep its
    # heading and bullets together as a unit on a fresh page instead of
    # letting them straddle the boundary.
    if comments_ax is not None:
        comments_ax.remove()
    findings_fig, findings_ax = _new_page()
    findings_ax.text(0.0, 0.98, "Findings", transform=findings_ax.transAxes, fontsize=15,
                      fontweight="bold", va="top", color=INK_PRIMARY)
    _draw_bullets(findings_ax, 0.0, 0.90, comments, fontsize=10.5, wrap_width=92,
                  line_spacing=_leading(CONTENT_HEIGHT, 15), bullet_gap=_leading(CONTENT_HEIGHT, 9))
    return [fig, findings_fig]


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
        pages.extend(_build_table_and_comments_page(
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


def _toc_rows_per_page() -> int:
    """How many TOC rows fit on one page at TOC_ROW_LEADING_PTS leading,
    below the heading reserve — computed from page geometry rather than a
    hardcoded row count, so it stays correct if the page size or font size
    here ever changes."""
    available_in = CONTENT_HEIGHT * PAGE_SIZE[1] - TOC_HEADING_RESERVE_IN
    row_height_in = TOC_ROW_LEADING_PTS / 72.0
    return max(1, int(available_in // row_height_in))


def _axes_row_rect_to_pdf_points(y_top_frac: float, y_bottom_frac: float) -> tuple[float, float, float, float]:
    """Convert a TOC row's vertical band — expressed as axes-fraction
    coordinates within the standard content axes that _new_page() builds
    ([CONTENT_LEFT, CONTENT_BOTTOM, CONTENT_WIDTH, CONTENT_HEIGHT] in
    figure-fraction) — into a PDF link-annotation rectangle in points,
    spanning the full one-inch-margined content width."""
    fig_y0 = CONTENT_BOTTOM + y_bottom_frac * CONTENT_HEIGHT
    fig_y1 = CONTENT_BOTTOM + y_top_frac * CONTENT_HEIGHT
    return (
        CONTENT_LEFT * PAGE_SIZE[0] * 72, fig_y0 * PAGE_SIZE[1] * 72,
        CONTENT_RIGHT * PAGE_SIZE[0] * 72, fig_y1 * PAGE_SIZE[1] * 72,
    )


def build_toc_pages(entries: list[tuple[str, str]], page_numbers: dict[str, int]) -> tuple[list[plt.Figure], list[dict]]:
    """Render the Table of Contents as one or more pages (matplotlib can't
    make text clickable itself — the returned link_specs are consumed later
    by _add_pdf_navigation(), which turns each row into a real PDF Link
    annotation once the pages have real, final page indices).

    entries: [(display title, anchor name), ...] in the order they should
    be listed and jumped to. page_numbers: anchor name -> final 1-based page
    number, already resolved by the caller before this is built (so the
    printed numbers are never a guess).
    """
    rows_per_page = _toc_rows_per_page()
    n_pages = max(1, -(-len(entries) // rows_per_page))  # ceil division

    figures: list[plt.Figure] = []
    row_records = []  # (fig, ax, title_artist, page_artist, row_top, row_bottom, anchor, page_offset)
    entry_idx = 0
    for page_offset in range(n_pages):
        fig, ax = _new_page()
        if page_offset == 0:
            ax.text(0.0, 0.98, "Table of Contents", transform=ax.transAxes, fontsize=17,
                    fontweight="bold", va="top", color=INK_PRIMARY)
            y = 0.98 - _leading(CONTENT_HEIGHT, 46)
        else:
            ax.text(0.0, 0.98, "Table of Contents (continued)", transform=ax.transAxes, fontsize=13,
                    fontweight="bold", va="top", color=INK_SECONDARY)
            y = 0.98 - _leading(CONTENT_HEIGHT, 34)

        row_leading = _leading(CONTENT_HEIGHT, TOC_ROW_LEADING_PTS)
        for title, anchor in entries[entry_idx: entry_idx + rows_per_page]:
            row_top = y
            indent = 0.035 if anchor.startswith("repository_") else 0.0
            title_artist = ax.text(indent, y, title, transform=ax.transAxes, fontsize=TOC_FONT_SIZE,
                                    color=INK_PRIMARY, va="top", ha="left")
            page_artist = ax.text(1.0, y, str(page_numbers.get(anchor, "")), transform=ax.transAxes,
                                   fontsize=TOC_FONT_SIZE, color=INK_PRIMARY, va="top", ha="right")
            row_bottom = y - row_leading
            row_records.append((fig, ax, title_artist, page_artist, row_top, row_bottom, anchor, page_offset))
            y = row_bottom
        entry_idx += rows_per_page
        figures.append(fig)

    # Dot leaders are drawn from each row's actual rendered text extents
    # (measured after a draw pass), the same draw-then-measure pattern this
    # file already uses to place Findings under a repository table — not a
    # guessed character count, which would misalign under a proportional font.
    for fig in figures:
        fig.canvas.draw()
    renderer_by_fig = {id(fig): fig.canvas.get_renderer() for fig in figures}

    link_specs: list[dict] = []
    for fig, ax, title_artist, page_artist, row_top, row_bottom, anchor, page_offset in row_records:
        renderer = renderer_by_fig[id(fig)]
        title_bbox = title_artist.get_window_extent(renderer)
        page_bbox = page_artist.get_window_extent(renderer)
        ax_bbox = ax.get_window_extent(renderer)
        pad_px = 4
        leader_x0 = (title_bbox.x1 + pad_px - ax_bbox.x0) / ax_bbox.width
        leader_x1 = (page_bbox.x0 - pad_px - ax_bbox.x0) / ax_bbox.width
        if leader_x1 > leader_x0:
            leader_y = row_top - _leading(CONTENT_HEIGHT, 11)  # roughly the text's visual mid-height
            ax.plot([leader_x0, leader_x1], [leader_y, leader_y], transform=ax.transAxes,
                    linestyle=(0, (1, 2)), color=INK_MUTED, linewidth=1, zorder=1)

        link_specs.append({
            "page_offset": page_offset,
            "rect": _axes_row_rect_to_pdf_points(row_top, row_bottom),
            "anchor": anchor,
        })

    return figures, link_specs


def _add_pdf_navigation(
    path: Path,
    anchor_page_numbers: dict[str, int],
    repo_ids_processed: list[int],
    toc_link_specs: list[dict],
    toc_start_page_num: int,
) -> None:
    """Layer clickable navigation onto the already-rendered PDF: named
    destinations and an outline/bookmark hierarchy for every section, plus a
    Link annotation on each Table-of-Contents row pointing at its target
    page. matplotlib's PdfPages backend has no API for any of this, so it
    runs as a pypdf post-process over the finished file — the pages
    themselves, their content, and their rendering are untouched; this only
    adds navigation metadata on top."""
    from pypdf import PdfReader, PdfWriter
    from pypdf.annotations import Link
    from pypdf.generic import Fit

    reader = PdfReader(str(path))
    writer = PdfWriter()
    for page in reader.pages:
        writer.add_page(page)
    if reader.metadata:
        writer.add_metadata(reader.metadata)

    for anchor, page_num in anchor_page_numbers.items():
        writer.add_named_destination(anchor, page_num - 1)

    writer.add_outline_item("Methodology", anchor_page_numbers["methodology"] - 1)
    writer.add_outline_item("Global Summary", anchor_page_numbers["global_summary"] - 1)
    if repo_ids_processed:
        first_repo_idx = anchor_page_numbers[_repo_anchor(repo_ids_processed[0])] - 1
        repositories_parent = writer.add_outline_item("Repositories", first_repo_idx)
        for repo_id in repo_ids_processed:
            repo_idx = anchor_page_numbers[_repo_anchor(repo_id)] - 1
            writer.add_outline_item(f"Repository {repo_id}", repo_idx, parent=repositories_parent)
    writer.add_outline_item("Limitations", anchor_page_numbers["limitations"] - 1)

    for spec in toc_link_specs:
        toc_page_index = (toc_start_page_num - 1) + spec["page_offset"]
        target_page_index = anchor_page_numbers[spec["anchor"]] - 1
        link = Link(rect=spec["rect"], target_page_index=target_page_index, fit=Fit.fit())
        writer.add_annotation(page_number=toc_page_index, annotation=link)

    with open(path, "wb") as f:
        writer.write(f)


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
    print(f"Font family: {RESOLVED_FONT_FAMILY}")
    for _variant_name in ("regular", "bold", "italic", "bolditalic"):
        _variant_file = RESOLVED_FONT_VARIANTS.get(_variant_name)
        if _variant_file:
            print(f"  {_variant_name:<10}: {_variant_file}")

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

    # Scoped for the whole build+save pass (not just rcParams.update() at
    # import time) so this module's font/style settings win regardless of
    # what other matplotlib-using modules import before or after it — see
    # REPORT_RC's comment above.
    with plt.rc_context(REPORT_RC):
        # --- Pass 1: build every content page except the TOC itself, so the
        # exact page count of every section (methodology, global summary,
        # each repository, limitations) is known from real rendered content
        # rather than assumed. ---
        title_fig = build_title_page(db_path, generated_at, global_stats)
        methodology_fig = build_methodology_page(preferred_method, fallback_method)
        global_summary_figs = build_global_summary_pages(global_stats, titles, preferred_method, fallback_method)

        repo_ids_processed: list[int] = []
        repo_figs_by_id: dict[int, list[plt.Figure]] = {}
        for repo_id in sorted(by_repo.keys()):
            repo_figs_by_id[repo_id] = build_repository_pages(repo_id, by_repo[repo_id], titles, top_n)
            repo_ids_processed.append(repo_id)

        limitations_fig = build_limitations_page()

        content_sections: list[tuple[str, list[plt.Figure]]] = [
            ("methodology", [methodology_fig]),
            ("global_summary", global_summary_figs),
        ]
        for repo_id in repo_ids_processed:
            content_sections.append((_repo_anchor(repo_id), repo_figs_by_id[repo_id]))
        content_sections.append(("limitations", [limitations_fig]))

        toc_entries: list[tuple[str, str]] = [("Methodology", "methodology"), ("Global Summary", "global_summary")]
        toc_entries += [(f"Repository {repo_id}", _repo_anchor(repo_id)) for repo_id in repo_ids_processed]
        toc_entries.append(("Limitations", "limitations"))

        # Required order is title, TOC, then content — the TOC's own page
        # count depends only on how many entries it lists (known already),
        # not on anything the TOC page itself renders, so this "second pass"
        # is a direct calculation rather than a guess-and-retry loop.
        rows_per_toc_page = _toc_rows_per_page()
        n_toc_pages = max(1, -(-len(toc_entries) // rows_per_toc_page))
        toc_start_page_num = 2  # title is page 1
        first_content_page_num = toc_start_page_num + n_toc_pages

        anchor_page_numbers: dict[str, int] = {}
        running = first_content_page_num
        for name, figs in content_sections:
            anchor_page_numbers[name] = running
            running += len(figs)
        total_pages = running - 1

        # --- Pass 2: now that every section's final page number is known,
        # render the actual TOC content (never a guessed number). ---
        toc_figs, toc_link_specs = build_toc_pages(toc_entries, anchor_page_numbers)

        pages: list[plt.Figure] = [title_fig, *toc_figs]
        for _name, figs in content_sections:
            pages.extend(figs)
        assert len(pages) == total_pages

        with tempfile.TemporaryDirectory() as _tmp_dir:
            # Any ad-hoc chart preview during generation would be written
            # under _tmp_dir and discarded when this block exits; the PDF
            # pages below are written directly (vector) into the final
            # PdfPages output, so no intermediate raster files are produced
            # in the normal path.
            with PdfPages(out_path) as pdf:
                for i, fig in enumerate(pages, start=1):
                    if i > 1:  # title page (1) keeps no footer; every other page gets one
                        _add_footer(fig, i, total_pages)
                    pdf.savefig(fig)
                    plt.close(fig)

                info = pdf.infodict()
                info["Title"] = "QDArchive Project Classification Report"
                info["Subject"] = f"Student {STUDENT_ID} — ISIC Rev. 5 classification statistics"

    # matplotlib's PdfPages has no concept of internal links, named
    # destinations, or an outline/bookmark sidebar — those are layered onto
    # the already-saved file as a pypdf post-process rather than requiring a
    # different rendering engine for any of the content above.
    _add_pdf_navigation(out_path, anchor_page_numbers, repo_ids_processed, toc_link_specs, toc_start_page_num)

    return {
        "output_path": out_path,
        "page_count": total_pages,
        "repo_ids_processed": repo_ids_processed,
        "global_stats": global_stats,
        "by_repo": by_repo,
        "titles": titles,
        "anchor_page_numbers": anchor_page_numbers,
        "toc_entries": toc_entries,
        "toc_start_page_num": toc_start_page_num,
        "n_toc_pages": n_toc_pages,
    }


def _flatten_outline_titles(outline) -> set[str]:
    """pypdf's .outline is a nested list (sub-lists for nested bookmarks);
    flatten it to the set of every title present at any level."""
    titles: set[str] = set()
    for item in outline:
        if isinstance(item, list):
            titles |= _flatten_outline_titles(item)
        else:
            titles.add(item.get("/Title", ""))
    return titles


def _pdf_font_resource_names(path: Path) -> set[str]:
    """The /BaseFont name of every font resource referenced across every
    page of the saved PDF (subset tag prefixes like 'ABCDEF+' stripped).
    With pdf.fonttype=42 these are the font's real PostScript/family names
    rather than matplotlib's generic Type 3 placeholder names, so this is
    what lets validate() confirm the resolved family was actually embedded
    rather than merely applied in memory."""
    from pypdf import PdfReader

    names: set[str] = set()
    reader = PdfReader(str(path))
    for page in reader.pages:
        resources = page.get("/Resources")
        if not resources:
            continue
        fonts = resources.get("/Font")
        if not fonts:
            continue
        for font_ref in fonts.values():
            font_obj = font_ref.get_object()
            base_font = font_obj.get("/BaseFont")
            if not base_font:
                continue
            name = str(base_font).lstrip("/")
            if "+" in name and name.split("+", 1)[0].isalnum() and len(name.split("+", 1)[0]) == 6:
                name = name.split("+", 1)[1]
            names.add(name)
    return names


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

    anchor_page_numbers: dict[str, int] = result.get("anchor_page_numbers", {})
    toc_entries: list[tuple[str, str]] = result.get("toc_entries", [])
    toc_start_page_num: int | None = result.get("toc_start_page_num")
    n_toc_pages: int = result.get("n_toc_pages", 1)

    if actual_pages is not None and toc_start_page_num is not None:
        try:
            toc_first_page_text = reader.pages[toc_start_page_num - 1].extract_text() or ""
        except Exception as exc:
            add("page 2 contains the Table of Contents", False, str(exc))
        else:
            add(
                "page 2 contains the Table of Contents", toc_start_page_num == 2 and
                "Table of Contents" in toc_first_page_text,
                f"heading found on page {toc_start_page_num}" if "Table of Contents" in toc_first_page_text
                else "heading not found",
            )

        add(
            "every TOC entry has a resolved page number",
            len(toc_entries) > 0 and all(anchor in anchor_page_numbers for _title, anchor in toc_entries),
            f"{len(toc_entries)} entries",
        )

        expected_repo_anchors = {_repo_anchor(rid) for rid in result["repo_ids_processed"]}
        toc_repo_anchors = {anchor for _title, anchor in toc_entries if anchor.startswith("repository_")}
        add(
            "no repository sections missing from the TOC",
            expected_repo_anchors == toc_repo_anchors,
            f"{len(toc_repo_anchors)} in TOC vs {len(expected_repo_anchors)} repositories",
        )

        try:
            outline_titles = _flatten_outline_titles(reader.outline)
            expected_top_level = {"Methodology", "Global Summary", "Limitations"}
            if result["repo_ids_processed"]:
                expected_top_level.add("Repositories")
                expected_top_level |= {f"Repository {rid}" for rid in result["repo_ids_processed"]}
            add(
                "PDF bookmarks/outline present for every section",
                expected_top_level.issubset(outline_titles),
                f"missing: {sorted(expected_top_level - outline_titles)}" if not expected_top_level.issubset(outline_titles)
                else f"{len(outline_titles)} outline entries",
            )
        except Exception as exc:
            add("PDF bookmarks/outline present for every section", False, str(exc))

        try:
            rows_per_toc_page = _toc_rows_per_page()
            total_links = 0
            mismatched: list[str] = []
            entry_idx = 0
            for page_offset in range(n_toc_pages):
                page = reader.pages[(toc_start_page_num - 1) + page_offset]
                annots = page.get("/Annots") or []
                link_annots = [a.get_object() for a in annots if a.get_object().get("/Subtype") == "/Link"]
                total_links += len(link_annots)
                for (_title, anchor), annot in zip(toc_entries[entry_idx:entry_idx + rows_per_toc_page], link_annots):
                    dest = annot.get("/Dest")
                    landed_page_idx = int(dest[0]) if dest else None
                    if landed_page_idx != anchor_page_numbers.get(anchor, -1) - 1:
                        mismatched.append(anchor)
                entry_idx += rows_per_toc_page
            add("every TOC entry is a clickable link", total_links == len(toc_entries),
                f"{total_links} link annotations for {len(toc_entries)} TOC entries")
            add("each TOC link lands on the correct section", len(mismatched) == 0,
                "all correct" if not mismatched else f"mismatched: {mismatched}")
        except Exception as exc:
            add("every TOC entry is a clickable link", False, str(exc))
            add("each TOC link lands on the correct section", False, str(exc))

        try:
            methodology_page_num = anchor_page_numbers["methodology"]
            methodology_text = reader.pages[methodology_page_num - 1].extract_text() or ""
            expected_footer = f"Page {methodology_page_num} of {result['page_count']}"
            add(
                "footer page numbers match TOC page numbers",
                expected_footer in methodology_text,
                f"looked for '{expected_footer}' on page {methodology_page_num}",
            )
        except Exception as exc:
            add("footer page numbers match TOC page numbers", False, str(exc))

        try:
            mediabox = reader.pages[0].mediabox
            width_in = float(mediabox.width) / 72
            height_in = float(mediabox.height) / 72
            add(
                "A4 page dimensions preserved",
                abs(width_in - PAGE_SIZE[0]) < 0.01 and abs(height_in - PAGE_SIZE[1]) < 0.01,
                f"{width_in:.2f}in x {height_in:.2f}in",
            )
        except Exception as exc:
            add("A4 page dimensions preserved", False, str(exc))

    try:
        font_names = _pdf_font_resource_names(out_path)
        normalized_target = RESOLVED_FONT_FAMILY.replace(" ", "").lower()
        matched = {n for n in font_names if normalized_target in n.replace(" ", "").replace("-", "").lower()}
        add(
            "resolved font family is embedded/referenced in the PDF",
            len(matched) > 0,
            f"looked for '{RESOLVED_FONT_FAMILY}' among embedded fonts: {sorted(font_names)}",
        )
    except Exception as exc:  # defensive: validation must not crash the run
        add("resolved font family is embedded/referenced in the PDF", False, str(exc))

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

    excluded_present = set(result["by_repo"].keys()) & set(EXCLUDED_REPOSITORY_IDS)
    add(
        "repository_id 99 absent from final outputs",
        len(excluded_present) == 0,
        "0 rows" if not excluded_present else f"{len(excluded_present)} rows present",
    )

    return checks


def write_validation_report(checks: list[dict], path: str | Path = VALIDATION_REPORT) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
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
