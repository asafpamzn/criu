import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# Shared styling
BGSAVE_P50_COLOR = "#cf222e"   # red
BGSAVE_P99_COLOR = "#8b0000"   # dark red
DOLLY_P50_COLOR = "#0969da"    # blue
DOLLY_P99_COLOR = "#0b2b7f"    # dark blue

STEADY_P50 = 0.5   # ms
STEADY_P99 = 0.8   # ms


def style_axes(ax, x_max, title, ymin=0.3, ymax=60):
    ax.set_yscale("log")
    ax.set_ylim(ymin, ymax)
    ax.set_xlim(0, x_max)
    ax.grid(True, which="both", alpha=0.25, linewidth=0.6)
    ax.set_xlabel("Seconds since SAVE was called", fontsize=11)
    ax.set_ylabel("Latency (ms, log scale)", fontsize=11)
    ax.set_title(title, fontsize=13, pad=14)

    # Steady-state reference lines (labels centered)
    ax.axhline(STEADY_P50, color="#888", linestyle=":", linewidth=1, alpha=0.7)
    ax.text(x_max * 0.5, STEADY_P50, "steady state P50", va="center",
            ha="center", color="#555", fontsize=8.5, style="italic",
            bbox=dict(facecolor="white", edgecolor="none", pad=2))
    ax.axhline(STEADY_P99, color="#888", linestyle=":", linewidth=1, alpha=0.7)
    ax.text(x_max * 0.5, STEADY_P99, "steady state P99", va="center",
            ha="center", color="#555", fontsize=8.5, style="italic",
            bbox=dict(facecolor="white", edgecolor="none", pad=2))

    yticks = [0.5, 1, 2, 5, 10, 20, 50]
    ax.yaxis.set_major_locator(ticker.FixedLocator(yticks))
    ax.yaxis.set_major_formatter(ticker.FormatStrFormatter("%g"))

    # "SAVE post-freeze" annotation at t = 0
    ax.axvline(0, color="#333", linestyle="-", linewidth=1.2, alpha=0.55)
    ax.annotate(
        "t = 0: SAVE post-freeze\n(freeze window excluded)",
        xy=(0, 35), xycoords="data",
        xytext=(x_max * 0.08, 48), textcoords="data",
        fontsize=10, fontweight="bold", color="#222",
        ha="left", va="center",
        bbox=dict(boxstyle="round,pad=0.4", facecolor="#fff8c5",
                  edgecolor="#9a6700", linewidth=1),
        arrowprops=dict(arrowstyle="->", color="#9a6700",
                        linewidth=1.4, shrinkA=2, shrinkB=2),
    )


def annotate_recovery(ax, x, y_p50, y_p99, label, text_x, text_y, color):
    """Add two arrows pointing at the p50 and p99 recovery samples.
    Both arrows start from *below* the caption box: p99 from bottom-right,
    p50 from bottom-left. The P99 arrow is anchored to the annotation's
    bbox (so `relpos` works); the P50 arrow is drawn as a standalone
    FancyArrowPatch starting from an explicit point below the box."""
    from matplotlib.patches import FancyArrowPatch

    # Draw BOTH arrows first (at low zorder), then the caption box (at high
    # zorder) so the filled caption background covers the arrow shafts.
    # Arrows start just below the caption box so they visually emerge
    # from the box's bottom edge. The caption box (drawn last, below)
    # covers the top of the arrow shafts.
    x_span = ax.get_xlim()[1] - ax.get_xlim()[0]
    start_y = text_y * 0.78  # closer to the box (higher on log axis)

    # P99 arrow: exits from caption-bottom-right
    p99_start_x = text_x + x_span * 0.020
    p99_arrow = FancyArrowPatch(
        (p99_start_x, start_y), (x, y_p99),
        arrowstyle="->", color=color, linewidth=1.3,
        shrinkA=0, shrinkB=2, mutation_scale=12,
        zorder=1,
    )
    ax.add_patch(p99_arrow)

    # P50 arrow: exits from caption-bottom-left
    p50_start_x = text_x - x_span * 0.020
    p50_arrow = FancyArrowPatch(
        (p50_start_x, start_y), (x, y_p50),
        arrowstyle="->", color=color, linewidth=1.3,
        shrinkA=0, shrinkB=2, mutation_scale=12,
        zorder=1,
    )
    ax.add_patch(p50_arrow)

    # Caption box, drawn on top so its fill covers the arrow stubs that
    # would otherwise clip into the text.
    ax.text(
        text_x, text_y,
        f"{label}\nP50 & P99 back to\nsteady state",
        fontsize=9.5, fontweight="bold", color="#111",
        ha="center", va="center", zorder=10,
        bbox=dict(boxstyle="round,pad=0.35", facecolor="#ddf4ff",
                  edgecolor=color, linewidth=1),
    )


def plot_scenario(filename, title, bgsave, dolly, x_max,
                  dolly_recovery, bgsave_recovery):
    fig, ax = plt.subplots(figsize=(10, 5.2), dpi=150)

    bg_t, bg_p50, bg_p99 = bgsave
    d_t, d_p50, d_p99 = dolly

    ax.plot(bg_t, bg_p99, color=BGSAVE_P99_COLOR, linewidth=2.2,
            marker="o", markersize=4, label="BGSAVE p99")
    ax.plot(bg_t, bg_p50, color=BGSAVE_P50_COLOR, linewidth=2.0,
            marker="o", markersize=4, linestyle="--", label="BGSAVE p50")
    ax.plot(d_t, d_p99, color=DOLLY_P99_COLOR, linewidth=2.2,
            marker="s", markersize=4, label="DollySave p99")
    ax.plot(d_t, d_p50, color=DOLLY_P50_COLOR, linewidth=2.0,
            marker="s", markersize=4, linestyle="--", label="DollySave p50")

    style_axes(ax, x_max, title)
    ax.legend(loc="upper right", frameon=True, framealpha=0.95, fontsize=10)

    # Recovery callouts — label-only (no timestamp), positioned well above the lines
    d_x, d_p50_val, d_p99_val = dolly_recovery
    annotate_recovery(
        ax, d_x, d_p50_val, d_p99_val,
        "DollySave",
        text_x=d_x + x_max * 0.03, text_y=18,
        color=DOLLY_P99_COLOR,
    )
    bg_x, bg_p50_val, bg_p99_val = bgsave_recovery
    annotate_recovery(
        ax, bg_x, bg_p50_val, bg_p99_val,
        "BGSAVE",
        text_x=bg_x - x_max * 0.05, text_y=9,
        color=BGSAVE_P99_COLOR,
    )

    fig.tight_layout()
    fig.savefig(filename, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {filename}")


# -----------------------------------------------------------------------------
# Scenario 2 — 400K GET + 1.5K SET
# t = 0 is SAVE called; freeze-window samples excluded.
# -----------------------------------------------------------------------------
s2_bg_t   = [0,    1,    2,    3,    4,    5,    6,    7,    8,    9,    11,   12,   13,   17,   18,   19]
s2_bg_p50 = [4.53, 4.36, 4.12, 3.94, 3.82, 3.65, 3.52, 3.38, 3.25, 3.09, 2.83, 2.65, 2.60, 2.07, 0.45, 0.43]
s2_bg_p99 = [5.01, 4.95, 4.76, 4.36, 4.62, 4.10, 4.00, 3.81, 3.65, 3.52, 3.34, 3.21, 3.13, 2.44, 2.16, 0.86]

# DollySave line ends at t+3, where both p50 and p99 have reached steady state.
s2_d_t    = [0,    1,    2,    3]
s2_d_p50  = [2.17, 2.07, 2.03, 0.41]
s2_d_p99  = [2.66, 2.58, 2.39, 0.70]

plot_scenario(
    "/Users/asafp/work/valkey/dollysave_scenario2_latency.png",
    "Scenario 2 — 400K GET + 1.5K SET: p50/p99 recovery after SAVE",
    (s2_bg_t, s2_bg_p50, s2_bg_p99),
    (s2_d_t, s2_d_p50, s2_d_p99),
    x_max=22,
    dolly_recovery=(3, 0.41, 0.70),
    bgsave_recovery=(19, 0.43, 0.86),
)

# -----------------------------------------------------------------------------
# Scenario 3 — 400K GET + 100K SET
# BGSAVE trace is from the restart after OOM crash.
# -----------------------------------------------------------------------------
s3_bg_t   = [0,    1,    2,    3,    4,    5,    7,    10,   13,   15,   20,   25,   30,   35,   40,   45,   48,   49]
s3_bg_p50 = [6.01, 5.85, 5.65, 5.50, 5.25, 5.18, 4.87, 4.44, 4.19, 3.93, 3.34, 3.02, 2.79, 2.56, 2.47, 2.41, 0.45, 0.43]
s3_bg_p99 = [6.62, 6.52, 6.29, 6.24, 5.74, 5.69, 5.44, 4.99, 4.75, 4.45, 3.75, 3.47, 3.50, 2.98, 2.96, 2.83, 1.93, 0.82]

# DollySave line ends at t+4, where both p50 and p99 have reached steady state.
s3_d_t    = [0,    1,    2,    3,    4]
s3_d_p50  = [2.19, 2.14, 2.09, 2.04, 0.40]
s3_d_p99  = [2.54, 2.56, 2.48, 2.39, 0.73]

plot_scenario(
    "/Users/asafp/work/valkey/dollysave_scenario3_latency.png",
    "Scenario 3 — 400K GET + 100K SET: p50/p99 recovery after SAVE",
    (s3_bg_t, s3_bg_p50, s3_bg_p99),
    (s3_d_t, s3_d_p50, s3_d_p99),
    x_max=52,
    dolly_recovery=(4, 0.40, 0.73),
    bgsave_recovery=(49, 0.43, 0.82),
)
