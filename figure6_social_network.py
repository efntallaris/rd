"""
Figure 6: Measurements of Social-Network under diurnal workload.
(a) and (b) show the latency and CPU statistics achieved by Autothrottle.
(c) and (d) demonstrate how Tower adjusts throttle targets in response to
time-varying workload for the two CPU usage groups (Appendix C).
"""

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

np.random.seed(42)

# ── Global style: match the paper ──────────────────────────
plt.rcParams.update({
    "font.family": "sans-serif",
    "font.size": 9,
    "axes.linewidth": 0.6,
    "xtick.direction": "out",
    "ytick.direction": "out",
    "xtick.major.size": 3,
    "ytick.major.size": 3,
    "xtick.major.width": 0.6,
    "ytick.major.width": 0.6,
    "xtick.top": True,       # ticks on all 4 sides
    "ytick.right": True,
    "axes.xmargin": 0.02,    # small margin like the paper
    "axes.ymargin": 0.05,
})

# Time axis: 0 to 60 minutes
t = np.linspace(0, 60, 300)

# --- Diurnal workload envelope ---
def diurnal_envelope(t, t_start=12, t_end=48, steepness=1.0):
    env = 1.0 / (1 + np.exp(-steepness * (t - t_start))) * \
          1.0 / (1 + np.exp(steepness * (t - t_end)))
    return env / env.max()

envelope = diurnal_envelope(t)

# ── (a) Latency (P99) — placeholder ───────────────────────
latency_base = 5.0
latency_peak = 25.0
latency_noise = np.random.normal(0, 1.5, len(t))
latency = latency_base + (latency_peak - latency_base) * envelope + latency_noise
latency = np.clip(latency, 2, 35)

# ── (b) CPU utilization — placeholder ─────────────────────
cpu_base = 10.0
cpu_peak = 65.0
cpu_noise = np.random.normal(0, 3, len(t))
cpu = cpu_base + (cpu_peak - cpu_base) * envelope + cpu_noise
cpu = np.clip(cpu, 5, 80)

# ── (c) Throttle target #1 ────────────────────────────────
throttle1_base = 0.05
throttle1_peak = 0.25
throttle1_noise = np.random.uniform(-0.06, 0.06, len(t))
spikes1 = np.zeros_like(t)
spike_idx1 = np.random.choice(
    np.where((t >= 12) & (t <= 48))[0], size=40, replace=False
)
spikes1[spike_idx1] = np.random.uniform(0.05, 0.15, len(spike_idx1))
throttle1 = throttle1_base + (throttle1_peak - throttle1_base) * envelope \
    + throttle1_noise * envelope + spikes1
throttle1 = np.clip(throttle1, 0.0, 0.32)
throttle1[t < 10] = np.random.uniform(0.01, 0.06, np.sum(t < 10))
throttle1[t > 50] = np.random.uniform(0.01, 0.06, np.sum(t > 50))

# ── (d) Throttle target #2 ────────────────────────────────
throttle2_base = 0.01
throttle2_peak = 0.04
throttle2_noise = np.random.uniform(-0.01, 0.01, len(t))
spikes2 = np.zeros_like(t)
spike_idx2 = np.random.choice(
    np.where((t >= 18) & (t <= 48))[0], size=25, replace=False
)
spikes2[spike_idx2] = np.random.uniform(0.03, 0.08, len(spike_idx2))
throttle2 = throttle2_base + (throttle2_peak - throttle2_base) * envelope \
    + throttle2_noise * envelope + spikes2
throttle2 = np.clip(throttle2, 0.0, 0.11)
throttle2[t < 15] = np.random.uniform(0.0, 0.02, np.sum(t < 15))
throttle2[t > 50] = np.random.uniform(0.0, 0.02, np.sum(t > 50))

# ── Plotting ───────────────────────────────────────────────
fig, axes = plt.subplots(2, 2, figsize=(7, 5))


def style_ax(ax, xlabel, ylabel, xticks, yticks, yfmt=None):
    """Apply the paper's axis style to a subplot."""
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_xticks(xticks)
    if yticks is not None:
        ax.set_yticks(yticks)
    if yfmt is not None:
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter(yfmt))
    # Detach bottom/left spines so origin labels don't overlap
    ax.spines["bottom"].set_position(("outward", 2))
    ax.spines["left"].set_position(("outward", 4))
    # Hide top/right spines (only ticks, no frame overlap)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.tick_params(top=False, right=False)


xt = [0, 12, 24, 36, 48, 60]

# (a) Latency
ax = axes[0, 0]
ax.plot(t, latency, "k-", linewidth=0.7)
ax.set_xlim(0, 60)
ax.set_ylim(0.0, 30)
style_ax(ax, "Time (min)", "P99 latency (ms)", xt, None)
ax.text(0.5, -0.32, "(a) P99 latency", transform=ax.transAxes,
        ha="center", fontsize=9)

# (b) CPU
ax = axes[0, 1]
ax.plot(t, cpu, "k-", linewidth=0.7)
ax.set_xlim(0, 60)
ax.set_ylim(0.0, 80)
style_ax(ax, "Time (min)", "CPU utilization (%)", xt, None)
ax.text(0.5, -0.32, "(b) CPU statistics", transform=ax.transAxes,
        ha="center", fontsize=9)

# (c) Throttle target #1
ax = axes[1, 0]
ax.plot(t, throttle1, "k-", linewidth=0.7)
ax.set_xlim(0, 60)
ax.set_ylim(0.0, 0.3)
style_ax(ax, "Time (min)", "Throttle target", xt,
         [0.0, 0.1, 0.2, 0.3], yfmt="%.1f")
ax.text(0.5, -0.32, "(c) Throttle target #1", transform=ax.transAxes,
        ha="center", fontsize=9)

# (d) Throttle target #2
ax = axes[1, 1]
ax.plot(t, throttle2, "k-", linewidth=0.7)
ax.set_xlim(0, 60)
ax.set_ylim(0.0, 0.10)
style_ax(ax, "Time (min)", "Throttle target", xt,
         [0.00, 0.05, 0.10], yfmt="%.2f")
ax.text(0.5, -0.32, "(d) Throttle target #2", transform=ax.transAxes,
        ha="center", fontsize=9)

plt.tight_layout(h_pad=2.5, w_pad=2.0)
plt.savefig("figure6_social_network.png", dpi=200, bbox_inches="tight")
plt.show()
