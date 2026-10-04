import numpy as np
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Ellipse
from matplotlib.legend_handler import HandlerBase
from scipy.interpolate import PchipInterpolator

BLUE = "#3B6FB6"
GRAY = "#AAAAAA"
GRAY_LT = "#DDDDDD"
DARK = "#333333"
GREEN = "#6E9169"
CLOUD_THRESH = 0.55

plt.rcParams.update({
    # R's default plotting font (Helvetica/Arial); Liberation Sans is the
    # metric-compatible substitute available on this system
    "font.family": "sans-serif",
    "font.sans-serif": ["Arial", "Helvetica", "Liberation Sans",
                        "Nimbus Sans", "DejaVu Sans"],
    "font.size": 11.5,
    "axes.linewidth": 0.8,
})

days = np.arange(0, 366)
t = np.linspace(0, 365, 1000)

#  hydrograph
_ctrl_x = [0, 60, 100, 125, 140, 155, 170, 190, 215, 245, 275, 300, 330, 365]
_ctrl_y = [62, 62, 61, 64, 74, 82, 80, 73, 65, 57, 59, 62, 62, 62]
_hydro = PchipInterpolator(_ctrl_x, _ctrl_y)

# jagged day-to-day variability 
_rng_h = np.random.default_rng(42)
_e = _rng_h.normal(0, 0.85, len(days))
_n = np.zeros(len(days))
for _i in range(1, len(days)):
    _n[_i] = 0.62 * _n[_i - 1] + _e[_i]
_area_series = _hydro(days.astype(float)) + _n

def area(x):
    return np.interp(np.asarray(x, dtype=float), days, _area_series)

y = _area_series
true_max = float(y.max())

# seasonal mean cloud cover, higher during the early-summer high-water
# period (cloud cover correlated with the hydrologic peak)
def cloud_mean(x):
    return 0.15 + 0.60 * np.exp(-((x - 165) / 48) ** 2)

# noisy daily cloud cover: AR(1) weather noise around seasonal mean.
def daily_clouds(seed):
    rng = np.random.default_rng(seed)
    e = rng.normal(0, 0.12, len(days))
    n = np.zeros(len(days))
    for i in range(1, len(days)):
        n[i] = 0.75 * n[i - 1] + e[i]
    return np.clip(cloud_mean(days) + n, 0, 1)

sparse_sched = np.arange(20, 366, 35)
dense_sched = np.arange(8, 366, 15)  # 2 acquisitions per month (max possible)

for seed in range(20000):
    dc = daily_clouds(seed)
    cloudy = dc > CLOUD_THRESH
    sparse_clear = sparse_sched[~cloudy[sparse_sched]]
    dense_ok = dense_sched[~cloudy[dense_sched]]
    wet = (sparse_sched > 130) & (sparse_sched < 215)
    # conditions: sparse misses the peak badly; dense gets close to it
    if (len(sparse_clear) >= 5
            and cloudy[sparse_sched[wet]].sum() >= 1
            and 66 < area(sparse_clear).max() < true_max - 9
            and true_max - 6 < area(dense_ok).max() < true_max - 0.8
            and 0.20 < cloudy.mean() < 0.45):
        break

sparse_clear = sparse_sched[~cloudy[sparse_sched]]
sparse_cloudy = sparse_sched[cloudy[sparse_sched]]
dense_clear = dense_sched[~cloudy[dense_sched]]
dense_cloudy = dense_sched[cloudy[dense_sched]]

# ------------------------------------------------------------------
fig, (ax_c, ax_a, ax_b) = plt.subplots(
    3, 1, figsize=(7.0, 6.3), sharex=True,
    gridspec_kw={"height_ratios": [1.5, 2.6, 2.6], "hspace": 0.42})
fig.subplots_adjust(left=0.09, right=0.97, top=0.88, bottom=0.16)

# --- cloud-cover strip ---
ax_c.fill_between(days, 0, dc, color=GRAY_LT, lw=0, step="mid")
ax_c.plot(days, dc, color=GRAY, lw=0.6)
ax_c.set_ylim(0, 1.02)
ax_c.set_yticks([])
ax_c.set_ylabel("Cloud percentage")
ax_c.set_title("Cloud cover", loc="center", fontsize=12.5, pad=14)
for s in ("top", "right"):
    ax_c.spines[s].set_visible(False)

# --- cloud symbol ---
from shapely.geometry import Point as ShpPoint, LineString
from shapely.ops import unary_union
from matplotlib.path import Path as MplPath
from matplotlib.patches import PathPatch

CLOUD_FC = "#D2D2D2"
CLOUD_EC = "#2B2B2B"

_cap = LineString([(-1.5, -0.3), (1.5, -0.3)]).buffer(0.68, resolution=48)
_b1 = ShpPoint(-0.8, 0.35).buffer(0.60, resolution=48)
_b2 = ShpPoint(0.45, 0.60).buffer(0.80, resolution=48)
_cloud = unary_union([_cap, _b1, _b2])
CLOUD_XY = np.array(_cloud.exterior.coords)
CLOUD_XY[:, 0] -= CLOUD_XY[:, 0].mean()
CLOUD_XY[:, 1] -= CLOUD_XY[:, 1].min()  # base at y=0

def draw_cloud(ax, x, y, sx=3.4, sy=1.45):
    verts = CLOUD_XY * [sx, sy] + [x, y - 0.8]
    ax.add_patch(PathPatch(MplPath(verts), facecolor=CLOUD_FC,
                           edgecolor=CLOUD_EC, lw=1.3, zorder=3,
                           joinstyle="round"))

class CloudHandler(HandlerBase):
    def create_artists(self, legend, orig_handle, xdescent, ydescent,
                       width, height, fontsize, trans):
        w = CLOUD_XY[:, 0].max() - CLOUD_XY[:, 0].min()
        h = CLOUD_XY[:, 1].max() - CLOUD_XY[:, 1].min()
        s = min(width / w, height / h) * 1.25
        verts = CLOUD_XY * s + [width / 2 - xdescent,
                                height / 2 - ydescent - s * h / 2]
        p = PathPatch(MplPath(verts), facecolor=CLOUD_FC,
                      edgecolor=CLOUD_EC, lw=1.1, joinstyle="round")
        p.set_transform(trans)
        return [p]

# --- observation panels ---
panels = [
    (ax_a, "Low observation frequency", sparse_clear, sparse_cloudy),
    (ax_b, "High observation frequency", dense_clear, dense_cloudy),
]
for ax, title, obs, missed in panels:
    ax.plot(days, y, color=DARK, lw=1.1, zorder=2)
    ax.axhline(true_max, color=GREEN, lw=1.0, ls=(0, (1, 1.5)), zorder=1)
    # cloud-obscured acquisitions: no water measurement that day
    for d in missed:
        draw_cloud(ax, float(d), float(area(d)))
    # successful cloud-free observations
    ax.plot(obs, area(obs), "o", color=BLUE, ms=7, mec="white", mew=0.8,
            zorder=4)
    max_obs = float(area(obs).max())
    ax.axhline(max_obs, color=BLUE, lw=1.1, ls="--", zorder=1)
    ax.annotate("maximum observed lake area", xy=(363, max_obs),
                ha="right", va="top", fontsize=10.5, color=BLUE,
                xytext=(0, -3), textcoords="offset points")
    ax.set_ylim(50, 92)
    ax.set_xlim(0, 365)
    ax.set_ylabel("Lake area")
    ax.set_yticks([])
    ax.set_title(title, loc="center", fontsize=12.5)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)

for _ax in (ax_a, ax_b):
    _ax.annotate("true maximum lake area", xy=(3, true_max), ha="left",
                 va="bottom", fontsize=10.5, color=GREEN, xytext=(0, 2),
                 textcoords="offset points")

# unlabeled time axis (one year)
ax_b.set_xticks([])
ax_b.set_xlabel("Time (one year)")

# legend: single row, no frame
cloud_proxy = PathPatch(MplPath(CLOUD_XY), facecolor=CLOUD_FC,
                        edgecolor=CLOUD_EC,
                        label="Cloud-obscured acquisition (no data)")
handles = [
    Line2D([], [], color=DARK, lw=1.4, label="True lake area"),
    Line2D([], [], marker="o", color="none", mfc=BLUE, mec="white",
           ms=7.5, label="Cloud-free observation"),
    cloud_proxy,
]
fig.legend(handles=handles, loc="lower center", ncol=3,
           frameon=False, fontsize=11, bbox_to_anchor=(0.5, 0.02),
           columnspacing=1.2, handlelength=1.6, handletextpad=0.6,
           handler_map={cloud_proxy: CloudHandler()})

fig.savefig("figure_cloudbias.pdf")
fig.savefig("figure_cloudbias_preview.png", dpi=200)
print("seed:", seed,
      "| sparse max:", round(float(area(sparse_clear).max()), 1),
      "| dense max:", round(float(area(dense_clear).max()), 1),
      "| true:", round(true_max, 1),
      "| cloudy frac:", round(float(cloudy.mean()), 2))
