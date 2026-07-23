import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import Patch

# ------------------------------------------------------------------
# Grid definitions (5x5, row 0 = top). W=water, L=land
# True lake: 3x3 water block, rows 1-3, cols 1-3.
# ------------------------------------------------------------------
TRUE = ["LLLLL", "LWWWL", "LWWWL", "LWWWL", "LLLLL"]

def composite(a, b):
    out = []
    for r in range(5):
        row = ""
        for c in range(5):
            row += "W" if (a[r][c] == "W" or b[r][c] == "W") else "L"
        out.append(row)
    return out

# Row 1: image 1 shows short-term drying; image 2 shows short-term flooding
f_img1 = ["LLLLL", "LWWWL", "LWWLL", "LWWWL", "LLLLL"]
f_img2 = ["LLLLL", "LWWWL", "LWWWL", "LWWWL", "LLWWL"]

# Row 2: image 1 has an omission error (true water pixel classified as
# non-water); image 2 has a commission error (false water on the perimeter).
# The composite corrects the omission but retains the commission.
c_img1 = ["LLLLL", "LWWWL", "LWWWL", "LWWLL", "LLLLL"]
c_err1 = {(3, 3)}
c_img2 = ["LLWLL", "LWWWL", "LWWWL", "LWWWL", "LLLLL"]
c_err2 = {(0, 2)}

# Row 3: image 1 has an unmasked cloud -> lake pixels flagged clear and
# classified as non-water
u_img1 = ["LLLLL", "LLLWL", "LLLWL", "LWWWL", "LLLLL"]
u_img2 = TRUE
u_err = {(1, 1), (1, 2), (2, 1), (2, 2)}

# (label, img1, img2, errors keyed by column index 0-2, light-water cells
# in the recorded classification: water carried by a single scene)
rows = [
    ("Short-term\nflood & drying", f_img1, f_img2, {},
     {(2, 3), (4, 2), (4, 3)}),
    ("Omission &\ncommission\nerrors", c_img1, c_img2,
     {0: c_err1, 1: c_err2, 2: c_err2}, {(0, 2), (3, 3)}),
    ("Unmasked\ncloud", u_img1, u_img2, {0: u_err},
     {(1, 1), (1, 2), (2, 1), (2, 2)}),
]

# ------------------------------------------------------------------
# Styling
# ------------------------------------------------------------------
COLORS = {"W": "#3B6FB6", "V": "#85ACDB", "L": "#9DBB99"}
ERR = "#1A1A1A"  # misclassified-pixel outline (black; avoids red/green pairing)
plt.rcParams.update({
    # R's default plotting font (Helvetica/Arial); Liberation Sans is the
    # metric-compatible substitute available on this system
    "font.family": "sans-serif",
    "font.sans-serif": ["Arial", "Helvetica", "Liberation Sans",
                        "Nimbus Sans", "DejaVu Sans"],
    "font.size": 13,
})

fig = plt.figure(figsize=(7.0, 6.4))

pw, ph = 0.205, 0.21
bottoms = [0.67, 0.405, 0.14]
lefts = [0.185, 0.46, 0.775]
col_titles = ["First image\nof month", "Second image\nof month",
              "Recorded water\nclassification"]

def draw_grid(ax, grid, errors=(), light=()):
    ax.set_xlim(0, 5); ax.set_ylim(0, 5)
    ax.set_aspect("equal")
    ax.axis("off")
    for r in range(5):
        for c in range(5):
            v = grid[r][c]
            if v == "W" and (r, c) in light:
                v = "V"
            ax.add_patch(mpatches.Rectangle(
                (c, 4 - r), 1, 1, facecolor=COLORS[v],
                edgecolor="white", linewidth=0.8))
    for (r, c) in errors:
        ax.add_patch(mpatches.Rectangle(
            (c + 0.09, 4 - r + 0.09), 0.82, 0.82,
            facecolor="none", edgecolor=ERR,
            linewidth=2.0, linestyle=(0, (2.5, 1.5)), zorder=5))
    ax.add_patch(mpatches.Rectangle((0, 0), 5, 5, facecolor="none",
                                    edgecolor="#555555", linewidth=0.9))

for ri, (label, g1, g2, err_by_col, light) in enumerate(rows):
    bottom = bottoms[ri]
    grids = [g1, g2, composite(g1, g2)]
    for ci, grid in enumerate(grids):
        ax = fig.add_axes([lefts[ci], bottom, pw, ph])
        draw_grid(ax, grid, err_by_col.get(ci, ()),
                  light if ci == 2 else ())
        if ri == 0:
            ax.text(2.5, 5.5, col_titles[ci], ha="center", va="bottom",
                    fontsize=13)
    # row mechanism label
    fig.text(0.025, bottom + ph / 2, label, ha="left", va="center",
             fontsize=12.5, linespacing=1.4)
    # thin arrow between second image and result
    y = bottom + ph / 2
    fig.patches.append(mpatches.FancyArrow(
        0.69, y, 0.062, 0, transform=fig.transFigure,
        width=0.003, head_width=0.014, head_length=0.015,
        length_includes_head=True, facecolor="#333333", edgecolor="none",
    ))

# ------------------------------------------------------------------
# Legend (no frame)
# ------------------------------------------------------------------
legend_items = [
    Patch(facecolor=COLORS["W"], edgecolor="#999999", linewidth=0.5,
          label="Water"),
    Patch(facecolor=COLORS["L"], edgecolor="#999999", linewidth=0.5,
          label="Non-water"),
    Patch(facecolor=COLORS["V"], edgecolor="#999999", linewidth=0.5,
          label="Water (GSWO) or 50% water occurrence (GLAD)"),
    Patch(facecolor="none", edgecolor=ERR, linewidth=2.0,
          linestyle=(0, (2.5, 1.5)), label="Misclassified pixel"),
]
fig.legend(handles=legend_items, loc="lower center", ncol=2,
           frameon=False, fontsize=12, bbox_to_anchor=(0.5, 0.01),
           handlelength=1.3, handleheight=1.1, columnspacing=1.8)

fig.savefig("figure_composite.pdf")
fig.savefig("figure_composite_preview.png", dpi=200)
print("done")
