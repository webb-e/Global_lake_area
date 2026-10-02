#==============================================================================
# Percent change in lake area attributed to observation frequency, Landsat 8,
# changes in data acquisition (obs + Landsat 8), and year, for the three
# methods (unadjusted, complete-record lakes, composition-adjusted).
# Top row: by lake size class; bottom row: by climate zone. "All lakes" on
# the right of each panel.
#
# Reads the CSVs written by lake_area_models_all_methods.R:
#   {method}_all_lakes_obs-sum.csv, {method}_by_size_obs-sum.csv,
#   {method}_by_zone_obs-sum.csv
#
# Last updated by E. Webb Oct 2026
#==============================================================================

library(data.table)
library(ggplot2)
library(patchwork)
library(scales)
library(grid)

#==========================
# ===== settings
#==========================
csv_dir   <- '..'
RESP_PLOT <- "mean"          # "total", "mean", or "median"
size_brks <- switch(RESP_PLOT,
                    total  = c(-25, -10, -5, 0, 5, 10, 50, 100),
                    mean   = c(-25, -10, -5, 0, 5, 10, 50, 100),
                    median = c(-25, -10, -5, 0, 5, 10, 25))

ds_cols <- c("GLAD" = "#2a5674", "GSWO" = "#68abb8")
fntsize<-40
method_files <- c(`Full dataset`            = "unadjusted",
                  `Complete-record lakes` = "complete_record",
                  `Composition-adjusted`  = "composition_adjusted")
method_shape <- c(`Full dataset` = 15, `Complete-record lakes` = 17, `Composition-adjusted` = 16)

term_labs <- c(obs  = "Observation\nfrequency",
               era  = "Introduction of\nLandsat 8",
               acq  = "Changes in\ndata acquisition",
               year = "Temporal trend")

SIZE_BREAKS <- c(0, 0.1, 1, 10, 100, Inf)
ZONE_ORDER  <- c("Polar", "Continental", "Temperate", "Tropical", "Dry")
ALL_LABEL   <- "All lakes"

y_breaks <- c(-150, -100, -75, -50, -25, -10, -5, 0, 5, 10, 25, 50, 100, 150)

# size-class labels as written by the analysis script, and their axis labels
nsz <- length(SIZE_BREAKS) - 1
size_labs <- sprintf("%s-%s", SIZE_BREAKS[-(nsz + 1)], SIZE_BREAKS[-1])
size_labs[1]   <- sprintf("<%s", SIZE_BREAKS[2])
size_labs[nsz] <- sprintf(">%s", SIZE_BREAKS[nsz])

#==========================
# ===== load
#==========================
load_method <- function(grouping, grp_col) {
  cols <- c("dataset", "xg", "response", "term", "pct_area", "pct_area_lo", "pct_area_hi")
  rbindlist(lapply(names(method_files), function(mn) {
    f   <- method_files[[mn]]
    grp <- fread(file.path(csv_dir, sprintf("%s_by_%s_obs-sum.csv", f, grouping)), sep = ",")
    all <- fread(file.path(csv_dir, sprintf("%s_all_lakes_obs-sum.csv", f)), sep = ",")
    setnames(grp, grp_col, "xg")
    all[, xg := ALL_LABEL]
    rbind(grp[, ..cols], all[, ..cols])[, method := mn]
  }))
}

prep <- function(d, x_levels) {
  d <- d[response == RESP_PLOT & term %in% names(term_labs)]
  d[, xg := sub(" km2$", "", xg)]
  d[, `:=`(xg      = factor(xg, levels = x_levels),
           term    = factor(term, levels = names(term_labs), labels = term_labs),
           method  = factor(method, levels = names(method_files)),
           dataset = factor(dataset, levels = names(ds_cols)))]
  d[!is.na(xg)]
}

d_size <- prep(load_method("size", "group"), c(size_labs, ALL_LABEL))
d_zone <- prep(load_method("zone", "zone"),  c(ZONE_ORDER, ALL_LABEL))

#==========================
# ===== panels
#==========================
panel <- function(d, tm, lim, brks) {
  dd <- d[term == tm]
  half <- 0.8 * fntsize / 2                       # default strip margin
  pad  <- if (grepl("\n", tm)) half else half + 0.8 * fntsize * 0.9 / 2   # + half a line
  ggplot(dd, aes(x = xg, y = pct_area, colour = dataset, shape = method,
                 group = interaction(method, dataset, lex.order = TRUE))) +
    geom_hline(yintercept = 0, colour = "grey60", linewidth = 0.3) +
    geom_vline(xintercept = nlevels(d$xg) - 0.5, linetype = "dashed",
               colour = "grey45", linewidth = 0.8) +
    geom_pointrange(aes(ymin = pct_area_lo, ymax = pct_area_hi),
                    position = position_dodge(width = 0.8),
                    size = 1.2, linewidth = 0.8) +
    scale_colour_manual(values = ds_cols, name = NULL, drop = FALSE,
                        guide = guide_legend(order = 1,
                                             override.aes = list(shape = 16, linetype = 0))) +
    scale_shape_manual(values = method_shape, name = NULL, drop = FALSE,
                       guide = guide_legend(order = 2,
                                            override.aes = list(colour = "black"))) +
    scale_x_discrete(drop = FALSE) +
    scale_y_continuous(trans = pseudo_log_trans(sigma = 5), breaks = brks,
                       limits = lim) +
    facet_wrap(~ term) +
    labs(x = NULL, y = NULL) +
    theme_bw(base_size=fntsize) +
    theme(panel.grid  = element_blank(),
          strip.text  = element_text(face = "bold", margin = margin(pad, half, pad, half, unit = "pt")),
          axis.text.x = element_text(angle = 45, hjust = 1))
}

row_of <- function(d, brks) {
  lim <- range(c(d$pct_area, d$pct_area_lo, d$pct_area_hi), na.rm = TRUE)
  wrap_plots(lapply(term_labs, function(tm) panel(d, tm, lim, brks)), nrow = 1)
}

row_size <- row_of(d_size, brks = size_brks)
row_zone <- row_of(d_zone, brks = c(-50, -10, 0, 10, 50, 100))
x_size <- wrap_elements(textGrob("Lake size (km\u00b2)", gp = gpar(fontsize = fntsize)))
x_zone <- wrap_elements(textGrob("Climate zone", gp = gpar(fontsize = fntsize)))
y_lab <- wrap_elements(textGrob(sprintf("Effect on %s lake area (%% of 1999\u20132021 average)", RESP_PLOT),
                                rot = 90, gp = gpar(fontsize = fntsize)),clip = FALSE)
design <- "
AB
AC
AD
#E
"


fig <- wrap_plots(A = y_lab, B = row_size, C = x_size, D = row_zone, E = x_zone,
                  design = design) +
  plot_layout(widths = c(0.06, 1), heights = c(1, 0.12, 1, 0.12), guides = "collect") &
  theme(legend.position = "bottom")

ggsave(file.path("...",
                 sprintf("%s_coefficients.png", RESP_PLOT)), fig, width = 30, height = 20, dpi = 300)

