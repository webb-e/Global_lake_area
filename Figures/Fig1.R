########
### This code takes the annual median lake areas, generates annual total, mean, and median annual lake areas, 
### and produces a global figure of the trends of these variables over time as well as
### how they relate to observation frequency.
###
### Last updated Sept 30, 2026 by E. Webb
########

library(arrow)
library(tidyverse)
library(data.table)
library(scales)
library(patchwork)
library(EnvStats)

#==========================
# ===== read in files
#==========================
parquet_path <- "/Users/elizabethwebb/Library/CloudStorage/Box-Box/Landsat8/annual_lake_medians_dataset"
parquet_files <- list.files(parquet_path, pattern = "\\.parquet$", full.names = TRUE, recursive = TRUE)

read_filtered <- function(path, ds_name) {
  open_dataset(path) %>%
    filter(dataset == ds_name) %>%
    collect() %>%
    as.data.table()
}

gswo_dt <- rbindlist(lapply(parquet_files, read_filtered, ds_name = "GSWO"), use.names = TRUE, fill = TRUE)
glad_dt <- rbindlist(lapply(parquet_files, read_filtered, ds_name = "GLAD"), use.names = TRUE, fill = TRUE)

#==========================
# ===== global summaries
#==========================
summarise_global <- function(dt) {
  dt[, .(
    median_water_median = median(median_water, na.rm = TRUE),
    median_water_mean   = mean(median_water, na.rm = TRUE),
    median_water_sum    = sum(median_water, na.rm = TRUE),
    n_obs_median        = median(n_obs, na.rm = TRUE),
    n_obs_mean          = mean(n_obs, na.rm = TRUE),
    n_obs_sum           = sum(n_obs, na.rm = TRUE)
  ), by = .(dataset, year)][, climate_zone := "Global"]
}

df <- rbind(summarise_global(glad_dt), summarise_global(gswo_dt))

df[, total_lake_area  := median_water_sum / 1e6]
df[, median_lake_area := median_water_median / 1e6]
df[, mean_lake_area   := median_water_mean / 1e6]

vars_to_melt <- c("total_lake_area", "n_obs_sum", "median_lake_area", "n_obs_median", "mean_lake_area", "n_obs_mean")
plot_dt <- melt(df, id.vars = c("dataset", "year"),
                measure.vars = vars_to_melt, variable.name = "metric", value.name = "value")

#==========================
# ===== statistics for panel labels
#==========================
fmt_p <- function(p) ifelse(p < 0.001, "p < 0.001", paste0("p = ", signif(p, 2)))

### plotmath helper (scientific notation for slopes)
sci_expr <- function(x) {
  e <- floor(log10(abs(x)))
  e_txt <- sub("-", "\u2212", as.character(e))   # true minus sign for negative exponents
  ifelse(x == 0, '"0"',
         paste0('"', sprintf("%.1f", x / 10^e), ' \u00d7 10"^bold("', e_txt, '")'))
}

### label placement: GSWO stacked in upper left, GLAD stacked in lower right

step <- 0.11   # vertical spacing between stacked lines (fraction of panel height)

npc_to_data <- function(v, npc) {
  r  <- range(v, na.rm = TRUE)
  lo <- r[1] - 0.05 * diff(r)   
  hi <- r[2] + 0.05 * diff(r)
  lo + npc * (hi - lo)
}

place_lines <- function(dt, xvar) {
  dt[, N   := .N, by = .(dataset, metric)]
  dt[, xpc := ifelse(dataset == "GSWO", 0.02, 0.98)]
  dt[, ypc := ifelse(dataset == "GSWO", 0.93 - step * (pos - 1), 0.07 + step * (N - pos))]
  dt[, hj  := ifelse(dataset == "GSWO", 0, 1)]
  dt[, `:=`(x = npc_to_data(df[[xvar[[.BY$metric]]]], xpc),
            y = npc_to_data(df[[.BY$metric]], ypc)), by = metric]
  dt[]
}

### Mann-Kendall tau, Theil-Sen slope, MK p-value (vs year)
trend_vars <- data.table(
  metric = c("total_lake_area", "mean_lake_area", "median_lake_area",
             "n_obs_sum", "n_obs_mean", "n_obs_median"))

trend_lab <- rbindlist(lapply(c("GLAD", "GSWO"), function(ds) {
  rbindlist(lapply(seq_len(nrow(trend_vars)), function(i) {
    v   <- trend_vars$metric[i]
    res <- kendallTrendTest(reformulate("year", v), data = df[dataset == ds])
    data.table(dataset = ds, metric = v,
               tau   = unname(res$estimate[["tau"]]),
               slope = unname(res$estimate[["slope"]]),
               p     = res$p.value)
  }))
}))

trend_lines <- place_lines(trend_lab[, .(
  line = c(paste0('"\u03c4 = ', sprintf("%.2f", tau), '"'),
           paste0('"s = " * ', sci_expr(slope)),
           paste0('"', fmt_p(p), '"')),
  pos  = 1:3), by = .(dataset, metric)],
  xvar = setNames(as.list(rep("year", 6)), trend_vars$metric))

### Pearson r and p-value (lake area vs observations)
cor_pairs <- list(c("n_obs_sum", "total_lake_area"),
                  c("n_obs_mean", "mean_lake_area"),
                  c("n_obs_median", "median_lake_area"))
cor_lab <- rbindlist(lapply(c("GLAD", "GSWO"), function(ds) {
  d <- df[dataset == ds]
  rbindlist(lapply(cor_pairs, function(pr) {
    ct <- cor.test(d[[pr[1]]], d[[pr[2]]], method = "pearson")
    data.table(dataset = ds, metric = pr[2],
               r = unname(ct$estimate),
               p = ct$p.value)
  }))
}))

cor_lines <- place_lines(cor_lab[, .(
  line = c(paste0('r == "', sprintf("%.2f", r), '"'),
           paste0('"', fmt_p(p), '"')),
  pos  = 1:2), by = .(dataset, metric)],
  xvar = list(total_lake_area = "n_obs_sum", mean_lake_area = "n_obs_mean", median_lake_area = "n_obs_median"))

#==========================
# ===== Plots!
#==========================
basesize  = 30
pointsize = 5
textsize  = 30
labsize   = 6.5    

stat_label <- function(lab) {
  geom_text(data = lab, aes(x = x, y = y, label =paste0('bold(', line, ' * vphantom(p^"1"))'), color = dataset, hjust = hj),
            vjust = 0.5, size = labsize, fontface = "bold", parse = TRUE,
            inherit.aes = FALSE, show.legend = FALSE)
}

### total lake area
LT <- ggplot(plot_dt[metric == 'total_lake_area'],
             aes(x = year, y = value, color = dataset)) +
  geom_vline(xintercept = 2012.5, linetype = "dotted", color='grey45') +
  facet_wrap(~metric, labeller = labeller(metric = c(total_lake_area = "Total"))) +
  geom_point(size = pointsize) +
  stat_label(trend_lines[metric == "total_lake_area"]) +
  scale_color_manual(values = c("GLAD" = "#2a5674", "GSWO" = "#68abb8")) +
  scale_y_continuous(labels = label_number(scale_cut = cut_short_scale())) +
  theme_bw(basesize) +
  theme(legend.position = "bottom",
        legend.justification = "center",
        legend.title = element_blank(),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        strip.text = element_text(size = textsize, face = "bold")) +
  ylab('Lake area (km²)') + xlab('Year')

### mean lake area
MT <- ggplot(plot_dt[metric == 'mean_lake_area'],
             aes(x = year, y = value, color = dataset)) +
  geom_vline(xintercept = 2012.5, linetype = "dotted", color='grey45') +
  facet_wrap(~metric, labeller = labeller(metric = c(mean_lake_area = "Mean"))) +
  geom_point(size = pointsize) +
  stat_label(trend_lines[metric == "mean_lake_area"]) +
  scale_color_manual(values = c("GLAD" = "#2a5674", "GSWO" = "#68abb8")) +
  scale_y_continuous(labels = label_number(scale_cut = cut_short_scale())) +
  theme_bw(basesize) +
  theme(legend.position = "bottom",
        legend.justification = "center",
        legend.title = element_blank(),
        strip.text = element_text(size = textsize, face = "bold"),
        panel.grid.major = element_blank(), panel.grid.minor = element_blank()) +
  ylab('') + xlab('Year')

### median lake area
RT <- ggplot(plot_dt[metric == 'median_lake_area'],
             aes(x = year, y = value, color = dataset)) +
  geom_vline(xintercept = 2012.5, linetype = "dotted", color='grey45') +
  facet_wrap(~metric, labeller = labeller(metric = c(median_lake_area = "Median"))) +
  geom_point(size = pointsize) +
  stat_label(trend_lines[metric == "median_lake_area"]) +
  scale_color_manual(values = c("GLAD" = "#2a5674", "GSWO" = "#68abb8")) +
  scale_y_continuous(labels = label_number(scale_cut = cut_short_scale())) +
  theme_bw(basesize) +
  theme(legend.position = "bottom",
        legend.justification = "center",
        legend.title = element_blank(),
        strip.text = element_text(size = textsize, face = "bold"),
        panel.grid.major = element_blank(), panel.grid.minor = element_blank()) +
  ylab('') + xlab('Year')

### total number of observations
LM <- ggplot(plot_dt[metric == 'n_obs_sum'],
             aes(x = year, y = value, color = dataset)) +
  geom_vline(xintercept = 2012.5, linetype = "dotted", color='grey45') +
  geom_point(size = pointsize) +
  stat_label(trend_lines[metric == "n_obs_sum"]) +
  scale_color_manual(values = c("GLAD" = "#2a5674", "GSWO" = "#68abb8")) +
  scale_y_continuous(labels = label_number(scale_cut = cut_short_scale())) +
  theme_bw(basesize) +
  theme(legend.position = "bottom",
        legend.justification = "center",
        legend.title = element_blank(),
        panel.grid.major = element_blank(), panel.grid.minor = element_blank()) +
  ylab('Number of observations') + xlab('Year')

### mean number of observations
MM <- ggplot(plot_dt[metric == 'n_obs_mean'],
             aes(x = year, y = value, color = dataset)) +
  geom_vline(xintercept = 2012.5, linetype = "dotted", color='grey45') +
  geom_point(size = pointsize) +
  stat_label(trend_lines[metric == "n_obs_mean"]) +
  scale_color_manual(values = c("GLAD" = "#2a5674", "GSWO" = "#68abb8")) +
  scale_y_continuous(labels = label_number(scale_cut = cut_short_scale())) +
  theme_bw(basesize) +
  theme(legend.position = "bottom",
        legend.justification = "center",
        legend.title = element_blank(),
        panel.grid.major = element_blank(), panel.grid.minor = element_blank()) +
  ylab('') + xlab('Year')

### median number of observations
RM <- ggplot(plot_dt[metric == 'n_obs_median'],
             aes(x = year, y = value, color = dataset)) +
  geom_vline(xintercept = 2012.5, linetype = "dotted", color='grey45') +
  geom_point(size = pointsize) +
  stat_label(trend_lines[metric == "n_obs_median"]) +
  scale_color_manual(values = c("GLAD" = "#2a5674", "GSWO" = "#68abb8")) +
  scale_y_continuous(labels = label_number(scale_cut = cut_short_scale())) +
  theme_bw(basesize) +
  theme(legend.position = "bottom",
        legend.justification = "center",
        legend.title = element_blank(),
        panel.grid.major = element_blank(), panel.grid.minor = element_blank()) +
  ylab('') + xlab('Year')

### observations vs lake area (total)
LB <- ggplot(df, aes(x = n_obs_sum, y = total_lake_area, color = dataset)) +
  geom_point(size = pointsize) +
  stat_label(cor_lines[metric == "total_lake_area"]) +
  scale_color_manual(values = c("GLAD" = "#2a5674", "GSWO" = "#68abb8")) +
  scale_y_continuous(labels = label_number(scale_cut = cut_short_scale())) +
  scale_x_continuous(labels = label_number(scale_cut = cut_short_scale())) +
  theme_bw(basesize) +
  theme(legend.position = "bottom",
        legend.justification = "center",
        legend.title = element_blank(),
        panel.grid.major = element_blank(), panel.grid.minor = element_blank()) +
  ylab('Lake area (km²)') + xlab('Number of observations')

### observations vs lake area (mean)
MB <- ggplot(df, aes(x = n_obs_mean, y = mean_lake_area, color = dataset)) +
  geom_point(size = pointsize) +
  stat_label(cor_lines[metric == "mean_lake_area"]) +
  scale_color_manual(values = c("GLAD" = "#2a5674", "GSWO" = "#68abb8")) +
  scale_y_continuous(labels = label_number(scale_cut = cut_short_scale())) +
  scale_x_continuous(labels = label_number(scale_cut = cut_short_scale())) +
  theme_bw(basesize) +
  theme(legend.position = "bottom",
        legend.justification = "center",
        legend.title = element_blank(),
        panel.grid.major = element_blank(), panel.grid.minor = element_blank()) +
  xlab('Number of observations') + ylab(' ')

### observations vs lake area (median)
RB <- ggplot(df, aes(x = n_obs_median, y = median_lake_area, color = dataset)) +
  geom_point(size = pointsize) +
  stat_label(cor_lines[metric == "median_lake_area"]) +
  scale_color_manual(values = c("GLAD" = "#2a5674", "GSWO" = "#68abb8")) +
  scale_y_continuous(labels = label_number(scale_cut = cut_short_scale())) +
  scale_x_continuous(labels = label_number(scale_cut = cut_short_scale())) +
  theme_bw(basesize) +
  theme(legend.position = "bottom",
        legend.justification = "center",
        legend.title = element_blank(),
        panel.grid.major = element_blank(), panel.grid.minor = element_blank()) +
  xlab('Number of observations') + ylab(' ')

#==========================
# ===== combine
#==========================
final_plot <- (LT + MT + RT) / (LM + MM + RM) / (LB + MB + RB) +
  plot_layout(guides = "collect") &
  theme(legend.position = "bottom",
        legend.justification = "center",
        legend.text = element_text(size = textsize * 0.9))

ggsave(file.path(" ",
                 "Fig1.png"), final_plot, width = 20, height = 14, dpi = 300)



