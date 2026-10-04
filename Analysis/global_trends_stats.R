#==============================================================================
# Global (and climate-zone) lake area and observation-frequency trends:
# numbers for the "Without accounting for any observational biases..." results
# paragraph, Supplementary Table 1 (Mann-Kendall / Theil-Sen) and
# Supplementary Table 2 (Pearson correlations).
#==============================================================================
library(arrow)
library(dplyr)
library(data.table)
library(EnvStats)
library(flextable)

#==========================
# ===== settings
#==========================
parquet_path <- "/Users/elizabethwebb/Library/CloudStorage/Box-Box/Landsat8/annual_lake_medians_dataset"
YEARS        <- 1999:2021
DATASETS     <- c("GSWO", "GLAD")
ZONE_LABELS  <- c(`1` = "Tropical", `2` = "Dry", `3` = "Temperate", `4` = "Continental", `5` = "Polar")

# The results paragraph defines mean observation frequency as "including lakes
# with no valid observations in a given year". TRUE = observation metrics use
# every lake-year (zeros included); FALSE = only lake-years with >= 1 valid obs.
# Lake area metrics always use lake-years with >= 1 valid observation.
OBS_INCLUDE_ZERO <- TRUE

area_vars <- c(total = "total_lake_area", mean = "mean_lake_area", median = "median_lake_area")
obs_vars  <- c(total = "n_obs_sum",       mean = "n_obs_mean",     median = "n_obs_median")

#==========================
# ===== read
#==========================
parquet_files <- list.files(parquet_path, pattern = "^[^.].*\\.parquet$", full.names = TRUE, recursive = TRUE)

read_ds <- function(ds) {
  rbindlist(lapply(parquet_files, function(p)
    open_dataset(p) %>%
      filter(dataset == ds, year >= !!min(YEARS), year <= !!max(YEARS)) %>%
      select(dataset, year, climate_zone, n_obs, median_water) %>%
      collect() %>% as.data.table()),
    use.names = TRUE, fill = TRUE)
}
raw <- rbindlist(lapply(DATASETS, read_ds))
raw[, zone := ZONE_LABELS[as.character(climate_zone)]]   # 6 / unclassified -> NA

#==========================
# ===== annual aggregation
#==========================
# one grouped pass, no subset copies of raw (keeps memory down)
agg <- function(dt, by) {
  dt[, {
    v  <- n_obs > 0 & is.finite(median_water)          # valid lake-years
    w  <- median_water[v]
    no <- if (OBS_INCLUDE_ZERO) n_obs else n_obs[v]
    list(total_lake_area  = sum(w)    / 1e6,
         mean_lake_area   = mean(w)   / 1e6,
         median_lake_area = as.numeric(median(w)) / 1e6,
         n_obs_sum        = as.numeric(sum(no, na.rm = TRUE)),
         n_obs_mean       = as.numeric(mean(no, na.rm = TRUE)),
         n_obs_median     = as.numeric(median(no, na.rm = TRUE)))
  }, by = by]
}

glob  <- agg(raw, c("dataset", "year"))[, zone := "Global"]
gc()
zones <- agg(raw, c("dataset", "zone", "year"))[!is.na(zone)]   # NA zone group dropped after
ann   <- rbind(glob, zones, use.names = TRUE)
setorder(ann, dataset, zone, year)
rm(raw); gc()

#==========================
# ===== per-series statistics
#==========================
series_stats <- function(y, yr) {
  mk  <- kendallTrendTest(y = y, x = yr)
  s   <- unname(mk$estimate[["slope"]])
  pre <- mean(y[yr < 2013]); post <- mean(y[yr >= 2013])
  data.table(tau      = unname(mk$estimate[["tau"]]),
             slope    = s,
             slope_lo = unname(mk$interval$limits[1]),
             slope_hi = unname(mk$interval$limits[2]),
             z        = unname(mk$statistic),
             p        = mk$p.value,
             n_years  = length(y),
             pct_record = 100 * s * (max(yr) - min(yr)) / mean(y),   # % change over record
             pct_L8     = 100 * (post - pre) / pre)                    # post- vs pre-2013
}

all_vars <- c(area_vars, obs_vars)
stats <- ann[, rbindlist(lapply(all_vars, function(v)
  cbind(variable = v, series_stats(.SD[[v]], year)))), by = .(dataset, zone)]
stats[, `:=`(type   = ifelse(variable %in% area_vars, "lake area", "observations"),
             metric = sub("^n_obs_", "", sub("_lake_area$", "", variable)))]
stats[metric == "sum", metric := "total"]

#==========================
# ===== numbers for the results paragraph (global)
#==========================
cat("\n--- Global % change over 1999-2021 (Theil-Sen) and post- vs pre-2013 ---\n")
print(stats[zone == "Global",
            .(dataset, type, metric, tau = round(tau, 2), p = signif(p, 2),
              pct_record = round(pct_record), pct_L8 = round(pct_L8))],
      nrows = Inf)

# "a pattern that persists across all climate zones": per-zone area trends
cat("\n--- Lake area trends by climate zone ---\n")
print(stats[zone != "Global" & type == "lake area",
            .(zone, dataset, metric, tau = round(tau, 2), p = signif(p, 2),
              pct_record = round(pct_record), pct_L8 = round(pct_L8))],
      nrows = Inf)

#==========================
# ===== formatting helpers
#==========================
fmt_p     <- function(p) ifelse(p < 0.001, "<0.001",
                                formatC(round(p, 3), format = "f", digits = 3, drop0trailing = TRUE))
fmt_slope <- function(x) ifelse(abs(x) >= 100,
                                formatC(round(x), format = "d", big.mark = ""),
                                formatC(round(x, 4), format = "f", digits = 4, drop0trailing = TRUE))
fmt_1     <- function(x) formatC(round(x, 1), format = "f", digits = 1, drop0trailing = TRUE)

#==========================
# ===== Supplementary Table 1: Mann-Kendall / Theil-Sen (global lake area)
#==========================
t1 <- stats[zone == "Global" & type == "lake area"]
t1 <- t1[order(factor(metric, levels = names(area_vars)), factor(dataset, levels = DATASETS))]
supp1 <- data.frame(
  `Dataset`                               = t1$dataset,
  `Global lake area aggregation metric`   = t1$metric,
  `Kendall's tau`                         = sprintf("%.2f", t1$tau),
  `Theil-Sen slope (km2 yr-1)`            = fmt_slope(t1$slope),
  `slope 95% confidence interval`         = paste0("[", fmt_slope(t1$slope_lo), ",", fmt_slope(t1$slope_hi), "]"),
  `z-statistic`                           = fmt_1(t1$z),
  `p-value`                               = fmt_p(t1$p),
  check.names = FALSE)

ft1 <- flextable(supp1) |>
  flextable::compose(part = "header", j = "Theil-Sen slope (km2 yr-1)",
          value = as_paragraph("Theil-Sen slope (km", as_sup("2"), " yr", as_sup("-1"), ")")) |>
  theme_booktabs() |> align(align = "center", part = "all") |>
  font(fontname = "Arial", part = "all") |> fontsize(size = 9, part = "all") |> autofit()
ft1

#==========================
# ===== Supplementary Table 2: Pearson r, lake area vs observation frequency
#==========================
t2 <- rbindlist(lapply(names(area_vars), function(m) rbindlist(lapply(DATASETS, function(ds) {
  d  <- ann[zone == "Global" & dataset == ds]
  ct <- cor.test(d[[obs_vars[[m]]]], d[[area_vars[[m]]]], method = "pearson")
  data.table(dataset = ds, metric = m, r = unname(ct$estimate),
             lo = ct$conf.int[1], hi = ct$conf.int[2],
             df = unname(ct$parameter), p = ct$p.value, n = nrow(d))
}))))

supp2 <- data.frame(
  `Dataset`                             = t2$dataset,
  `Global lake area aggregation metric` = tools::toTitleCase(t2$metric),
  `r (Pearson)`                         = sprintf("%.2f", t2$r),
  `95% confidence interval`             = sprintf("[%.2f, %.2f]", t2$lo, t2$hi),
  `df`                                  = t2$df,
  `p-value`                             = fmt_p(t2$p),
  check.names = FALSE)

ft2 <- flextable(supp2) |>
  theme_booktabs() |> align(align = "center", part = "all") |>
  font(fontname = "Arial", part = "all") |> fontsize(size = 9, part = "all") |>
  add_footer_lines(sprintf(paste(
    "Supplementary Table 2. Pearson correlation coefficients (two sided) between lake area",
    "metrics and observation frequency. There are %d datapoints used in each test."),
    unique(t2$n))) |>
  autofit()
ft2
