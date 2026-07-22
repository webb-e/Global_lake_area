# =============================================================================
# Landsat lake observation frequency  
#
# Supports manuscript statements on:
#   (1) Mean (+/- SD) annual observations per lake by climate zone, 1999-2021
#   (2) Percent increase in observation frequency, 2013-2021 vs 1999-2012,
#       globally and by climate zone
#   (3) Monotonic trends in observations over time
#       (Sen's slope + Mann-Kendall test)
# =============================================================================

library(arrow)
library(data.table)
library(trend)   

parquet_path <- "lake_area_observations.parquet"
df <- read_parquet(parquet_path) |> setDT()

# ---- Setup ------------------------------------------------------------------
yr_cols     <- as.character(1999:2021)
before_cols <- as.character(1999:2012)   # Landsat 7 era
after_cols  <- as.character(2013:2021)   # Landsat 8 era 

zone_labels <- c("1" = "Tropical", "2" = "Dry", "3" = "Temperate",
                 "4" = "Continental", "5" = "Polar")
df[, zone := zone_labels[as.character(climate_zone)]]   # zone 6 & NA -> NA

# =============================================================================
# (1) Mean +/- SD annual observations per lake by climate zone, 1999-2021
#
# For each dataset x zone: mean number of observations per lake is computed
# for each year (mean across lakes), giving 23 yearly values; reported value
# is the mean +/- SD of those yearly values (SD = interannual variability).
# =============================================================================

# Yearly zone means, computed column-wise (memory-safe)
yearly_zone <- df[!is.na(zone), lapply(.SD, mean, na.rm = TRUE),
                  by = .(dataset, zone), .SDcols = yr_cols]

# This summary table is tiny (2 datasets x 5 zones), safe to melt
yearly_zone_long <- melt(yearly_zone, id.vars = c("dataset", "zone"),
                         variable.name = "year", value.name = "mean_obs")
yearly_zone_long[, year := as.integer(as.character(year))]

obs_freq_by_zone <- yearly_zone_long[
  , .(mean_annual_obs = mean(mean_obs), sd_annual_obs = sd(mean_obs)),
  by = .(dataset, zone)][order(dataset, zone)]

cat("\n--- (1) Mean +/- SD observations per lake per year, 1999-2021 ---\n")
print(obs_freq_by_zone[, .(dataset, zone,
                           mean = round(mean_annual_obs, 1),
                           sd   = round(sd_annual_obs, 1))])

# =============================================================================
# (2) Percent increase in observation frequency: 2013-2021 vs 1999-2012
#
# Per lake: mean observations per year within each period (rowMeans).
# Per group: mean of those per-lake rates -> 'before' and 'after' =
# average observations per lake per year; pct_increase = % change.
# Dividing by the number of years in each period makes the two periods
# (14 vs 9 years) directly comparable.
# =============================================================================

df[, obs_before := rowMeans(.SD, na.rm = TRUE), .SDcols = before_cols]
df[, obs_after  := rowMeans(.SD, na.rm = TRUE), .SDcols = after_cols]

pct_increase <- function(dt, by_cols) {
  dt[, .(before = mean(obs_before, na.rm = TRUE),
         after  = mean(obs_after,  na.rm = TRUE)),
     by = by_cols][, pct_increase := 100 * (after - before) / before][]
}

# Global (all lakes, all climate zones incl. unclassified)
pct_global <- pct_increase(df, "dataset")

# By climate zone (unclassified excluded)
pct_zone <- pct_increase(df[!is.na(zone)], c("dataset", "zone"))

cat("\n--- (2) % increase in obs per lake per year, 2013-2021 vs 1999-2012 ---\n")
cat("\nGlobal:\n")
print(pct_global[, .(dataset,
                     before = round(before, 2), after = round(after, 2),
                     pct_increase = round(pct_increase, 0))])
cat("\nBy climate zone:\n")
print(pct_zone[order(dataset, zone),
               .(dataset, zone,
                 before = round(before, 2), after = round(after, 2),
                 pct_increase = round(pct_increase, 0))])

# =============================================================================
# (3) monotonic trend in observations over time
#     Sen's slope + Mann-Kendall p-value on yearly summary series
# =============================================================================

sen_mk <- function(y) {
  s <- sens.slope(y[!is.na(y)])
  list(slope = unname(s$estimates), p = s$p.value)
}

# Yearly total / mean / median across lakes, computed column-wise (memory-safe)
yearly_stats <- function(dt, by_cols) {
  ys <- dt[, c(list(stat = c("total", "mean", "median")),
               lapply(.SD, function(x) c(sum(x, na.rm = TRUE),
                                         mean(x, na.rm = TRUE),
                                         median(as.double(x), na.rm = TRUE)))),
           by = by_cols, .SDcols = yr_cols]
  # small table -> safe to melt
  ysl <- melt(ys, id.vars = c(by_cols, "stat"),
              variable.name = "year", value.name = "value")
  ysl[, year := as.integer(as.character(year))]
  ysl[order(year), c(sen_mk(value)), by = c(by_cols, "stat")]
}

trend_global <- yearly_stats(df, "dataset")
trend_zone   <- yearly_stats(df[!is.na(zone)], c("dataset", "zone"))

cat("\n--- (3) Sen's slope (obs/yr) + Mann-Kendall p, 1999-2021 ---\n")
cat("\nGlobal:\n")
print(trend_global)
cat("\nBy climate zone:\n")
print(trend_zone[order(dataset, zone, stat)])

