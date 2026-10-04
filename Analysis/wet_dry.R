########
#### Wet/dry season analysis
####  1. Detrended wet/dry month classification per lake (area ~ year + month, k-means on month effects)
####  2. Consistency test: random split-half comparison of wet/dry labels (random sample of lakes)
####  3. Climate-zone Kendall tau between mean observations per lake and wet-season proportion,
####     with significance from phase-randomized surrogates
####  Last updated Oct 2026 by E. Webb
########

library(arrow)
library(data.table)
library(dplyr)

########
### Paths
#########
parquet_path <- "/Volumes/EW_external/Global_landsat/lake_area_parquets/"
output_dir   <- '/Volumes/EW_external/Global_landsat/seasonal_dataset/'
climate_path <- "/Volumes/EW_external/Postdoc_Duke/Landsat8/annual_lake_medians_dataset/"

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
results_out <- "/Users/elizabethwebb/Library/CloudStorage/Box-Box/Landsat8/csvs"

########
### Settings
#########
n_rep      <- 10            # random split-half repetitions per lake
n_sample   <- 100000        # lakes (lake x dataset) randomly sampled for the consistency test
chunk_size <- 10000         # lakes per chunk in the consistency test
n_surr     <- 1000          # phase-randomized surrogates per Kendall test

# climate_zone code -> name (standard Koppen-Geiger order A-E; code 6 not labeled)
ZONE_LABELS <- c(`1` = "Tropical", `2` = "Dry", `3` = "Temperate", `4` = "Continental", `5` = "Polar")
ZONE_ORDER  <- c("Polar", "Continental", "Temperate", "Tropical", "Dry")


################################################################################
### PART 1: Per-lake wet/dry classification (all lakes)
################################################################################

parquet_files <- list.files(parquet_path, pattern = "\\.parquet$", full.names = TRUE, recursive = TRUE)
parquet_files <- parquet_files[!grepl("lake_group=000", parquet_files)]

schema_override <- schema(
  lake_id = string(), year = int32(), month = int32(),
  water = float64(), dataset = string(), lake_group = string())

## climate_zone lookup from the annual lake medians parquets (matched by lake_id)
climate_data <- open_dataset(climate_path, format = "parquet") %>%
  select(lake_id, climate_zone) %>%
  distinct() %>%
  collect() %>%
  as.data.table()
climate_data[, lake_id := as.character(lake_id)]
stopifnot(!anyDuplicated(climate_data$lake_id))   # one climate_zone per lake

########
### Detrended wet/dry classification for one lake-dataset
### Model: area ~ factor(year) + factor(month)
###   - year effects remove interannual variability
###   - month effects = detrended seasonal cycle -> two clusters (wet / dry)
### At least two calendar months (and two years, for the year effects) are needed.
### With exactly two months, the higher month is wet and the lower month is dry
### (each month is its own cluster); with three or more, k-means is used.
#########
classify_seasons_detrended <- function(year, month, water) {
  out_na <- list(month = NA_integer_, season = NA_character_)
  ok <- !is.na(water)
  y  <- water[ok]
  yr <- factor(year[ok])
  mo <- factor(month[ok])
  if (nlevels(mo) < 2 || nlevels(yr) < 2) return(out_na)   # need >= 2 months and >= 2 years

  fit <- lm.fit(model.matrix(~ yr + mo), y)

  # Detrended month effects (first month level = reference, 0)
  mo_coef <- fit$coefficients[grep("^mo", names(fit$coefficients))]
  effects <- setNames(c(0, unname(mo_coef)), levels(mo))
  effects <- effects[!is.na(effects)]
  if (length(effects) < 2 || length(unique(effects)) < 2) return(out_na)

  if (length(effects) == 2) {
    # Two months: each month is its own cluster (higher = wet, lower = dry).
    # Handled directly because R's default k-means algorithm requires > 2 points.
    season <- ifelse(effects == max(effects), "wet", "dry")
  } else {
    km <- tryCatch(kmeans(effects, centers = 2, nstart = 10), error = function(e) NULL)
    if (is.null(km)) return(out_na)
    wet_cluster <- which.max(tapply(effects, km$cluster, mean))
    season <- ifelse(km$cluster == wet_cluster, "wet", "dry")
  }

  list(month = as.integer(names(effects)), season = season)
}

########
### Full-period labels and annual wet/dry proportions
#########
summarize_seasonality <- function(monthly_medians) {

  # One label per calendar month per lake
  labels_full <- monthly_medians[, classify_seasons_detrended(year, month, median_water),
                                 by = .(lake_id, dataset)][!is.na(month)]

  classified_lakes <- unique(labels_full[, .(lake_id, dataset)])[, classified := TRUE]

  # Apply fixed month labels to every year
  monthly_labeled <- merge(monthly_medians, labels_full,
                           by = c("lake_id", "dataset", "month"), all.x = TRUE)

  # Annual wet/dry counts and proportions
  season_summary <- monthly_labeled[, {
    total     <- .N
    wet_count <- sum(season == "wet", na.rm = TRUE)
    dry_count <- sum(season == "dry", na.rm = TRUE)
    list(
      n_total  = total,
      n_wet    = wet_count,
      n_dry    = dry_count,
      prop_wet = if (total > 0) wet_count / total else NA_real_,
      prop_dry = if (total > 0) dry_count / total else NA_real_
    )
  }, by = .(lake_id, dataset, year)]

  season_summary <- merge(season_summary, classified_lakes, by = c("lake_id", "dataset"), all.x = TRUE)
  season_summary[is.na(classified), classified := FALSE]
  season_summary
}

########
### Loop over lake groups (skips groups already written to output_dir)
#########
for (file in parquet_files) {
  lake_group_val <- sub(".*lake_group=([0-9]+)/.*", "\\1", file)
  out_file <- file.path(output_dir, paste0("wetdry_dataset_group_", lake_group_val, ".parquet"))

  if (file.exists(out_file)) {
    message("Skipping already processed lake group: ", lake_group_val)
    next
  }
  message("Processing lake group: ", lake_group_val)

  dt <- open_dataset(file, format = "parquet", schema = schema_override) %>%
    filter(year >= 1999, year <= 2021) %>%
    select(lake_id, month, water, dataset, year) %>%
    collect() %>%
    as.data.table()

  if (nrow(dt) == 0) next
  dt[, lake_id := as.character(lake_id)]

  monthly_medians <- dt[, .(median_water = median(water, na.rm = TRUE)),
                        by = .(lake_id, dataset, year, month)]

  season_summary   <- summarize_seasonality(monthly_medians)
  summary_combined <- merge(season_summary, climate_data, by = "lake_id", all.x = TRUE)

  write_parquet(summary_combined, out_file)
}


################################################################################
### PART 2: Classification counts, Kendall surrogate tests, consistency test
################################################################################

files <- list.files(output_dir, pattern = "^wetdry_dataset_group_.*\\.parquet$", full.names = TRUE)
ds <- open_dataset(files, format = "parquet")   # lazy: nothing loaded yet

# Zone names + ordering
add_zone_names <- function(d) {
  d <- copy(d)
  d[, zone_name := factor(unname(ZONE_LABELS[as.character(climate_zone)]), levels = ZONE_ORDER)]
  setcolorder(d, c("dataset", "climate_zone", "zone_name"))
  setorderv(d, c("dataset", "zone_name"), na.last = TRUE)
  d
}

########
### Lake-level table: one row per lake x dataset
#########
lake_level <- ds %>%
  select(lake_id, dataset, climate_zone, classified) %>%
  distinct() %>%
  collect() %>%
  as.data.table()

# Number and percentage of lakes classified per dataset / climate zone
classification_counts <- lake_level[, .(n_lakes      = .N,
                                        n_classified = sum(classified),
                                        pct_classified = 100 * mean(classified)),
                                    by = .(dataset, climate_zone)]

########
### Phase-randomized surrogates (Ebisuzaki 1997)
### Preserves the autocorrelation (power spectrum) of x; randomizes its timing relative to y
#########
phase_randomize <- function(x) {
  n  <- length(x)
  mu <- mean(x)
  f  <- fft(x - mu)
  half <- floor((n - 1) / 2)
  f_new <- f
  ph <- runif(half, 0, 2 * pi)
  f_new[2:(half + 1)]     <- Mod(f[2:(half + 1)]) * exp(1i * ph)
  f_new[n:(n - half + 1)] <- Conj(f_new[2:(half + 1)])
  if (n %% 2 == 0) f_new[n / 2 + 1] <- Mod(f[n / 2 + 1]) * sample(c(-1, 1), 1)
  Re(fft(f_new, inverse = TRUE)) / n + mu
}

kendall_surrogate <- function(x, y, n_surr = 1000, min_years = 10) {
  ok <- complete.cases(x, y)
  x <- x[ok]; y <- y[ok]
  if (length(x) < min_years) {
    return(list(n_years = length(x), tau = NA_real_, p_surrogate = NA_real_, p_naive = NA_real_))
  }
  tau_obs  <- cor(x, y, method = "kendall")
  tau_null <- replicate(n_surr, cor(phase_randomize(x), y, method = "kendall"))
  list(n_years     = length(x),
       tau         = tau_obs,
       p_surrogate = (sum(abs(tau_null) >= abs(tau_obs)) + 1) / (n_surr + 1),
       p_naive     = suppressWarnings(cor.test(x, y, method = "kendall")$p.value))
}

########
### Climate-zone annual series (classified lakes)
###   mean_n_obs    = sum(n_total) / L  (L = classified lakes in the zone; unobserved lakes contribute 0)
###   mean_prop_wet = mean wet-season proportion across lakes observed that year
#########
zone_year_series <- function() {
  L_tab <- lake_level[classified == TRUE, .(L = uniqueN(lake_id)), by = .(dataset, climate_zone)]

  zy <- ds %>%
    filter(classified == TRUE) %>%
    group_by(dataset, climate_zone, year) %>%
    summarise(sum_obs       = sum(n_total, na.rm = TRUE),
              mean_prop_wet = mean(prop_wet, na.rm = TRUE)) %>%
    collect() %>%
    as.data.table()

  zy <- merge(zy, L_tab, by = c("dataset", "climate_zone"))
  zy[, mean_n_obs := sum_obs / L]
  setorder(zy, year)
  zy
}

########
### Kendall tau with surrogate p-values
#########
set.seed(42)

zone_year <- zone_year_series()
kendall_results <- zone_year[, kendall_surrogate(mean_n_obs, mean_prop_wet, n_surr = n_surr),
                             by = .(dataset, climate_zone)]

########
### Consistency test: random split-half comparison on a random sample of classified lakes
###   For each repetition, each lake's years are randomly assigned to two equal halves,
###   months are classified separately in each half, and the proportion of calendar months
###   with the same wet/dry label in both halves is calculated. Averaged over n_rep reps.
#########
random_split_agreement <- function(monthly_medians, n_rep) {
  yrs <- unique(monthly_medians[!is.na(median_water), .(lake_id, dataset, year)])

  agree_reps <- rbindlist(lapply(seq_len(n_rep), function(r) {
    halves <- yrs[, .(year, half = sample(rep(1:2, length.out = .N))), by = .(lake_id, dataset)]
    mm <- merge(monthly_medians, halves, by = c("lake_id", "dataset", "year"))

    lab <- mm[, classify_seasons_detrended(year, month, median_water),
              by = .(lake_id, dataset, half)][!is.na(month)]

    cmp <- merge(lab[half == 1, .(lake_id, dataset, month, s1 = season)],
                 lab[half == 2, .(lake_id, dataset, month, s2 = season)],
                 by = c("lake_id", "dataset", "month"))

    cmp[, .(n_months_compared = .N,
            prop_months_agree = mean(s1 == s2)),
        by = .(lake_id, dataset)]
  }))

  agree_reps[, .(n_months_compared = mean(n_months_compared),
                 prop_months_agree = mean(prop_months_agree),
                 n_reps            = .N),
             by = .(lake_id, dataset)]
}

set.seed(42)

classified_named <- lake_level[classified == TRUE & climate_zone %in% 1:5]   # named zones only
lake_sample      <- classified_named[sample(.N, min(.N, n_sample))]          # simple random sample
sample_ids  <- unique(lake_sample$lake_id)

dt_sample <- open_dataset(parquet_files, format = "parquet", schema = schema_override) %>%
  filter(year >= 1999, year <= 2021, lake_id %in% sample_ids) %>%
  select(lake_id, dataset, year, month, water) %>%
  collect() %>%
  as.data.table()

# keep only the sampled lake x dataset combinations
dt_sample <- dt_sample[lake_sample[, .(lake_id, dataset)], on = .(lake_id, dataset), nomatch = 0]

monthly_sample <- dt_sample[, .(median_water = median(water, na.rm = TRUE)),
                            by = .(lake_id, dataset, year, month)]

# Run in chunks of lakes to limit memory use and show progress
lake_keys <- unique(monthly_sample[, .(lake_id, dataset)])
lake_keys[, chunk := ceiling(seq_len(.N) / chunk_size)]

split_sample <- rbindlist(lapply(unique(lake_keys$chunk), function(k) {
  message("Consistency test chunk ", k, " of ", max(lake_keys$chunk))
  keys_k <- lake_keys[chunk == k, .(lake_id, dataset)]
  random_split_agreement(monthly_sample[keys_k, on = .(lake_id, dataset)], n_rep)
}))

# Summary by dataset and both datasets combined
summarize_agree <- function(d) {
  d[, .(n_lakes             = .N,
        mean_agree          = mean(prop_months_agree),
        median_agree        = median(prop_months_agree),
        pct_lakes_all_agree = 100 * mean(prop_months_agree == 1))]
}

split_summary <- rbind(split_sample[, summarize_agree(.SD), by = dataset],
                       summarize_agree(split_sample)[, dataset := "Both"],
                       use.names = TRUE)

classification_counts <- add_zone_names(classification_counts)
kendall_results       <- add_zone_names(kendall_results)

########
### Save
#########

fwrite(zone_year,             file.path(results_out, "zone_year.csv"))             
fwrite(kendall_results,       file.path(results_out, "kendall_results.csv"))
fwrite(classification_counts, file.path(results_out, "classification_counts.csv"))
fwrite(split_summary,         file.path(results_out, "split_summary.csv"))
fwrite(split_sample,          file.path(results_out, "split_sample.csv"))         
