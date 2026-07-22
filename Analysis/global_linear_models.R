#############
### Linear models relating global total, mean, and median annual lake area to observation
### frequency and year
###
### Observation frequency is represented three ways (n_obs_sum, n_obs_mean,
### n_obs_median), entered one at a time as alternative models.
### For each annually aggregated response the code tries
### combination (3 obs variants x {no year, +year} = 6 models) and selects the
### model with the lowest AIC, repeating for each dataset. 
### Each response x dataset is fit twice:
###   (a) all lakes
###   (b) complete-record lakes: lakes with >= 1 observation in EVERY year.
###
### Output: summary table with terms (Observation frequency / Year / Entire model) as rows 
### and metrics (coefficient / 95% CI / r-squared / p-value) as columns
###
### Last updated July 2026 by E.Webb

#############
library(arrow)
library(tidyverse)
library(data.table)
library(relaimpo)

id_col <- "lake_id"   # per-lake identifier in the parquet
#==========================
# ===== read in files 
#==========================
parquet_path <- 'annual_lake_medians_dataset'
parquet_files <- list.files(parquet_path, pattern = "\\.parquet$",
                            full.names = TRUE, recursive = TRUE)
read_raw <- function(path, ds_name) {
  open_dataset(path) %>%
    filter(dataset == ds_name) %>%
    dplyr::select(all_of(c(id_col, "dataset", "year", "n_obs", "median_water"))) %>%
    collect() %>% as.data.table()
}

annual_summary <- function(dt) {
  out <- dt[, .(
    median_water_median = median(median_water, na.rm = TRUE),
    median_water_mean   = mean(median_water,   na.rm = TRUE),
    median_water_sum    = sum(median_water,    na.rm = TRUE),
    n_obs_median        = median(n_obs, na.rm = TRUE),
    n_obs_mean          = mean(n_obs,   na.rm = TRUE),
    n_obs_sum           = sum(n_obs,    na.rm = TRUE)), by = .(dataset, year)]
  out[, `:=`(total_lake_area  = median_water_sum    / 1e6,   # km2
             mean_lake_area   = median_water_mean   / 1e6,
             median_lake_area = median_water_median / 1e6)]
  out[]
}
# lakes observed (n_obs >= 1) in every year of the record
subset_complete <- function(dt) {
  n_years <- uniqueN(dt$year)
  cnt  <- dt[n_obs > 0, .(nyr = uniqueN(year)), by = c(id_col)]
  keep <- cnt[nyr == n_years][[id_col]]
  dt[get(id_col) %in% keep]
}
gswo_raw <- rbindlist(lapply(parquet_files, read_raw, ds_name = "GSWO"),
                      use.names = TRUE, fill = TRUE)
glad_raw <- rbindlist(lapply(parquet_files, read_raw, ds_name = "GLAD"),
                      use.names = TRUE, fill = TRUE)
gswo_c_raw <- subset_complete(gswo_raw)
glad_c_raw <- subset_complete(glad_raw)

# annual summaries: full and complete-record subset
gswo   <- annual_summary(gswo_raw);   glad   <- annual_summary(glad_raw)
gswo_c <- annual_summary(gswo_c_raw); glad_c <- annual_summary(glad_c_raw)

#==========================
# ===== model selection: all-subsets AIC
#==========================
select_model_aic <- function(data, response, obs_candidates = c("n_obs_sum", "n_obs_mean","n_obs_median"),label = "") {
  specs <- list()
  for (ov in obs_candidates) {
    specs[[ov]]                <- ov            # obs only
    specs[[paste0(ov, "+yr")]] <- c(ov, "year") # obs + year
  }
  fits <- lapply(specs, function(terms) lm(reformulate(terms, response), data = data))
  aic  <- vapply(fits, AIC, numeric(1))
  best <- names(which.min(aic))
  m    <- fits[[best]]
  cat(sprintf("%-32s best: %-16s AIC=%.1f (dAIC next=%.1f)\n",
              label, best, min(aic), sort(aic)[2] - min(aic)))
  attr(m, "obs_var")  <- specs[[best]][1]
  attr(m, "has_year") <- "year" %in% specs[[best]]
  m
}
#==========================
# ===== table formatting helpers
#==========================
fmt_p <- function(p) ifelse(p < 0.01, "<0.01", sprintf("%.2f", p))
fmt_coef <- function(x) ifelse(abs(x) < 1e-4 & x != 0,
                               formatC(x, format = "e", digits = 2),
                               as.character(signif(x, 2)))
fmt_ci <- function(lo, hi) paste0("[", signif(lo, 2), ", ", signif(hi, 2), "]")
# One model -> 3 rows (Observation frequency / Year / Entire model),
# metrics as columns (coefficient / 95% CI / r-squared / p-value).

model_block <- function(data, response, dataset_label, tag) {
  model   <- select_model_aic(data, response, label = paste(response, dataset_label, tag))
  obs_var <- attr(model, "obs_var")
  s       <- summary(model)
  coefs <- coef(model)
  preds <- names(coefs)[-1]
  ci    <- confint(model)
  pv    <- setNames(s$coefficients[preds, "Pr(>|t|)"], preds)
  r2_total <- s$r.squared
  model_p  <- pf(s$fstatistic[1], s$fstatistic[2], s$fstatistic[3],
                 lower.tail = FALSE)
  r2p <- if (length(preds) >= 2)
           calc.relimp(model, type = "lmg", rela = FALSE)$lmg[preds]
         else setNames(r2_total, preds)
  term_row <- function(var) {
    if (!var %in% preds) return(c(coef = "-", ci = "-", r2 = "-", p = "-"))
    c(coef = unname(fmt_coef(coefs[var])),
      ci   = unname(fmt_ci(ci[var, 1], ci[var, 2])),
      r2   = unname(sprintf("%.2f", r2p[var])),
      p    = unname(fmt_p(pv[var])))
  }
  obs <- term_row(obs_var); yr <- term_row("year")
  tibble(
    Dataset     = c(dataset_label, "", ""),
    Term        = c(paste0("Observation frequency (", obs_var, ")"),
                    "Year", "Entire model"),
    Coefficient = c(obs["coef"], yr["coef"], "-"),
    `95% CI`    = c(obs["ci"],   yr["ci"],   "-"),
    `r-squared` = c(obs["r2"],   yr["r2"],   sprintf("%.2f", r2_total)),
    `p-value`   = c(obs["p"],    yr["p"],    fmt_p(model_p))
  )
}
section_header <- function(label) {
  tibble(Dataset = label, Term = "", Coefficient = "",
         `95% CI` = "", `r-squared` = "", `p-value` = "")
}
#==========================
# ===== build the summary table
#==========================
summary_table <- bind_rows(
  section_header("Total lake area - all lakes"),
  model_block(gswo,   "total_lake_area",  "GSWO", "all"),
  model_block(glad,   "total_lake_area",  "GLAD", "all"),
  section_header("Total lake area - complete-record lakes"),
  model_block(gswo_c, "total_lake_area",  "GSWO", "complete"),
  model_block(glad_c, "total_lake_area",  "GLAD", "complete"),
  section_header("Mean lake area - all lakes"),
  model_block(gswo,   "mean_lake_area",   "GSWO", "all"),
  model_block(glad,   "mean_lake_area",   "GLAD", "all"),
  section_header("Mean lake area - complete-record lakes"),
  model_block(gswo_c, "mean_lake_area",   "GSWO", "complete"),
  model_block(glad_c, "mean_lake_area",   "GLAD", "complete"),
  section_header("Median lake area - all lakes"),
  model_block(gswo,   "median_lake_area", "GSWO", "all"),
  model_block(glad,   "median_lake_area", "GLAD", "all"),
  section_header("Median lake area - complete-record lakes"),
  model_block(gswo_c, "median_lake_area", "GSWO", "complete"),
  model_block(glad_c, "median_lake_area", "GLAD", "complete")
)

write.csv(summary_table,
           file.path("lake_area_model_summary_table.csv"),
           row.names = FALSE, na = "")

#==========================
#   For each response x dataset x subset we take the obs variable selected by
#   AIC and report:
#     cor_obs_year = Pearson r between year and that obs variable
#     VIF          = variance inflation factor for the 2-predictor
#==========================
collinearity_diag <- function(data, response, dataset_label, subset_label) {
  m       <- select_model_aic(data, response,
                             label = paste(response, dataset_label, subset_label))
  obs_var <- attr(m, "obs_var")
  r       <- cor(data[[obs_var]], data$year, use = "complete.obs")
  data.frame(
    response     = response,
    dataset      = dataset_label,
    subset       = subset_label,
    obs_var      = obs_var,
    cor_obs_year = round(r, 3),
    VIF          = round(1 / (1 - r^2), 2)   # 2-predictor VIF = 1 / (1 - r^2)
  )
}
collinearity <- bind_rows(
  collinearity_diag(gswo,   "total_lake_area",  "GSWO", "all"),
  collinearity_diag(glad,   "total_lake_area",  "GLAD", "all"),
  collinearity_diag(gswo_c, "total_lake_area",  "GSWO", "complete"),
  collinearity_diag(glad_c, "total_lake_area",  "GLAD", "complete"),
  collinearity_diag(gswo,   "mean_lake_area",   "GSWO", "all"),
  collinearity_diag(glad,   "mean_lake_area",   "GLAD", "all"),
  collinearity_diag(gswo_c, "mean_lake_area",   "GSWO", "complete"),
  collinearity_diag(glad_c, "mean_lake_area",   "GLAD", "complete"),
  collinearity_diag(gswo,   "median_lake_area", "GSWO", "all"),
  collinearity_diag(glad,   "median_lake_area", "GLAD", "all"),
  collinearity_diag(gswo_c, "median_lake_area", "GSWO", "complete"),
  collinearity_diag(glad_c, "median_lake_area", "GLAD", "complete")
)

print(collinearity, row.names = FALSE)
