#############
### Buffer-distance sensitivity
###
### Same model logic as the main script (12-candidate all-subsets AIC
### selection with corAR1(~ year), ML selection, REML refit), re-run
### separately for each buffer distance (0, 30, 60, 90 m).
###
#############

library(tidyverse)
library(data.table)
library(nlme)
library(flextable)
library(officer)


id_col <- "lake_id"

#==========================
# ===== read in buffer CSVs
#==========================
buffer_path <- '...'

buffer_files <- list.files(buffer_path, pattern = "^lake_area_chunk_.*\\.csv$",
                           full.names = TRUE, recursive = TRUE)

# read one file at a time and collapse monthly -> annual per lake before
# binding, so the full monthly table is never held in memory
read_annual <- function(f) {
  dt <- fread(f, select = c(id_col, "buffer_m", "year", "water", "dataset"))
  dt[, (id_col) := as.character(get(id_col))]
  dt[, .(median_water = median(water, na.rm = TRUE),
         n_obs        = sum(!is.na(water))),
     by = c(id_col, "buffer_m", "dataset", "year")]
}
buf_annual <- rbindlist(lapply(buffer_files, read_annual), use.names = TRUE)


#==========================
# ===== annual global summaries
#==========================
annual_summary <- function(dt) {
  out <- dt[, .(
    median_water_median = median(median_water, na.rm = TRUE),
    median_water_mean   = mean(median_water,   na.rm = TRUE),
    median_water_sum    = sum(median_water,    na.rm = TRUE),
    n_obs_median        = median(n_obs, na.rm = TRUE),
    n_obs_mean          = mean(n_obs,   na.rm = TRUE),
    n_obs_sum           = sum(n_obs,    na.rm = TRUE)), by = c("dataset", "year")]
  out[, `:=`(total_lake_area  = median_water_sum    / 1e6,
             mean_lake_area   = median_water_mean   / 1e6,
             median_lake_area = median_water_median / 1e6)]
  out[]
}

responses      <- c(total = "total_lake_area", mean = "mean_lake_area", median = "median_lake_area")
resp_label     <- c(total = "Total lake area", mean = "Mean lake area", median = "Median lake area")
response_order <- c("total", "median", "mean")

#==========================
# ===== formatting helpers
#==========================
fmt_p    <- function(p) ifelse(p < 0.01, "<0.01", sprintf("%.2f", p))
fmt_coef <- function(x) ifelse(abs(x) < 1e-4 & x != 0,
                               formatC(x, format = "e", digits = 2),
                               as.character(signif(x, 2)))
fmt_ci   <- function(lo, hi) paste0("[", signif(lo, 2), ", ", signif(hi, 2), "]")

obs_code <- c(n_obs_sum = "S", n_obs_mean = "M", n_obs_median = "Md")

fmt_pct <- function(x) if (is.na(x) || !is.finite(x)) "-" else paste0(signif(x, 2), "%")

#==========================
# ===== all-subsets AIC selection with AR(1) errors
#   12 candidates = {n_obs_sum, n_obs_mean, n_obs_median} x {no year, + year}
#   x {no era, + era}, where era = pre/post-2013 step. Selection under ML;
#   winner refit under REML.
#==========================
select_gls_aic <- function(data, response,
                           obs_candidates = c("n_obs_sum", "n_obs_mean", "n_obs_median"),
                           label = "") {
  data <- as.data.frame(data)
  data <- data[order(data$year), ]
  data$era <- factor(ifelse(data$year >= 2013, "post", "pre"),
                     levels = c("pre", "post"))
  specs <- list()
  for (ov in obs_candidates) {
    specs[[ov]]                    <- ov
    specs[[paste0(ov, "+yr")]]     <- c(ov, "year")
    specs[[paste0(ov, "+era")]]    <- c(ov, "era")
    specs[[paste0(ov, "+yr+era")]] <- c(ov, "year", "era")
  }
  fit_one <- function(terms, reml = FALSE)
    tryCatch(gls(reformulate(terms, response), data = data,
                 correlation = corAR1(form = ~ year),
                 method = if (reml) "REML" else "ML"),
             error = function(e) NULL)
  
  fits <- lapply(specs, fit_one)
  ok   <- !vapply(fits, is.null, logical(1))
  if (!any(ok)) return(NULL)
  fits <- fits[ok]; specs <- specs[ok]
  aic  <- vapply(fits, AIC, numeric(1))
  best <- names(which.min(aic))
  
  m <- fit_one(specs[[best]], reml = TRUE)
  if (is.null(m)) m <- fits[[best]]
  cat(sprintf("%-40s best: %-16s AIC=%.1f (dAIC next=%.1f)\n",
              label, best, min(aic),
              if (length(aic) > 1) sort(aic)[2] - min(aic) else NA))
  attr(m, "obs_var")   <- specs[[best]][1]
  attr(m, "has_year")  <- "year" %in% specs[[best]]
  attr(m, "has_era")   <- "era"  %in% specs[[best]]
  attr(m, "sel_terms") <- specs[[best]]
  m
}

#==========================
# ===== per-model stats -> tidy block
#==========================
term_levels_gls <- c("Observation frequency", "Year", "Landsat 8", "Entire model")

model_stats_gls <- function(data, response) {
  d <- as.data.frame(data[order(year)])
  if (nrow(d) < 6L) return(NULL)
  d$era <- factor(ifelse(d$year >= 2013, "post", "pre"), levels = c("pre", "post"))
  m <- tryCatch(select_gls_aic(d, response, label = response),
                error = function(e) NULL)
  if (is.null(m)) return(NULL)
  
  obs_var <- attr(m, "obs_var")
  tt      <- summary(m)$tTable
  preds   <- setdiff(rownames(tt), "(Intercept)")
  dfres   <- nrow(d) - length(coef(m))
  ci_lo   <- tt[, "Value"] - qt(0.975, dfres) * tt[, "Std.Error"]
  ci_hi   <- tt[, "Value"] + qt(0.975, dfres) * tt[, "Std.Error"]
  obs_vals <- as.numeric(fitted(m) + residuals(m))
  r2       <- tryCatch(cor(fitted(m), obs_vals)^2, error = function(e) NA_real_)
  base     <- d[[response]][which.min(d$year)]
  
  # overall-model significance: LRT of full vs intercept-only, both ML, same AR(1)
  sel_terms <- attr(m, "sel_terms")
  model_p <- tryCatch({
    full_ml <- gls(reformulate(sel_terms, response), data = d,
                   correlation = corAR1(form = ~ year), method = "ML")
    null_ml <- gls(reformulate("1", response), data = d,
                   correlation = corAR1(form = ~ year), method = "ML")
    anova(null_ml, full_ml)[["p-value"]][2]
  }, error = function(e) NA_real_)
  
  term_row <- function(var) {
    if (!var %in% preds) return(c(coef = "-", pct = "-", ci = "-", p = "-"))
    est <- tt[var, "Value"]
    pct <- if (is.na(base) || base == 0) NA_real_ else 100 * est / base
    c(coef = unname(fmt_coef(est)),
      pct  = fmt_pct(pct),
      ci   = unname(fmt_ci(ci_lo[var], ci_hi[var])),
      p    = unname(fmt_p(tt[var, "p-value"])))
  }
  obs <- term_row(obs_var); yr <- term_row("year"); era <- term_row("erapost")
  tibble(
    Term        = term_levels_gls,
    `obs var`   = c(unname(obs_code[obs_var]), "", "", ""),
    Coefficient = c(obs["coef"], yr["coef"], era["coef"], "-"),
    `% trend`   = c(obs["pct"], yr["pct"], era["pct"], "-"),
    `95% CI`    = c(obs["ci"], yr["ci"], era["ci"], "-"),
    `r-squared` = c("-", "-", "-", if (is.na(r2)) "-" else sprintf("%.2f", r2)),
    `p-value`   = c(obs["p"], yr["p"], era["p"], if (is.na(model_p)) "-" else fmt_p(model_p))
  )
}

skel_block_gls <- function() tibble(Term = term_levels_gls, `obs var` = "-",
                                    Coefficient = "-", `% trend` = "-", `95% CI` = "-",
                                    `r-squared` = "-", `p-value` = "-")

pair_block_gls <- function(dg, dl, response) {
  gs <- model_stats_gls(dg, response)
  gl <- model_stats_gls(dl, response)
  if (is.null(gs) && is.null(gl)) return(NULL)
  if (is.null(gs)) gs <- skel_block_gls()
  if (is.null(gl)) gl <- skel_block_gls()
  gsw <- gs %>% rename_with(~ paste0("GSWO ", .x), -Term)
  glw <- gl %>% rename_with(~ paste0("GLAD ", .x), -Term)
  full_join(gsw, glw, by = "Term") %>%
    mutate(Term = factor(Term, levels = term_levels_gls)) %>%
    arrange(Term) %>% mutate(Term = as.character(Term))
}

#==========================
# ===== build summary table 
#==========================
col_order <- c("Model", "Term",
               "GSWO obs var", "GSWO Coefficient", "GSWO % trend", "GSWO 95% CI",
               "GSWO r-squared", "GSWO p-value",
               "GLAD obs var", "GLAD Coefficient", "GLAD % trend", "GLAD 95% CI",
               "GLAD r-squared", "GLAD p-value")

build_table <- function(gswo, glad) {
  blocks <- lapply(response_order, function(rn) {
    b <- pair_block_gls(gswo, glad, responses[[rn]])
    if (is.null(b)) return(NULL)
    b %>% mutate(Model = c(resp_label[[rn]], rep("", nrow(b) - 1)), .before = Term)
  })
  out <- bind_rows(Filter(Negate(is.null), blocks))[, col_order]
  out[is.na(out)] <- ""
  out
}

#==========================
# ===== sensitivity loop over buffer distance
#==========================
buffers <- sort(unique(buf_annual$buffer_m))
summary_by_buffer <- list()

for (b in buffers) {
  cat(sprintf("\n===== buffer %s m =====\n", b))
  gswo <- annual_summary(buf_annual[buffer_m == b & dataset == "GSWO"])
  glad <- annual_summary(buf_annual[buffer_m == b & dataset == "GLAD"])
  summary_by_buffer[[as.character(b)]] <- build_table(gswo, glad)
}

summary_buffers <- rbindlist(summary_by_buffer, idcol = "buffer_m")

#==========================
# ===== publication table
#==========================

tab <- as.data.frame(summary_buffers)
tab <- tab[, !grepl("% trend$", names(tab))]

# fill Model down so rows can be re-sorted
tab$Model[tab$Model == ""] <- NA
tab$Model <- zoo::na.locf(tab$Model)

tab <- tab[order(factor(tab$Model, levels = resp_label[response_order]),
                 tab$buffer_m,
                 factor(tab$Term, levels = term_levels_gls)), ]

tab$buffer_m <- paste0(tab$buffer_m, " m")
names(tab)[names(tab) == "buffer_m"] <- "Buffer"
tab <- tab[, c("Model", "Buffer", setdiff(names(tab), c("Model", "Buffer")))]

model_starts  <- which(!duplicated(tab$Model))
buffer_starts <- setdiff(which(!duplicated(paste(tab$Model, tab$Buffer))), model_starts)

# show Model once per block, Buffer once per model x buffer
tab$Buffer[duplicated(paste(tab$Model, tab$Buffer))] <- ""
tab$Model[duplicated(tab$Model)] <- ""
tab[tab == "-"] <- "–"

# two-level header: dataset on top, statistic below
keys <- names(tab)
sub_lab <- sub("^(GSWO|GLAD) ", "", keys)
sub_lab <- dplyr::recode(sub_lab, `obs var` = "Obs. var.", `r-squared` = "R²", `p-value` = "p")
top_lab <- ifelse(grepl("^GSWO ", keys), "GSWO",
                  ifelse(grepl("^GLAD ", keys), "GLAD", sub_lab))
hdr <- data.frame(col_keys = keys, top = top_lab, sub = sub_lab)

ft <- flextable(tab) |>
  set_header_df(mapping = hdr, key = "col_keys") |>
  merge_h(part = "header") |>
  merge_v(part = "header") |>
  theme_booktabs() |>
  hline(i = model_starts[-1] - 1, border = fp_border(width = 1), part = "body") |>
  hline(i = buffer_starts - 1, border = fp_border(width = 0.5, color = "grey70"), part = "body") |>
  align(align = "center", part = "all") |>
  align(j = c("Model", "Buffer", "Term"), align = "left", part = "all") |>
  bold(part = "header") |>
  italic(i = 2, j = grep("p-value$", keys), part = "header") |>
  font(fontname = "Arial", part = "all") |>
  fontsize(size = 8, part = "all") |>
  padding(padding.top = 1, padding.bottom = 1, part = "body") |>
  fontsize(size = 7, part = "footer") |>
  autofit()

ft

save_as_docx(ft, path = "buffer_sensitivity_table.docx",
             pr_section = prop_section(page_size = page_size(orient = "landscape")))
