#==============================================================================
# Buffer-distance sensitivity: global aggregated lake area models
#
# Same model as the main analysis (lake_area_models_all_methods.R)
# fit separately for each buffer distance (0, 30, 60, 90 m):
#
#   area ~ n_obs_sum + year + era,  corAR1(~year), REML
#     n_obs_sum = total valid observations across included lakes per year
#     era       = pre-2013 vs 2013 onward (introduction of Landsat 8)
#
# Responses: total, mean, median lake area (km2).
#
#==============================================================================
library(tidyverse)
library(data.table)
library(nlme)
library(patchwork)
library(scales)
library(grid)
library(flextable)
library(officer)

#==========================
# ===== settings
#==========================
buffer_path <- '.../buffer_csvs'

id_col   <- "lake_id"
DATASETS <- c("GSWO", "GLAD")
YEARS    <- 1999:2021
RESP     <- c("total", "mean", "median")
TERMS    <- c("obs", "era", "year")

RESP_LABEL <- c(total = "Total lake area", median = "Median lake area", mean = "Mean lake area")
TERM_LABEL <- c(obs = "Observation frequency", era = "Landsat 8", year = "Year",
                acq = "Data acquisition (obs + Landsat 8)")

#==========================
# ===== read buffer CSVs -> annual lake table
#==========================
# chunk files only (excludes buffer_analysis/buffer_wide.csv)
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

# a lake-year split across two files would give two partial medians
dup <- buf_annual[, .N, by = c(id_col, "buffer_m", "dataset", "year")][N > 1]
if (nrow(dup) > 0) stop(nrow(dup), " lake-years are split across files")

# same lake-year filters as the main analysis; area in km2
buf_annual <- buf_annual[year %between% range(YEARS) & n_obs > 0 & is.finite(median_water)]
buf_annual[, w := median_water / 1e6]

buffers <- sort(unique(buf_annual$buffer_m))

#==========================
# ===== annual global series for one dataset x buffer
#==========================
annual_series <- function(dt) {
  dt[, .(n_obs_sum = as.numeric(sum(n_obs)),
         n_lakes   = .N,
         total     = sum(w),
         mean      = sum(w) / .N,
         median    = median(w)), by = year][order(year)]
}

#==========================
# ===== fit one dataset x buffer
#==========================
fit_buffer <- function(d, ds, buf) {
  d <- as.data.frame(d)
  d$era <- factor(ifelse(d$year >= 2013, "post", "pre"), levels = c("pre", "post"))
  rec_len <- max(d$year) - min(d$year)
  dN      <- unname(coef(lm(n_obs_sum ~ year, data = d))[2]) * rec_len
  nm      <- c(obs = "n_obs_sum", era = "erapost", year = "year")
  
  rbindlist(lapply(RESP, function(rv) {
    m <- tryCatch(gls(reformulate(c("n_obs_sum", "year", "era"), rv), data = d,
                      correlation = corAR1(form = ~ year), method = "REML"),
                  error = function(e) NULL)
    if (is.null(m)) return(NULL)
    
    abar <- mean(d[[rv]])
    sc   <- c(obs = 100 * dN / abar, era = 100 / abar, year = 100 * rec_len / abar)
    co   <- unname(coef(m)[nm])
    V    <- vcov(m)[nm, nm]
    se   <- sqrt(diag(V))
    dfr  <- nrow(d) - length(coef(m))
    tq   <- qt(0.975, dfr)
    
    rows <- data.table(term = TERMS, b = co, b_lo = co - tq * se, b_hi = co + tq * se,
                       p = unname(summary(m)$tTable[nm, "p-value"]))
    rows[, `:=`(pct_area    = sc[term] * b,
                pct_area_lo = pmin(sc[term] * b_lo, sc[term] * b_hi),
                pct_area_hi = pmax(sc[term] * b_lo, sc[term] * b_hi))]
    
    # data acquisition = obs + era (in % of mean area), CI from the covariance
    g      <- c(sc[["obs"]], sc[["era"]], 0)
    acq    <- sum(g * co)
    acq_se <- sqrt(drop(t(g) %*% V %*% g))
    rows <- rbind(rows, data.table(
      term = "acq", b = NA_real_, b_lo = NA_real_, b_hi = NA_real_,
      p = 2 * pt(-abs(acq / acq_se), dfr),
      pct_area = acq, pct_area_lo = acq - tq * acq_se, pct_area_hi = acq + tq * acq_se))
    
    rows[, `:=`(method = "unadjusted", dataset = ds, buffer_m = buf,
                n_lakes_per_year = mean(d$n_lakes), response = rv)]
    rows
  }))
}
#==========================
# ===== run over buffers
#==========================
res <- rbindlist(lapply(DATASETS, function(ds) rbindlist(lapply(buffers, function(b) {
  cat(sprintf("%s | %s m\n", ds, b))
  fit_buffer(annual_series(buf_annual[dataset == ds & buffer_m == b]), ds, b)
}))))
setcolorder(res, c("method", "dataset", "buffer_m", "n_lakes_per_year", "response", "term"))

print(res, nrows = Inf)


#==========================
# ===== Table
#==========================
fmt   <- function(x) ifelse(is.na(x), "–",
                            ifelse(abs(x) < 1e-3 & x != 0, formatC(x, format = "e", digits = 1),
                                   as.character(signif(x, 2))))
fmt_ci <- function(lo, hi) ifelse(is.na(lo), "–", paste0("[", fmt(lo), ", ", fmt(hi), "]"))

tb <- copy(res)
tb[, `:=`(coef  = fmt(b),
         ci    = fmt_ci(b_lo, b_hi),
         pct   = fmt(pct_area),
         pctci = fmt_ci(pct_area_lo, pct_area_hi),
         pval  = ifelse(p < 0.01, "<0.01", sprintf("%.2f", p)))]

stat_cols <- c("coef", "ci", "pct", "pctci", "pval")
tab <- dcast(tb, response + buffer_m + term ~ dataset, value.var = stat_cols, sep = "|")
tab <- tab[order(factor(response, levels = names(RESP_LABEL)), buffer_m,
                 factor(term, levels = names(TERM_LABEL)))]

tab[, `:=`(Model  = RESP_LABEL[response],
           Buffer = paste0(buffer_m, " m"),
           Term   = TERM_LABEL[term])]
data_cols <- as.vector(t(outer(DATASETS, stat_cols, function(d, s) paste0(s, "|", d))))
tab <- as.data.frame(tab[, c("Model", "Buffer", "Term", data_cols), with = FALSE])

model_starts  <- which(!duplicated(tab$Model))
buffer_starts <- setdiff(which(!duplicated(paste(tab$Model, tab$Buffer))), model_starts)

# show Model once per block, Buffer once per model x buffer
tab$Buffer[duplicated(paste(tab$Model, tab$Buffer))] <- ""
tab$Model[duplicated(tab$Model)] <- ""

# two-level header: dataset on top, statistic below
keys    <- names(tab)
stat_of <- sub("\\|.*$", "", keys)
sub_lab <- dplyr::recode(stat_of, coef = "Coefficient", ci = "95% CI",
                         pct = "% of mean area", pctci = "95% CI", pval = "p")
top_lab <- ifelse(grepl("\\|", keys), sub("^.*\\|", "", keys), sub_lab)
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
  italic(i = 2, j = which(stat_of == "pval"), part = "header") |>
  font(fontname = "Arial", part = "all") |>
  fontsize(size = 8, part = "all") |>
  padding(padding.top = 1, padding.bottom = 1, part = "body") |>
  add_footer_lines(paste(
    "Coefficients from area ~ n_obs_sum + year + era with AR(1) errors (REML); 95% CIs are model-based.",
    "% of mean area: observation frequency = coefficient × change in n_obs_sum over the record;",
    "Landsat 8 = coefficient; year = coefficient × record length;",
    "data acquisition = observation frequency + Landsat 8.")) |>
  fontsize(size = 7, part = "footer") |>
  autofit()

ft

save_as_docx(ft, path = "buffer_sensitivity_table.docx",
             pr_section = prop_section(page_size = page_size(orient = "landscape")))


#==========================
# ===== Figure
#==========================
fntsize   <- 40
ds_cols   <- c("GLAD" = "#2a5674", "GSWO" = "#68abb8")
term_labs <- c(obs  = "Observation\nfrequency",
               era  = "Introduction of\nLandsat 8",
               acq  = "Changes in\ndata acquisition",
               year = "Temporal trend")
resp_rows <- c(total = "Total", median = "Median", mean = "Mean")
y_breaks  <- c(-150, -100, -75, -50, -25, -10, -5, 0, 5, 10, 25, 50, 100, 150)

dB <- res[term %in% names(term_labs)]
dB[, `:=`(xg      = factor(buffer_m, levels = buffers),
          term    = factor(term, levels = names(term_labs), labels = term_labs),
          dataset = factor(dataset, levels = names(ds_cols)))]

panel <- function(d, tm, lim, top, bottom) {
  dd   <- d[term == tm]
  half <- 0.8 * fntsize / 2
  pad  <- if (grepl("\n", tm)) half else half + 0.8 * fntsize * 0.9 / 2
  ggplot(dd, aes(x = xg, y = pct_area, colour = dataset, group = dataset)) +
    geom_hline(yintercept = 0, colour = "grey60", linewidth = 0.3) +
    geom_pointrange(aes(ymin = pct_area_lo, ymax = pct_area_hi),
                    position = position_dodge(width = 0.6),
                    shape = 16, size = 1.2, linewidth = 0.8) +
    scale_colour_manual(values = ds_cols, name = NULL, drop = FALSE) +
    scale_x_discrete(drop = FALSE) +
    scale_y_continuous(trans = pseudo_log_trans(sigma = 5), breaks = y_breaks,
                       limits = lim) +
    facet_wrap(~ term) +
    labs(x = NULL, y = NULL) +
    theme_bw(base_size = fntsize) +
    theme(panel.grid = element_blank(),
          strip.text = if (top) element_text(face = "bold",
                                             margin = margin(pad, half, pad, half, unit = "pt"))
          else element_blank(),
          strip.background = if (top) element_rect() else element_blank(),
          axis.text.x = if (bottom) element_text() else element_blank())
}

row_of <- function(rv, top, bottom) {
  d   <- dB[response == rv]
  lim <- range(c(d$pct_area, d$pct_area_lo, d$pct_area_hi), na.rm = TRUE)
  wrap_plots(lapply(term_labs, function(tm) panel(d, tm, lim, top, bottom)), nrow = 1)
}

y_lab <- function(rv) wrap_elements(
  textGrob(sprintf("%s lake area\n(%% of 1999\u20132021 average)", resp_rows[[rv]]),
           rot = 90, gp = gpar(fontsize = fntsize * 0.8)), clip = FALSE)

x_lab <- wrap_elements(textGrob("Buffer distance (m)", gp = gpar(fontsize = fntsize)))

design <- "
AB
CD
EF
#G
"

fig_buf <- wrap_plots(A = y_lab("total"),  B = row_of("total",  top = TRUE,  bottom = FALSE),
                      C = y_lab("median"), D = row_of("median", top = FALSE, bottom = FALSE),
                      E = y_lab("mean"),   F = row_of("mean",   top = FALSE, bottom = TRUE),
                      G = x_lab, design = design) +
  plot_layout(widths = c(0.08, 1), heights = c(1, 1, 1, 0.12), guides = "collect") &
  theme(legend.position = "bottom")

fig_buf

ggsave("buff_fig.png",
       fig_buf, width = 30, height = 20, dpi = 300)
