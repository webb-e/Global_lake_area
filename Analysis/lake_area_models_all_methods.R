#==============================================================================
# Effects of observation frequency, Landsat 8, and the long-term trend on
# aggregated lake area (total, mean, median), estimated three ways:
#
#   unadjusted           : all lakes, full series
#   complete_record      : only lakes included in every year of the record
#   composition_adjusted : all lakes, full series minus area-constant series
#                          (area-constant = each lake held at its long-term
#                          median area, with the year-to-year set of included
#                          lakes kept)
#
# Each method is run for all lakes, by lake size class, and by climate zone,
# for GSWO and GLAD, 1999-2021, with a paired spatial block bootstrap
# (500 km blocks, 200 replicates).
#
# Model (all methods): area ~ n_obs_sum + year + era,  corAR1(~year), REML
#   n_obs_sum = total valid observations across included lakes per year
#   era       = pre-2013 vs 2013 onward (introduction of Landsat 8)
#
# Scaling to % of mean lake area (full series) over the record:
#   obs  : coef x change in n_obs_sum over the record (linear trend)
#   era  : coef
#   year : coef x record length
#   acq  : obs + era (combined effect of changes in data acquisition),
#          summed within each bootstrap replicate so its CI is valid
#
# Collinearity: variance inflation factors (VIF) for n_obs_sum, year and era
# are computed on the unresampled predictors of every model. Unadjusted and
# composition-adjusted models share predictors, so their VIFs are identical.
#
# Outputs (csv_dir):
#   {method}_all_lakes_obs-sum.csv, {method}_by_size_obs-sum.csv,
#   {method}_by_zone_obs-sum.csv, vif_by_group.csv, size_labels.csv
#==============================================================================

library(DBI)
library(duckdb)
library(data.table)
library(sf)
library(nlme)

#==========================
# ===== settings
#==========================
# "all", or any of "unadjusted", "complete_record", "composition_adjusted"
METHODS <- "all"

parquet_path  <- '/Users/elizabethwebb/Library/CloudStorage/Box-Box/Landsat8/annual_lake_medians_dataset'
grid_shp_path <- '/Volumes/EW_external/Postdoc_Duke/Landsat_observations/global_grid/Grid_global_50km.shp'
spill_dir     <- '/Volumes/EW_external/Global_landsat/duckdb_spill'
csv_dir       <- '/Users/elizabethwebb/Library/CloudStorage/Box-Box/Landsat8/csvs'

DATASETS    <- c("GSWO", "GLAD")
YEARS       <- 1999:2021
BK          <- 500
N_BOOT      <- 200
SEED        <- 1
RESP        <- c("total", "mean", "median")
TERMS       <- c("obs", "era", "year")
MIN_BLOCKS  <- 20
SIZE_BREAKS <- c(0, 0.1, 1, 10, 100, Inf)
ALL_LABEL   <- "All lakes"

# climate_zone code -> name (6 = unclassified -> NA)
ZONE_LABELS <- c(`1` = "Tropical", `2` = "Dry", `3` = "Temperate", `4` = "Continental", `5` = "Polar")
ZONE_ORDER  <- c("Polar", "Continental", "Temperate", "Tropical", "Dry")

ALL_METHODS <- c("unadjusted", "complete_record", "composition_adjusted")
RUN <- if (identical(METHODS, "all")) ALL_METHODS else intersect(ALL_METHODS, METHODS)
if (!length(RUN)) stop("METHODS must be 'all' or any of: ", paste(ALL_METHODS, collapse = ", "))

# methods grouped by the lake set they use
LAKE_SETS <- list(all      = intersect(RUN, c("unadjusted", "composition_adjusted")),
                  complete = intersect(RUN, "complete_record"))
LAKE_SETS <- LAKE_SETS[lengths(LAKE_SETS) > 0]

#==========================
# ===== size-class and zone SQL
#==========================
nsz <- length(SIZE_BREAKS) - 1
SIZE_LABS_GRP <- sprintf("%s-%s km2", SIZE_BREAKS[-(nsz + 1)], SIZE_BREAKS[-1])
SIZE_LABS_GRP[1]   <- sprintf("<%s km2", SIZE_BREAKS[2])
SIZE_LABS_GRP[nsz] <- sprintf(">%s km2", SIZE_BREAKS[nsz])
size_case <- paste0("CASE ",
                    paste(sprintf("WHEN f.w_frz < %s THEN '%s'", SIZE_BREAKS[2:nsz], SIZE_LABS_GRP[1:(nsz - 1)]), collapse = " "),
                    sprintf(" ELSE '%s' END", SIZE_LABS_GRP[nsz]))
zone_case <- paste0("CASE f.zc ",
                    paste(sprintf("WHEN %s THEN '%s'", names(ZONE_LABELS), ZONE_LABELS), collapse = " "), " END")

#==========================
# ===== grid cells -> spatial blocks
#==========================
g  <- st_read(grid_shp_path, quiet = TRUE)
if (isTRUE(st_is_longlat(g))) g <- st_transform(g, 6933)
xy <- st_coordinates(suppressWarnings(st_centroid(st_geometry(g)))) / 1000
grid_blk <- unique(data.table(
  grid_id = as.integer(g$id),
  block   = as.integer(round(xy[, 1] / BK)) * 100000L + as.integer(round(xy[, 2] / BK))
)[!is.na(grid_id)], by = "grid_id")
rm(g, xy)

#==========================
# ===== DuckDB
#==========================
dir.create(spill_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(csv_dir,   recursive = TRUE, showWarnings = FALSE)
con <- dbConnect(duckdb())
dbExecute(con, "SET memory_limit = '6GB'")
dbExecute(con, sprintf("SET temp_directory = '%s'", spill_dir))
duckdb_register(con, "grid_blk", grid_blk)

files <- list.files(parquet_path, pattern = "^[^.].*\\.parquet$", full.names = TRUE, recursive = TRUE)
dbExecute(con, sprintf("
  CREATE VIEW rawv AS
  SELECT lake_id, dataset, year, CAST(grid_id AS INTEGER) AS grid_id, n_obs, climate_zone,
         median_water / 1e6 AS w
  FROM read_parquet([%s], union_by_name = true)
  WHERE year BETWEEN %d AND %d AND n_obs > 0 AND isfinite(median_water)",
                       paste0("'", files, "'", collapse = ", "), min(YEARS), max(YEARS)))

#==========================
# ===== helpers
#==========================
fit_terms <- function(d, resp) {
  d$era <- factor(ifelse(d$year >= 2013, "post", "pre"), levels = c("pre", "post"))
  m <- tryCatch(gls(reformulate(c("n_obs_sum", "year", "era"), resp), data = d,
                    correlation = corAR1(form = ~ year), method = "REML"),
                error = function(e) NULL)
  if (is.null(m)) return(c(obs = NA_real_, era = NA_real_, year = NA_real_))
  co <- coef(m)
  c(obs = unname(co["n_obs_sum"]), era = unname(co["erapost"]), year = unname(co["year"]))
}

# weighted median for every bootstrap replicate; v sorted ascending
wmed_all <- function(v, bi, W) {
  vapply(seq_len(ncol(W)), function(r) {
    cw <- cumsum(W[bi, r]); v[which.max(cw >= cw[length(cw)] / 2)]
  }, numeric(1))
}

# VIF of each predictor = diagonal of the inverse correlation matrix
vif_of <- function(yrs, n_obs_sum) {
  X <- cbind(obs = n_obs_sum, year = yrs, era = as.numeric(yrs >= 2013))
  v <- tryCatch(diag(solve(cor(X))), error = function(e) c(obs = NA, year = NA, era = NA))
  setNames(as.list(v), paste0("vif_", colnames(X)))
}

# one row per term; b_all and pct_all have column 1 = unresampled, then replicates
out_row <- function(method, ds, val, nb, nl, rv, tm, b_all, pct_all) {
  ok <- is.finite(pct_all[-1])
  q  <- function(x) quantile(x[-1][ok], c(0.025, 0.975), names = FALSE)
  qb <- if (all(is.na(b_all))) c(NA_real_, NA_real_) else q(b_all)
  qp <- q(pct_all)
  data.table(method = method, obs_var = "sum", dataset = ds, group = val,
             n_blocks = nb, n_lakes_per_year = nl, response = rv, term = tm,
             b = b_all[1], b_lo = qb[1], b_hi = qb[2],
             pct_area = pct_all[1], pct_area_lo = qp[1], pct_area_hi = qp[2],
             n_boot = sum(ok))
}

# lake-year table: block, obs, area, long-term median area, size class, zone,
# and a flag for lakes included in every year
build_lake_table <- function(ds) {
  dbExecute(con, "DROP TABLE IF EXISTS lake_t")
  dbExecute(con, sprintf("
    CREATE TEMP TABLE lake_t AS
    WITH d  AS (SELECT r.lake_id, r.year, r.n_obs, r.w, r.climate_zone, g.block
                FROM rawv r JOIN grid_blk g ON r.grid_id = g.grid_id
                WHERE r.dataset = '%s'),
         f  AS (SELECT lake_id, median(w) AS w_frz, mode(climate_zone) AS zc,
                       count(DISTINCT year) AS nyr
                FROM d GROUP BY lake_id),
         ny AS (SELECT count(DISTINCT year) AS n FROM d)
    SELECT d.year, d.block, d.n_obs, d.w, f.w_frz,
           %s AS size_class, %s AS zone,
           (f.nyr = ny.n) AS complete
    FROM d JOIN f ON d.lake_id = f.lake_id CROSS JOIN ny", ds, size_case, zone_case))
}

#==========================
# ===== one dataset x one lake set x one group
#   col      = "all", "size_class", or "zone"
#   lake_set = "all" (unadjusted and/or composition_adjusted) or "complete"
#==========================
run_group <- function(ds, col, val, lake_set, methods) {
  wh <- if (col == "all") "TRUE" else sprintf("%s = '%s'", col, gsub("'", "''", val))
  if (lake_set == "complete") wh <- paste(wh, "AND complete")
  need_adj <- "composition_adjusted" %in% methods

  A <- setDT(dbGetQuery(con, sprintf("
    SELECT block, year, sum(n_obs)::DOUBLE AS n_obs_sum, count(*)::DOUBLE AS n_lakes,
           sum(w) AS tot_real, sum(w_frz) AS tot_frz
    FROM lake_t WHERE %s GROUP BY block, year", wh)))
  if (!nrow(A)) return(NULL)
  blocks <- sort(unique(A$block)); nb <- length(blocks)
  yrs    <- sort(unique(A$year));  ny <- length(yrs)
  if (nb < MIN_BLOCKS || !any(yrs < 2013) || !any(yrs >= 2013)) {
    cat(sprintf("  %s | %s | %s = %s skipped (%d blocks)\n", ds, lake_set, col, val, nb)); return(NULL)
  }
  A[, `:=`(bi = match(block, blocks), yi = match(year, yrs))]

  # block weights: column 1 = unresampled, then N_BOOT replicates
  set.seed(SEED)
  W <- cbind(1L, vapply(seq_len(N_BOOT), function(b)
    tabulate(sample.int(nb, nb, replace = TRUE), nbins = nb), integer(nb)))

  mk <- function(cl) { M <- matrix(0, ny, nb); M[cbind(A$yi, A$bi)] <- A[[cl]]; M %*% W }
  NO <- mk("n_obs_sum"); NL <- mk("n_lakes"); TR <- mk("tot_real"); TF <- mk("tot_frz")

  # medians of lake area (full and, if needed, area-constant)
  MR <- MF <- matrix(NA_real_, ny, ncol(W))
  for (i in seq_len(ny)) {
    x  <- dbGetQuery(con, sprintf("SELECT block, w, w_frz FROM lake_t WHERE %s AND year = %d", wh, yrs[i]))
    bi <- match(x$block, blocks)
    o  <- order(x$w); MR[i, ] <- wmed_all(x$w[o], bi[o], W); MR[i, 1] <- median(x$w)
    if (need_adj) {
      o <- order(x$w_frz); MF[i, ] <- wmed_all(x$w_frz[o], bi[o], W); MF[i, 1] <- median(x$w_frz)
    }
    rm(x, bi, o)
  }

  rec_len <- max(yrs) - min(yrs)
  dN      <- unname(coef(lm(NO[, 1] ~ yrs))[2]) * rec_len
  abar    <- c(total = mean(TR[, 1]), mean = mean(TR[, 1] / NL[, 1]), median = mean(MR[, 1]))
  nl      <- mean(NL[, 1])

  series <- function(r, adj) {
    if (adj) data.frame(year = yrs, n_obs_sum = NO[, r],
                        total  = TR[, r] - TF[, r],
                        mean   = (TR[, r] - TF[, r]) / NL[, r],
                        median = MR[, r] - MF[, r])
    else     data.frame(year = yrs, n_obs_sum = NO[, r],
                        total  = TR[, r],
                        mean   = TR[, r] / NL[, r],
                        median = MR[, r])
  }

  res <- rbindlist(lapply(methods, function(mt) {
    adj <- mt == "composition_adjusted"
    B <- t(vapply(seq_len(ncol(W)), function(r) {
      d <- series(r, adj)
      unlist(lapply(RESP, function(rv) fit_terms(d, rv)))
    }, numeric(length(TERMS) * length(RESP))))
    colnames(B) <- paste(rep(RESP, each = length(TERMS)), TERMS, sep = ".")

    rbindlist(lapply(RESP, function(rv) {
      sc  <- c(obs  = 100 * dN / abar[[rv]],
               era  = 100 / abar[[rv]],
               year = 100 * rec_len / abar[[rv]])
      bc  <- function(tm) B[, paste(rv, tm, sep = ".")]
      rows <- lapply(TERMS, function(tm)
        out_row(mt, ds, val, nb, nl, rv, tm, bc(tm), sc[[tm]] * bc(tm)))
      acq <- sc[["obs"]] * bc("obs") + sc[["era"]] * bc("era")
      rows[[length(rows) + 1]] <- out_row(mt, ds, val, nb, nl, rv, "acq",
                                          rep(NA_real_, ncol(W)), acq)
      rbindlist(rows)
    }))
  }))

  vif <- data.table(lake_set = lake_set, dataset = ds, grouping = col, group = val,
                    as.data.table(vif_of(yrs, NO[, 1])))

  cat(sprintf("  %s | %s | %s = %s done (%d blocks, ~%.0f lakes/yr)\n", ds, lake_set, col, val, nb, nl))
  list(res = res[, grouping := col], vif = vif)
}

#==========================
# ===== run
#==========================
out <- list(); vifs <- list()
for (ds in DATASETS) {
  cat(ds, ": building lake table ...\n")
  build_lake_table(ds)

  sizes <- intersect(SIZE_LABS_GRP, dbGetQuery(con, "SELECT DISTINCT size_class FROM lake_t")$size_class)
  zones <- dbGetQuery(con, "SELECT DISTINCT zone FROM lake_t WHERE zone IS NOT NULL")$zone
  zones <- c(intersect(ZONE_ORDER, zones), setdiff(zones, ZONE_ORDER))
  groups <- rbind(data.table(col = "all",        val = ALL_LABEL),
                  data.table(col = "size_class", val = sizes),
                  data.table(col = "zone",       val = zones))

  for (ls in names(LAKE_SETS)) for (i in seq_len(nrow(groups))) {
    r <- run_group(ds, groups$col[i], groups$val[i], ls, LAKE_SETS[[ls]])
    if (is.null(r)) next
    out[[length(out) + 1]]   <- r$res
    vifs[[length(vifs) + 1]] <- r$vif
  }

  dbExecute(con, "DROP TABLE lake_t")
}
dbDisconnect(con, shutdown = TRUE)

out <- rbindlist(out)
vif <- rbindlist(vifs)

#==========================
# ===== collinearity check
#==========================
vif_cols <- c("vif_obs", "vif_year", "vif_era")
cat("\nMaximum VIF by lake set, dataset and grouping:\n")
print(vif[, .(max_vif = max(unlist(.SD), na.rm = TRUE)), by = .(lake_set, dataset, grouping), .SDcols = vif_cols])
cat(sprintf("\nOverall maximum VIF: %.2f\n", max(unlist(vif[, ..vif_cols]), na.rm = TRUE)))

#==========================
# ===== save
#==========================
for (mt in RUN) {
  fwrite(out[method == mt & grouping == "all",        !"grouping"],
         file.path(csv_dir, sprintf("%s_all_lakes_obs-sum.csv", mt)))
  fwrite(out[method == mt & grouping == "size_class", !"grouping"],
         file.path(csv_dir, sprintf("%s_by_size_obs-sum.csv", mt)))
  z <- out[method == mt & grouping == "zone", !"grouping"]; setnames(z, "group", "zone")
  fwrite(z, file.path(csv_dir, sprintf("%s_by_zone_obs-sum.csv", mt)))
}
fwrite(vif, file.path(csv_dir, "vif_by_group.csv"))
fwrite(data.table(size_label = SIZE_LABS_GRP), file.path(csv_dir, "size_labels.csv"))
