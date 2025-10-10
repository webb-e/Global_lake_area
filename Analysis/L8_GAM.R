##########
##### This file fits two GAMs to each lake area time series, one that includes the effect of the L8 launch and
##### observation frequency and one that does not. 
##### It processes lake groups in parallel (chunking by 1000 lakes within each group) and outputs to a parquet.
##### Last updated by E. Webb Sept 7, 2025
#########

library(data.table)
library(arrow)
library(mgcv)
library(nlme)
library(future.apply)
library(dplyr)

#==========================
# ==== SETUP ====
#==========================

plan(multisession, workers = 10)
options(future.globals.maxSize = 2 * 1024^3)
chunksize <- 1000

# Paths
base_path <- ".../lake_area_parquets/"
group_dirs <- list.dirs(base_path, recursive = FALSE, full.names = FALSE)
group_ids <- sub("lake_group=", "", group_dirs)

# Some options if you only want to run specific groups
group_ids_numeric <- suppressWarnings(as.numeric(group_ids))
valid_idx <- !is.na(group_ids_numeric) & group_ids_numeric > 000 & group_ids != "000"
group_ids <- group_ids[valid_idx]

#==========================
# ==== MAIN FUNCTION  ====
#==========================
bias_analysis_function <- function(dt) {
  dt <- copy(dt)
  
  lake_id_val <- dt$lake_id[1]
  dataset_val <- dt$dataset[1]
  
  unique_months <- uniqueN(dt$month)
  unique_years <- uniqueN(dt$year)
  n_unique <- uniqueN(dt, by = c("year", "month"))
  n_pre_2013 <- nrow(dt[year < 2013])
  n_post_2013 <- nrow(dt[year >= 2013])
  
  # Minimal data requirement
  if (n_pre_2013 < 5 || n_post_2013 < 5) {
    return(data.table(
      lake_id = lake_id_val, dataset = dataset_val,
      n_years = unique_years, n_months = unique_months,
      n_observations = n_unique, n_pre_2013 = n_pre_2013, n_post_2013 = n_post_2013,
      year_coef_full = NA_real_, year_pval_full = NA_real_,
      year_coef_basic = NA_real_, year_pval_basic = NA_real_))}
  
  # Add additional variables
  dt[, `:=`( period = factor(ifelse(year >= 2013, "post", "pre"), levels = c("pre", "post")),
    time_numeric = year + month / 12,
    n_obs = .N, by = year )]
  
  # Initialize outputs
  year_coef_full <- year_pval_full <- NA_real_
  year_coef_basic <- year_pval_basic <- NA_real_
  
  # Full model
  full_model <- try(gam(water ~ s(month, k = unique_months) + period + year + n_obs,
        data = dt, correlation = corCAR1(form = ~ time_numeric), method = "REML"),silent = TRUE)
  
  if (!inherits(full_model, "try-error")) {
    coef_summary <- summary(full_model)$p.table
    if ("year" %in% rownames(coef_summary)) {
      year_coef_full <- coef_summary["year", "Estimate"]
      year_pval_full <- coef_summary["year", "Pr(>|t|)"]}}
  
  # Basic model
  basic_model <- try(gam(water ~ s(month, k = unique_months) + year, data = dt, correlation = corCAR1(form = ~ time_numeric),
                         method = "REML"), silent = TRUE)
  if (!inherits(basic_model, "try-error")) {
    coef_summary <- summary(basic_model)$p.table
    if ("year" %in% rownames(coef_summary)) {
      year_coef_basic <- coef_summary["year", "Estimate"]
      year_pval_basic <- coef_summary["year", "Pr(>|t|)"]  } }
  
  return(data.table(
    lake_id = lake_id_val, dataset = dataset_val,
    n_years = unique_years, n_months = unique_months, n_observations = n_unique,
    n_pre_2013 = n_pre_2013, n_post_2013 = n_post_2013,
    year_coef_full, year_pval_full,
    year_coef_basic, year_pval_basic))
}

#==========================================
# ==== PROCESS LAKE GROUPS IN PARALLEL ====
#==========================================
for (group_id in group_ids) {
  cat("\n🔍 Starting group:", group_id, "\n")
  tryCatch({
    thisgroup_path <- file.path(base_path, paste0("lake_group=", group_id))
    output_file <- file.path('...', paste0("GAMresults_group_", group_id, ".parquet"))
    
    schema_override <- schema(
      lake_id = string(),
      year = int32(),
      month = int32(),
      water = float64(),
      dataset = string(),
      lake_group = string() )
    
    dataset <- open_dataset(thisgroup_path, format = "parquet", schema = schema_override)
    dt_all <- dataset %>%
      filter(year >= 1999, year <= 2021) %>%
      select(lake_id, dataset) %>%
      distinct() %>%
      collect() %>%
      as.data.table()
    
    if (nrow(dt_all) == 0) stop("No valid data found.")
    
    existing_results <- tryCatch({
      read_parquet(output_file) %>%
        select(lake_id, dataset) %>%
        unique() }, error = function(e) {
      data.table(lake_id = character(), dataset = character()) })
    
    combos <- unique(dt_all[, .(lake_id, dataset)])
    if (nrow(existing_results) > 0) {
      combos <- fsetdiff(combos, existing_results) }
    
    if (nrow(combos) == 0) {
      cat("All combinations already processed for group", group_id, "\n")
      next }
    
    total_combos <- nrow(combos)
    chunks <- split(combos, ceiling(seq_len(total_combos) / chunksize))
    cat("Split", total_combos, "combinations into", length(chunks), "chunks\n")
    
    for (chunk_num in seq_along(chunks)) {
      chunk <- chunks[[chunk_num]]
      cat("Processing chunk", chunk_num, "of", length(chunks), "(", nrow(chunk), "combos)\n")
      
      chunk_results <- future_lapply(1:nrow(chunk), function(i) {
        library(arrow)
        library(dplyr)
        library(data.table)
        
        schema_override <- arrow::schema(
          lake_id = arrow::string(),
          year = arrow::int32(),
          month = arrow::int32(),
          water = arrow::float64(),
          dataset = arrow::string(),
          lake_group = arrow::string() )
        
        lid <- chunk$lake_id[i]
        dset <- chunk$dataset[i]
        
        subset_dt <- arrow::open_dataset(thisgroup_path, format = "parquet", schema = schema_override) %>%
          filter(year >= 1999, year <= 2021,
                 lake_id == lid,
                 dataset == dset) %>%
          collect() %>%
          as.data.table()
        
        if (nrow(subset_dt) == 0) return(NULL)
        
        subset_dt[, `:=`(
          period = factor(ifelse(year >= 2013, "post", "pre"), levels = c("pre", "post")),
          time_numeric = year + month / 12,
          time_id = factor(paste(year, month, sep = "-")),
          lake_id = as.character(lake_id),
          dataset = as.character(dataset) )]
        
        bias_analysis_function(subset_dt)
      }, future.packages = c("data.table", "arrow", "dplyr", "mgcv", "nlme"))
      
      result_dt <- rbindlist(Filter(Negate(is.null), chunk_results), fill = TRUE)
      
      if (nrow(result_dt) > 0) {
        if (file.exists(output_file)) {
          existing_dt <- read_parquet(output_file) %>% as.data.table()
          result_dt <- rbindlist(list(existing_dt, result_dt), use.names = TRUE, fill = TRUE)}
        
        write_parquet(result_dt, output_file)}
      
      rm(result_dt, chunk_results)
      gc()
    }
    
    cat(" Completed analysis for group", group_id, "\n")
    
  }, error = function(e) {
    cat("Error in group", group_id, ":", conditionMessage(e), "\n")
  })
}
