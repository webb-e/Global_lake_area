########
### This code takes a dataset of annual median lake areas (per lake)
### and runs a breakpoint analysis to identify years of change.
### Outputs results as a .parquet with one row per lake_id/dataset/breakpoint.
### Lakes without breakpoints get NA.
### Last updated Sept 11, 2025 by E. Webb
########

library(data.table)
library(arrow)
library(strucchange)

#==========================
# === PATHS ===
#==========================
parquet_path <- '.../annual_lake_medians_dataset'
output_dir   <- ".../breakpoint_dataset"

# Create output directory
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

# List parquet files 
parquet_files <- list.files(parquet_path, pattern = "\\.parquet$", full.names = TRUE, recursive = TRUE)
parquet_files <- parquet_files[!grepl("lake_group=000", parquet_files)]

#==========================
# === FUNCTIONS ===
#==========================

## Function that identifies breakpoints in a single lake’s annual median series
find_breakpoints <- function(median_series) {
  valid <- median_series[!is.na(median_water)]
  
  # Need at least 6 years of data
  if (nrow(valid) < 6) {return(data.table(break_year = NA_integer_))}
  
  # Sort by year
  valid <- valid[order(year)]
  
  y <- valid$median_water
  years <- valid$year
  
  # Run breakpoint analysis
  bp <- tryCatch(suppressWarnings(breakpoints(y ~ 1)), error = function(e) NULL)
  
  if (!is.null(bp) && !all(is.na(bp$breakpoints)) && length(bp$breakpoints) > 0) {
    break_years <- years[bp$breakpoints]
    return(data.table(break_year = as.integer(break_years)))} 
  else {   return(data.table(break_year = NA_integer_))}
}

#==========================
# === MAIN LOOP ===
#==========================

for (file in parquet_files) {
  lake_group_val <- sub(".*group_([0-9]+)\\.parquet$", "\\1", basename(file))
  out_file <- file.path(output_dir, paste0("breakpoint_dataset_group_", lake_group_val, ".parquet"))
  
  if (file.exists(out_file)) {
    message("Skipping already processed lake group: ", lake_group_val)
    next
  }
  
  message("Processing lake group: ", lake_group_val)
  
  dataset <- open_dataset(file, format = "parquet")
  
  dt <- dataset %>%
    filter(year >= 1999, year <= 2021) %>%
    select(lake_id, year, median_water, dataset) %>%
    collect() %>%
    as.data.table()
  
  if (nrow(dt) == 0) next
  
  dt[, lake_id := as.character(lake_id)]
  
  # === Apply breakpoint analysis per lake_id/dataset ===
  results <- dt[, find_breakpoints(.SD), by = .(lake_id, dataset)]
  
  # === Write results ===
  write_parquet(results, out_file)
  
  rm(dt, results)
  gc()
}
