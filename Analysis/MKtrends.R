library(data.table)
library(arrow)
library(future.apply)
library(EnvStats)
#=========================================
# Setup
#=========================================
input_dir  <- ".../annual_lake_medians_dataset"
output_dir <- ".../MK_results"

dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

parquet_files <- list.files(input_dir, pattern = "\\.parquet$", full.names = TRUE)
plan(multisession, workers = 8)  
#=========================================
# Function: Mann–Kendall + Sen’s slope
#=========================================
mk_sen_function <- function(dt) {
  dt <- copy(dt)[order(year)]
  lake_id_val <- dt$lake_id[1]
  dataset_val <- dt$dataset[1]
  
  if (nrow(dt) < 3) {
    return(data.table(
      lake_id = lake_id_val,
      dataset = dataset_val,
      tau = NA_real_,
      p_value = NA_real_,
      sen_slope = NA_real_))
  }
  
  fit <- EnvStats::kendallTrendTest(median_water ~ year, data = dt)
  
  return(data.table(
    lake_id = lake_id_val,
    dataset = dataset_val,
    tau = fit$estimate[["tau"]],
    p_value = fit$p.value,
    sen_slope = fit$estimate[["slope"]]
  ))
}


#=========================================
# Parallel process
#=========================================
future_lapply(parquet_files, function(file) {
  library(data.table)
  library(arrow)
  library(EnvStats)
  
  lake_group_val <- sub(".*group_([0-9]+)\\.parquet$", "\\1", basename(file))
  out_file <- file.path(output_dir, paste0("MKresults_group_", lake_group_val, ".parquet"))
  
  if (file.exists(out_file)) {
    message("Skipping already processed lake group: ", lake_group_val)
    return(NULL)
  }
  
  message("Processing lake group: ", lake_group_val)
  
  dt <- read_parquet(file) |> as.data.table()
  dt <- dt[year >= 1999 & year <= 2021, .(lake_id, year, median_water, dataset)]
  
  if (nrow(dt) == 0) return(NULL)
  
  dt[, lake_id := as.character(lake_id)]
  dt[, dataset := as.character(dataset)]
  
  results <- dt[, mk_sen_function(.SD), by = .(lake_id, dataset)]
  
  # Write results
  write_parquet(results, out_file)
  rm(dt, results)
  gc()
  return(NULL)
}, future.packages = c("data.table", "arrow", "EnvStats"))
