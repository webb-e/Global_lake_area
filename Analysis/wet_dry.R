########
#### This code takes a dataset of monthly median lake areas (per lake) and runs a k-means clustering algorithm to
#### identify wet and dry months. Outputs results as a .parquet
#### Last updated Sept 8, 2025 by E. Webb
########

library(arrow)
library(data.table)
library(dplyr)

########
### Files to be processed
#########
# Set path to parquet files and output dataset directory
parquet_path <- "/Users/elizabethwebb/Library/CloudStorage/GoogleDrive-webb.elizabeth.e@gmail.com/My Drive/PostDoc/Landsat8/lake_area_parquets/"
output_dir <- "/Users/elizabethwebb/Library/CloudStorage/GoogleDrive-webb.elizabeth.e@gmail.com/My Drive/PostDoc/Landsat8/seasonal_dataset"

# create directory
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

# List all parquet files
parquet_files <- list.files(parquet_path, pattern = "\\.parquet$", full.names = TRUE, recursive = TRUE)
parquet_files <- parquet_files[!grepl("lake_group=000", parquet_files)]

## climate_zone data
climate_data <- fread('/Users/elizabethwebb/Library/CloudStorage/GoogleDrive-webb.elizabeth.e@gmail.com/My Drive/PostDoc/Landsat_observations/csv_lists/lake_climate_match.csv')
climate_data[, lake_id := as.character(lake_id)]

########
### Function that determines wet/dry months for each lake using k-means clustering
#########

summarize_seasonality <- function(monthly_medians) {
  library(data.table)
  
  # Function to classify seasons for a single lake-dataset group
  classify_seasons <- function(median_water) {
    valid_water <- median_water[!is.na(median_water)]
    
    # Check if we have enough data for clustering
    if (length(valid_water) < 2 || length(unique(valid_water)) < 2) {
      return(rep(NA_character_, length(median_water)))
    }
    
    tryCatch({
      # Perform k-means clustering
      km <- kmeans(valid_water, centers = 2, nstart = 10)
      
      # Assign clusters based on water level (higher = wet, lower = dry)
      cluster_means <- tapply(valid_water, km$cluster, mean)
      wet_cluster <- which.max(cluster_means)
      
      # Create season labels for all months (including NAs)
      seasons <- rep(NA_character_, length(median_water))
      seasons[!is.na(median_water)] <- ifelse(km$cluster == wet_cluster, "wet", "dry")
      
      return(seasons)
    }, error = function(e) {
      return(rep(NA_character_, length(median_water)))
    })
  }
  
  # Apply seasonality classification by to all lake_ids/groups
  monthly_labeled <- monthly_medians[,  season := classify_seasons(median_water), 
                                     by = .(lake_id, dataset)]

  # Compute seasonal proportions per year 
  season_summary <- monthly_labeled[, {
    total <- .N
    wet_count <- sum(season == "wet", na.rm = TRUE)
    dry_count <- sum(season == "dry", na.rm = TRUE)
    
    list(
      n_total = total,
      n_wet = wet_count,
      n_dry = dry_count,
      prop_wet = if (total > 0) wet_count / total else NA_real_,
      prop_dry = if (total > 0) dry_count / total else NA_real_
    )
  }, by = .(lake_id, dataset, year)]
  
  return(list(
    season_summary = season_summary,
    wet_dry_months = monthly_labeled
  ))
}
########
### Function that loops through parquet files, runs the summarize_seasonality function
### and outputs to a parquet file
#########

for (file in parquet_files) {
  lake_group_val <- sub(".*lake_group=([0-9]+)/.*", "\\1", file)
  out_file <- file.path(output_dir, paste0("wetdry_dataset_group_", lake_group_val, ".parquet"))
  
  if (file.exists(out_file)) {
    message("Skipping already processed lake group: ", lake_group_val)
    next
  }
  
  message("Processing lake group: ", lake_group_val)
  
  schema_override <- schema(
    lake_id = string(),
    year = int32(),
    month = int32(),
    water = float64(),
    dataset = string(),
    lake_group = string())
  
  dataset <- open_dataset(file, format = "parquet", schema = schema_override)
  
  dt <- dataset %>%
    filter(year >= 1999, year <= 2021) %>%
    select(lake_id, month, water, dataset, year) %>%
    collect() %>%
    as.data.table()
  
  if (nrow(dt) == 0) next
  
  dt[, lake_id := as.character(lake_id)]
  
  # Calculate monthly medians first
  monthly_medians <- dt[, .(
    median_water = median(water, na.rm = TRUE)
  ), by = .(lake_id, dataset, year, month)]
  
  # Apply seasonality analysis
  dt_summary <- summarize_seasonality(monthly_medians)
  season_summary <- dt_summary$season_summary
  summary_combined <- merge(season_summary, climate_data, by = "lake_id", all.x = TRUE)
  
  
  write_parquet(summary_combined, out_file)
}
