########
### This code takes parquets containing the timeseries of lake area  
### and produces a dataset of monthly median lake areas
### Last updated Sept 11, 2025 by E. Webb
########


library(arrow)
library(data.table)
library(dplyr)

#------------------------
#-- DEFINE DIRECTORIES AND READ IN DATA
#------------------------

# Set path to parquet files and output dataset directory
parquet_path <- ".../lake_area_parquets/"
output_dir <- ".../monthly_lake_medians_dataset"

# List all parquet files
parquet_files <- list.files(parquet_path, pattern = "\\.parquet$", full.names = TRUE, recursive = TRUE)

# Read in climate data
climate_data <- fread('/Users/elizabethwebb/Library/CloudStorage/GoogleDrive-webb.elizabeth.e@gmail.com/My Drive/PostDoc/Landsat_observations/csv_lists/lake_climate_match.csv')
climate_data[, lake_id := as.character(lake_id)]  

# create directory
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

# Process each file individually
for (file in parquet_files) {
  # Extract lake_group value from file path
  lake_group_val <- sub(".*lake_group=([0-9]+)/.*", "\\1", file)
  message("Processing: ",lake_group_val)
  
  # Define schema override
  schema_override <- schema(
    lake_id = string(),
    year = int32(),
    month = int32(),
    water = float64(),
    dataset = string(),
    lake_group = string()
  )
  
  # Read the file as dataset
  dataset <- open_dataset(file, format = "parquet", schema = schema_override)
  
  # Collect relevant data
  dt <- dataset %>%
    filter(year >= 1999, year <= 2021) %>%
    select(lake_id, month, water, dataset) %>%
    collect() %>%
    as.data.table()
  
  if (nrow(dt) == 0) next
  
  # Summarize: median water and number of observations
  dt_summary <- dt[, .(
    median_water = median(water, na.rm = TRUE),
    n_obs = .N
  ), by = .(lake_id, dataset, month)]
  
 
  # Merge with climate data
  dt_merged <- merge(dt_summary, climate_data, by = "lake_id", all.x = TRUE)

  # Write partitioned output by lake_group
  out_file <- file.path(output_dir, paste0("monthly_lake_medians_group_", lake_group_val, ".parquet"))
  write_parquet(dt_merged, out_file)
}
