########
### This code takes the output of L8_extract_lake_area.py 
### (i.e., csvs with lake area observations in a somewhat random order)
### and makes a parquet file for each basin with the lake area time series of each lake within that basin.
### Last updated Sept 7, 2025 by E. Webb
########

library(arrow)
library(readr)
library(data.table)
library(fs)

#------------------------
#-- DEFINE DIRECTORIES AND HELPER FUNCTION
#------------------------

## where csvs are stored
area_csvs<-dir_ls('...' , glob = "*.csv", recurse = FALSE)

## where parquets will be stored
parquet_dir <- '...'

## extracts basin ID from lake_id
get_lake_group <- function(lake_id) {substr(as.character(lake_id), 1, 3)}

#------------------------
#-- MAIN FUNCTION
#------------------------

for (csv_file in area_csvs) {
  # Read CSV and clean lake_id
  table_new <- read_csv_arrow(csv_file) |> collect()
  dt_new <- as.data.table(table_new)
  dt_new[, lake_id := as.character(round(as.numeric(lake_id)))]
  dt_new[, lake_group := substr(lake_id, 1, 3)]
  
  # Process each lake_group separately
  for (grp in unique(dt_new$lake_group)) {
    dt_grp_new <- dt_new[lake_group == grp]
    
    # Define partition path
    partition_path <- file.path(parquet_dir, paste0("lake_group=", grp))
    
    if (dir_exists(partition_path)) {
      # Read existing data
      existing <- open_dataset(partition_path, format = "parquet") |> collect()
      dt_existing <- as.data.table(existing)
      
      # Combine and make sure there are no duplicates
      dt_combined <- rbindlist(list(dt_existing, dt_grp_new), use.names = TRUE)
      setkeyv(dt_combined, c("lake_id", "year", "month", "dataset"))
      dt_combined <- unique(dt_combined)
      
      # Overwrite this partition
      write_dataset(
        dataset = dt_combined,
        path = partition_path,
        format = "parquet",
        existing_data_behavior = "overwrite"
      )
    } else {
      # Write new partition
      write_dataset(
        dataset = dt_grp_new,
        path = partition_path,
        format = "parquet"
      )
    }
  }
}