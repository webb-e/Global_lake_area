`extract_lake_area.py` extracts lake area for every lake within the Prior Lakes Database (PLD) for the period 1999-2021 for both the GSWO and GLAD datasets.

`area_csvs_to_parquets.R` collates the csvs ouptut from extract_lake_area.py and creates collated .parquet files of the lake area timeseries.

`create_annual_medians_dataset.R` uses the lake area timeseries to calculate annual median area for each lake

`create_monthly_medians_dataset.R` uses the lake area timeseries to calculate monthly median area for each lake
