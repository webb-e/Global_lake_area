# Global_lake_area
Companion code to the paper *Seminal global surface water datasets are temporally biased* 

This repository is organized into folders as follows:

`Dataset creation` contains the code used to:
  (1) extract lake areas from the GSWO and GLAD datasets
  (2) collate lake areas into .parquet files organized by basin ID
  (3) calculate the annual median lake area of each lake
  (4) calculate the monthly median lake area of each lake

`Analysis` contains the code used to:
  (1) calculate globally aggregated lake area trends and quantify the relationship between aggregated lake area and observation frquency and the launch of Landsat 8
  (2) estimate area trends for individual lakes 
  (3) culster months into wet and dry seasons and calculate the proportion of observations that come from wet/dry seasons annually
  (4) quantify the relationship between observation frequency and in the proportion of observations that come from wet seasons
  (5) perform the breakpoint analysis

`Figures` contains the code used to produce the figures
