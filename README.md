
This repository is organized into folders as follows:

`Dataset creation` contains the code used to:
  * extract lake areas from the GSWO and GLAD datasets
* collate lake areas into .parquet files organized by basin ID
* calculate the annual median lake area of each lake
* calculate the monthly median lake area of each lake

`Analysis` contains the code used to:
 * calculate globally aggregated lake area trends and quantify the relationship between aggregated lake area and observation frquency and the launch of Landsat 8
* estimate area trends for individual lakes 
* culster months into wet and dry seasons and calculate the proportion of observations that come from wet/dry seasons annually
* quantify the relationship between observation frequency and in the proportion of observations that come from wet seasons
* perform the breakpoint analysis

`Figures` contains the code used to produce the figures

The lake area timeseries, annual medians, and monthly medians are archived through the Arctic Data Center ## here.
