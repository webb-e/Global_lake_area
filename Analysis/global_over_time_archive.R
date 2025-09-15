########
### This code takes the annual median lake areas, generates annual total, mean, and median annual lake areas, 
### and produces basic statistics on how they relate to observation frequency.
###
### Last updated Sept 7, 2025 by E. Webb
########

library(arrow)
library(tidyverse)
library(data.table)
library(scales)
library(patchwork)

#==========================
# ===== read in files
#==========================
# Set the file path
parquet_path <- "...annual_lake_medians_dataset/"  
parquet_files <- list.files(parquet_path, pattern = "\\.parquet$", full.names = TRUE, recursive = TRUE)

#########
## Read in GSWO data
#########
read_filtered <- function(path) {
  ds <- open_dataset(path)
  filtered <- ds %>%
    filter(dataset == "GSWO") %>%
    collect()
  as.data.table(filtered)}

gswo_dt <- rbindlist(lapply(parquet_files, read_filtered), use.names = TRUE, fill = TRUE)

#########
## Read in GLAD data
#########
read_filtered_GLAD <- function(path) {
  ds <- open_dataset(path)
  filtered <- ds %>%
    filter(dataset == "GLAD") %>%
    collect()
  as.data.table(filtered)}

glad_dt <- rbindlist(lapply(parquet_files, read_filtered_GLAD), use.names = TRUE, fill = TRUE)

#==========================
# ===== data wrangling
#=========================

#########
## zonal summary GSWO 
#########
zone_stats_GSWO <- gswo_dt[, .(
  median_water_median = median(median_water, na.rm = TRUE),
  median_water_mean   = mean(median_water, na.rm = TRUE),
  median_water_sum    = sum(median_water, na.rm = TRUE),
  n_obs_median        = median(n_obs, na.rm = TRUE),
  n_obs_mean          = mean(n_obs, na.rm = TRUE),
  n_obs_sum           = sum(n_obs, na.rm = TRUE)), by = .(dataset, year, climate_zone)]

### Global summary (across all climate zones)
global_stats_GSWO <- gswo_dt[, .(
  median_water_median = median(median_water, na.rm = TRUE),
  median_water_mean   = mean(median_water, na.rm = TRUE),
  median_water_sum    = sum(median_water, na.rm = TRUE),
  n_obs_median        = median(n_obs, na.rm = TRUE),
  n_obs_mean          = mean(n_obs, na.rm = TRUE),
  n_obs_sum           = sum(n_obs, na.rm = TRUE)), by = .(dataset, year)][, climate_zone := "Global"]

# combine
summary_GSWO <- rbindlist(list(zone_stats_GSWO, global_stats_GSWO), use.names = TRUE)

#########
## zonal summary GLAD 
#########

zone_stats_GLAD <- glad_dt[, .(
  median_water_median = median(median_water, na.rm = TRUE),
  median_water_mean   = mean(median_water, na.rm = TRUE),
  median_water_sum    = sum(median_water, na.rm = TRUE),
  n_obs_median        = median(n_obs, na.rm = TRUE),
  n_obs_mean          = mean(n_obs, na.rm = TRUE),
  n_obs_sum           = sum(n_obs, na.rm = TRUE)), by = .(dataset, year, climate_zone)]

### Global summary (across all climate zones)
global_stats_GLAD <- glad_dt[, .(
  median_water_median = median(median_water, na.rm = TRUE),
  median_water_mean   = mean(median_water, na.rm = TRUE),
  median_water_sum    = sum(median_water, na.rm = TRUE),
  n_obs_median        = median(n_obs, na.rm = TRUE),
  n_obs_mean          = mean(n_obs, na.rm = TRUE),
  n_obs_sum           = sum(n_obs, na.rm = TRUE)), by = .(dataset, year)][, climate_zone := "Global"]

# combine
summary_GLAD <- rbindlist(list(zone_stats_GLAD, global_stats_GLAD), use.names = TRUE)

#########
## combine GLAD and GSWO and recode climate zones
#########

data<-rbind(summary_GLAD, summary_GSWO)

###### re-code climate zones
data[, climate_zone := as.character(climate_zone)]

data[climate_zone == 1, climate_zone := "Tropical"]
data[climate_zone == 2, climate_zone := "Dry"]
data[climate_zone == 3, climate_zone := "Temperate"]
data[climate_zone == 4, climate_zone := "Continental"]
data[climate_zone == 5, climate_zone := "Polar"]
data[climate_zone == 6, climate_zone := NA]

#### get area in km2
data[, total_lake_area := median_water_sum / 1e6]
data[, median_lake_area := median_water_median / 1e6]
data[, mean_lake_area := median_water_mean / 1e6]


#==========================
# =====  stats for statements
#==========================
dat_gswo <- data[data$dataset == "GSWO" & data$climate_zone == "Global", ]
dat_glad <- data[data$dataset == "GLAD" & data$climate_zone == "Global", ]

###   Total, mean, and median global lake area derived from the GSWO and GLAD
###   surface water products increased over the 23-year record 
summary(lm(median_lake_area~year, data=data[dataset=='GSWO' & climate_zone=='Global',]))$coefficients["year", "Pr(>|t|)"]
summary(lm(mean_lake_area~year, data=data[dataset=='GSWO' & climate_zone=='Global',]))$coefficients["year", "Pr(>|t|)"]
summary(lm(total_lake_area~year, data=data[dataset=='GSWO' & climate_zone=='Global',]))$coefficients["year", "Pr(>|t|)"]
summary(lm(median_lake_area~year, data=data[dataset=='GLAD' & climate_zone=='Global',]))$coefficients["year", "Pr(>|t|)"]
summary(lm(mean_lake_area~year, data=data[dataset=='GLAD' & climate_zone=='Global',]))$coefficients["year", "Pr(>|t|)"]
summary(lm(total_lake_area~year, data=data[dataset=='GLAD' & climate_zone=='Global',]))$coefficients["year", "Pr(>|t|)"]

#####   the total, mean, and median lake area were strongly correlated with observation frequency

cor.test(dat_gswo$total_lake_area, dat_gswo$n_obs_sum, method = "pearson")
cor.test(dat_glad$total_lake_area, dat_glad$n_obs_sum, method = "pearson")


cor.test(dat_gswo$mean_lake_area, dat_gswo$n_obs_mean, method = "pearson")
cor.test(dat_glad$mean_lake_area, dat_glad$n_obs_mean, method = "pearson")

cor.test(dat_gswo$median_lake_area, dat_gswo$n_obs_median, method = "pearson")
cor.test(dat_glad$median_lake_area, dat_glad$n_obs_median, method = "pearson")

#### on average, the global total number of observations increased by X% 
#### while the mean and median observation density increased by x and  y %

data %>% filter(climate_zone=='Global')%>%  group_by(dataset) %>%summarise(
                  first_year = min(year),
                  last_year  = max(year),
                  first_n_obs_sum   = n_obs_sum[year == first_year],
                  last_n_obs_sum    = n_obs_sum[year == last_year],
                  first_n_obs_mean  = n_obs_mean[year == first_year],
                  last_n_obs_mean   = n_obs_mean[year == last_year],
                  first_n_obs_median= n_obs_median[year == first_year],
                  last_n_obs_median = n_obs_median[year == last_year]) %>%
          mutate( total_change    = (last_n_obs_sum - first_n_obs_sum) / first_n_obs_sum * 100,
                  mean_change   = (last_n_obs_mean - first_n_obs_mean) / first_n_obs_mean * 100,
                  median_change = (last_n_obs_median - first_n_obs_median) / first_n_obs_median * 100) %>%
          select (total_change, mean_change, median_change,dataset)




###  average total, mean and median lake area was X, y, and z% 
###  higher post-2013 relative to the pre-2013 average

dat_gswo$l8<-ifelse(dat_gswo$year<2013, 'pre', 'post')
dat_glad$l8<-ifelse(dat_glad$year<2013, 'pre', 'post')
dat_gswo %>%group_by(l8) %>%
  summarise(avg_median_water_median = mean(median_water_median),
    avg_median_water_mean = mean(median_water_mean),
    avg_median_water_sum = mean(median_water_sum),.groups = 'drop') %>%
  summarise(median_increase = (avg_median_water_median[l8 == "post"] - avg_median_water_median[l8 == "pre"]) / avg_median_water_median[l8 == "pre"] * 100,
    mean_increase = (avg_median_water_mean[l8 == "post"] - avg_median_water_mean[l8 == "pre"]) / avg_median_water_mean[l8 == "pre"] * 100,
    sum_increase = (avg_median_water_sum[l8 == "post"] - avg_median_water_sum[l8 == "pre"]) / avg_median_water_sum[l8 == "pre"] * 100)

dat_glad %>%group_by(l8) %>%
  summarise(avg_median_water_median = mean(median_water_median),
            avg_median_water_mean = mean(median_water_mean),
            avg_median_water_sum = mean(median_water_sum),.groups = 'drop') %>%
  summarise(median_increase = (avg_median_water_median[l8 == "post"] - avg_median_water_median[l8 == "pre"]) / avg_median_water_median[l8 == "pre"] * 100,
            mean_increase = (avg_median_water_mean[l8 == "post"] - avg_median_water_mean[l8 == "pre"]) / avg_median_water_mean[l8 == "pre"] * 100,
            sum_increase = (avg_median_water_sum[l8 == "post"] - avg_median_water_sum[l8 == "pre"]) / avg_median_water_sum[l8 == "pre"] * 100)

#==========================
# ===== quick count of the number of lakes  
#==========================
gswo_dt[, uniqueN(lake_id)]
glad_dt[, uniqueN(lake_id)]
