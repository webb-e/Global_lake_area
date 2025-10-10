library(arrow)
library(data.table)
library(tidyverse)
#==========================
# ===== read in files
#==========================
# folder with parquet files
breakpoint_folder <- ".../breakpoint_dataset"

# list all parquet files
parquet_files <- list.files(breakpoint_folder, pattern = "\\.parquet$", full.names = TRUE)

# read and combine all parquet files
breakpoints_dt <- rbindlist(lapply(parquet_files, read_parquet), use.names = TRUE, fill = TRUE)
setDT(breakpoints_dt)

## climate zone data
climatedata<-fread('.../lake_climate_match.csv')
## get lake_id in the right format
climatedata$lake_id<-as.character(round(climatedata$lake_id))

#==========================
# ===== merge and clean data
#==========================

## merge climate and breakpoint data
setkey(climatedata, lake_id)
setkey(breakpoints_dt, lake_id)

data <- breakpoints_dt[climatedata, nomatch = 0L, allow.cartesian = TRUE]

###### re-code climate zones
data[, climate_zone := as.character(climate_zone)]

data[climate_zone == 1, climate_zone := "Tropical"]
data[climate_zone == 2, climate_zone := "Dry"]
data[climate_zone == 3, climate_zone := "Temperate"]
data[climate_zone == 4, climate_zone := "Continental"]
data[climate_zone == 5, climate_zone := "Polar"]
data[climate_zone == 6, climate_zone := NA]


#==========================
# ===== some statistics - how many breakpoints in 2012?
#==========================
## breakpoints by dataset for 2012
breaks_2012 <- data[break_year == 2012, .(n_unique_lakes = uniqueN(lake_id)), by = dataset]

## total lakes by dataset
uniquelakes_bydataset <- data[, .(n_unique_lakes = uniqueN(lake_id)), by = dataset]

## merge and calculate percentage
setkey(breaks_2012, dataset)
setkey(uniquelakes_bydataset, dataset)

lakesbydataset <- breaks_2012[uniquelakes_bydataset, nomatch = 0L]
lakesbydataset[, percent_2012 := 100 * n_unique_lakes / i.n_unique_lakes]

#==========================
# ===== percentage by climate zone
#==========================

## breakpoints by climate zone/dataset/year
breakpoint_bydataset_andzone <- data[, .(n_breakpoints = uniqueN(lake_id)), 
                                     by = .(climate_zone, dataset, break_year)]

## lakes by dataset/climate zone
uniquelakes_bydataset_andzone <- data[, .(n_total = uniqueN(lake_id)), 
                                      by = .(dataset, climate_zone)]

## merge summaries
setkey(breakpoint_bydataset_andzone, dataset, climate_zone)
setkey(uniquelakes_bydataset_andzone, dataset, climate_zone)

summary_dt <- breakpoint_bydataset_andzone[uniquelakes_bydataset_andzone]
# Total per climate_zone and dataset (denominator for percentage)
summary_dt[, percent := 100 * n_breakpoints / n_total]
summary_dt[, year := break_year + 1]
summary_dt$highlight_2013 <- ifelse(summary_dt$year == 2013, "2013", "Other")
## add one year (so 2012 -> 2013, since the interpretation of the breakpoint analysis is that
## 2012 = last year before breakpoint and I want to say 2013 = breakpoint)

plot_data <- summary_dt[!is.na(climate_zone) & !is.na(dataset)]
plot_data$climate_zone <- factor(
  plot_data$climate_zone,
  levels = c("Polar", "Continental", "Temperate", "Tropical", "Dry"))

plot_data$dataset<-factor(plot_data$dataset, levels=c("GSWO", "GLAD"))

#==========================
# ===== Plots
#==========================
plot_highlight<-ggplot(plot_data, aes(x = year, y = percent, fill=dataset,  alpha = highlight_2013)) +
                    geom_col() +
                    scale_fill_manual(values = c("GLAD" = "#2a5674", "GSWO" = "#68abb8")) +
                    facet_grid(rows = vars(climate_zone), cols = vars(dataset), scales='free_y') +
                    scale_alpha_manual(values = c("2013" = 1, "Other" = 0.7),name = NULL) +
                    scale_y_continuous(name = "Lakes with a breakpoint (%)") +
                    scale_x_continuous(name = "Year", breaks = pretty(summary_dt$break_year)) +
                    theme_bw(base_size=35) +
                    theme(legend.position = "none",  axis.text.x = element_text(angle = 45, hjust = 1),
                      panel.grid.major = element_blank(), 
                      panel.grid.minor = element_blank())



#### SAVE!
ggsave("Fig3_breakpointV2.jpg", plot_highlight,  width=400, height=400,units="mm", scale=1, dpi=500,
       path='.../figures')

