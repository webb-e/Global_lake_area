########
### This code takes the annual median lake areas, generates annual total, mean, and median annual lake areas, 
### and produces a global figure of the trends of these variables over time as well as
### how they relate to observation frequency.
###
### Last updated Sept 18, 2025 by E. Webb
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
parquet_path <- "/Users/elizabethwebb/Library/CloudStorage/GoogleDrive-webb.elizabeth.e@gmail.com/My Drive/PostDoc/Landsat8/annual_lake_medians_dataset/"  
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

## remove lakes w/o climate zone
df <- data[!is.na(climate_zone)]
df$climate_zone <- factor(df$climate_zone,levels = c("Global", "Polar", "Continental", "Temperate", "Tropical", "Dry"))

### create df for plotting
vars_to_melt <- c("total_lake_area", "n_obs_sum", "median_lake_area", "n_obs_median", "mean_lake_area", "n_obs_mean")
plot_dt <- melt(df[climate_zone == "Global"],id.vars = c("dataset", "year"), 
                measure.vars = vars_to_melt, variable.name = "metric", value.name = "value")
#==========================
# ===== Plots!
#=========================
basesize = 30
pointsize = 7
textsize = 30
### total lake area
LT<-ggplot(plot_dt[metric == 'total_lake_area'],
           aes(x=year, y=value, color=dataset)) + 
  geom_rect( inherit.aes = FALSE,aes(xmin = -Inf, xmax = 2012.5, ymin = -Inf, ymax = Inf), 
             fill = "grey95", alpha = 0.3) +
  facet_wrap(~metric, labeller = labeller(metric = c(total_lake_area  = "Total")))+
  geom_point(size=pointsize) + 
  scale_color_manual(values = c("GLAD" = "#2a5674", "GSWO" = "#68abb8")) +
  scale_y_continuous(labels = label_number(scale_cut = cut_short_scale()))+ 
  theme_bw(basesize) +
  theme(legend.position = "bottom",
        legend.justification = "center",
        legend.title = element_blank(),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        strip.text = element_text(size = textsize, face = "bold"))+
  ylab('Lake area (km²)') + xlab('Year')
### mean lake area
MT<-ggplot(plot_dt[metric == 'mean_lake_area'],
           aes(x=year, y=value, color=dataset)) + 
  geom_rect( inherit.aes = FALSE,aes(xmin = -Inf, xmax = 2012.5, ymin = -Inf, ymax = Inf), 
             fill = "grey95", alpha = 0.3) +
  facet_wrap(~metric, labeller = labeller(metric = c(mean_lake_area  = "Mean")))+
  geom_point(size=pointsize) + 
  scale_color_manual(values = c("GLAD" = "#2a5674", "GSWO" = "#68abb8")) +
  scale_y_continuous(labels = label_number(scale_cut = cut_short_scale()))+ 
  theme_bw(basesize) +
  theme(  legend.position = "bottom",
          legend.justification = "center",
          legend.title = element_blank(),
          strip.text = element_text(size = textsize, face = "bold"),
          panel.grid.major = element_blank(),panel.grid.minor = element_blank()) +
  ylab('')+ xlab('Year')

### median lake area
RT<-ggplot(plot_dt[metric == 'median_lake_area'],
           aes(x=year, y=value, color=dataset)) + 
  geom_rect( inherit.aes = FALSE,aes(xmin = -Inf, xmax = 2012.5, ymin = -Inf, ymax = Inf), 
             fill = "grey95", alpha = 0.3) +
  facet_wrap(~metric, labeller = labeller(metric = c(median_lake_area  = "Median")))+
  geom_point(size=pointsize) + 
  scale_color_manual(values = c("GLAD" = "#2a5674", "GSWO" = "#68abb8")) +
  scale_y_continuous(labels = label_number(scale_cut = cut_short_scale()))+ 
  theme_bw(basesize) +
  theme(  legend.position = "bottom",
          legend.justification = "center",
          legend.title = element_blank(),
          strip.text = element_text(size = textsize, face = "bold"),
          panel.grid.major = element_blank(),panel.grid.minor = element_blank()) +
  ylab('')+ xlab('Year')

### total number of observations 
LM<-ggplot(plot_dt[metric == 'n_obs_sum'],
           aes(x=year, y=value, color=dataset)) + 
  geom_rect( inherit.aes = FALSE,aes(xmin = -Inf, xmax = 2012.5, ymin = -Inf, ymax = Inf), 
             fill = "grey95", alpha = 0.3) +
  geom_point(size=pointsize) + 
  scale_color_manual(values = c("GLAD" = "#2a5674", "GSWO" = "#68abb8")) +
  scale_y_continuous(labels = label_number(scale_cut = cut_short_scale()))+ 
  theme_bw(basesize) +
  theme(  legend.position = "bottom",
          legend.justification = "center",
          legend.title = element_blank(),
          panel.grid.major = element_blank(),panel.grid.minor = element_blank())+
  ylab('Number of observations')+ xlab('Year')

### median number of observations
MM<-ggplot(plot_dt[metric == 'n_obs_mean'],
           aes(x=year, y=value, color=dataset)) + 
  geom_rect( inherit.aes = FALSE,aes(xmin = -Inf, xmax = 2012.5, ymin = -Inf, ymax = Inf), 
             fill = "grey95", alpha = 0.3) +
  geom_point(size=pointsize) + 
  scale_color_manual(values = c("GLAD" = "#2a5674", "GSWO" = "#68abb8")) +
  scale_y_continuous(labels = label_number(scale_cut = cut_short_scale()))+ 
  theme_bw(basesize) +
  theme(  legend.position = "bottom",
          legend.justification = "center",
          legend.title = element_blank(),
          panel.grid.major = element_blank(),panel.grid.minor = element_blank()) +
  ylab('')+ xlab('Year')

### median number of observations
RM<-ggplot(plot_dt[metric == 'n_obs_median'],
           aes(x=year, y=value, color=dataset)) + 
  geom_rect( inherit.aes = FALSE,aes(xmin = -Inf, xmax = 2012.5, ymin = -Inf, ymax = Inf), 
             fill = "grey95", alpha = 0.3) +
  geom_point(size=pointsize) + 
  scale_color_manual(values = c("GLAD" = "#2a5674", "GSWO" = "#68abb8")) +
  scale_y_continuous(labels = label_number(scale_cut = cut_short_scale()))+ 
  theme_bw(basesize) +
  theme(  legend.position = "bottom",
          legend.justification = "center",
          legend.title = element_blank(),
          panel.grid.major = element_blank(),panel.grid.minor = element_blank()) +
  ylab('')+ xlab('Year')



### median number of observations
RM<-ggplot(plot_dt[metric == 'n_obs_median'],
           aes(x=year, y=value, color=dataset)) + 
  geom_rect( inherit.aes = FALSE,aes(xmin = -Inf, xmax = 2012.5, ymin = -Inf, ymax = Inf), 
             fill = "grey95", alpha = 0.3) +
  geom_point(size=pointsize) + 
  scale_color_manual(values = c("GLAD" = "#2a5674", "GSWO" = "#68abb8")) +
  scale_y_continuous(labels = label_number(scale_cut = cut_short_scale()))+ 
  theme_bw(basesize) +
  theme(  legend.position = "bottom",
          legend.justification = "center",
          legend.title = element_blank(),
          panel.grid.major = element_blank(),panel.grid.minor = element_blank()) +
  ylab('')+ xlab('Year')


### observations vs lake area (total) 
LB<-ggplot(df %>% filter(climate_zone=='Global'), aes(x=n_obs_sum, y=total_lake_area, color=dataset)) + 
  geom_point(size=pointsize) + 
  scale_color_manual(values = c("GLAD" = "#2a5674", "GSWO" = "#68abb8")) +
  scale_y_continuous(labels = label_number(scale_cut = cut_short_scale()))+ 
  scale_x_continuous(labels = label_number(scale_cut = cut_short_scale()))+ 
  theme_bw(basesize) +
  theme(  legend.position = "bottom",
          legend.justification = "center",
          legend.title = element_blank(),
          panel.grid.major = element_blank(),panel.grid.minor = element_blank()) +
  ylab('Lake area (km²)') + xlab ('Number of observations')

### observations vs lake area (mean) 
MB<-ggplot(df %>% filter(climate_zone=='Global'), aes(x=n_obs_mean, y=mean_lake_area, color=dataset)) + 
  geom_point(size=pointsize) + 
  scale_color_manual(values = c("GLAD" = "#2a5674", "GSWO" = "#68abb8")) +
  scale_y_continuous(labels = label_number(scale_cut = cut_short_scale()))+ 
  scale_x_continuous(labels = label_number(scale_cut = cut_short_scale()))+ 
  theme_bw(basesize) +
  theme(  legend.position = "bottom",
          legend.justification = "center",
          legend.title = element_blank(),
          panel.grid.major = element_blank(),panel.grid.minor = element_blank()) +
  xlab ('Number of observations') + ylab (' ')


### observations vs lake area (median) 
RB<-ggplot(df %>% filter(climate_zone=='Global'), aes(x=n_obs_median, y=median_lake_area, color=dataset)) + 
  geom_point(size=pointsize) + 
  scale_color_manual(values = c("GLAD" = "#2a5674", "GSWO" = "#68abb8")) +
  scale_y_continuous(labels = label_number(scale_cut = cut_short_scale()))+ 
  scale_x_continuous(labels = label_number(scale_cut = cut_short_scale()))+ 
  theme_bw(basesize) +
  theme(  legend.position = "bottom",
          legend.justification = "center",
          legend.title = element_blank(),
          panel.grid.major = element_blank(),panel.grid.minor = element_blank()) +
  xlab ('Number of observations') + ylab (' ')

#==========================
# ===== combine into one beautiful plot
#==========================
final_plot <- (LT + MT + RT) / (LM + MM + RM) / (LB + MB + RB) +
  plot_layout(guides = "collect") & 
  theme(legend.position = "bottom",
        legend.justification = "center",
        legend.text = element_text(size = textsize*0.9))

ggsave("Fig1.jpg", final_plot,  width=450, height=400,units="mm", scale=1.2, dpi=500,
       path="Google Drive/My Drive/PostDoc/Landsat8/figures/")

