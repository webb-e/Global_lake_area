library(arrow)
library(tidyverse)
library(data.table)
library(scales)
library(patchwork)
library(glue)
#==========================
# ===== read in files
#==========================
# Set the file path
#parquet_path <- "/Users/elizabethwebb/Library/CloudStorage/GoogleDrive-webb.elizabeth.e@gmail.com/My Drive/PostDoc/Landsat8/annual_lake_medians_dataset/"  
parquet_path <- '/Users/elizabethwebb/Library/CloudStorage/Box-Box/Landsat8/annual_lake_medians_dataset'

parquet_files <- list.files(parquet_path, pattern = "\\.parquet$", full.names = TRUE, recursive = TRUE)

#########
## Read in GSWO data
#########
read_filtered <- function(path) {
  ds <- open_dataset(path)
  filtered <- ds %>%
    filter(dataset == "GSWO") %>%
    collect()
  as.data.table(filtered)
}

gswo_dt <- rbindlist(lapply(parquet_files, read_filtered), use.names = TRUE, fill = TRUE)

#########
## Read in GLAD data
#########
read_filtered_GLAD <- function(path) {
  ds <- open_dataset(path)
  filtered <- ds %>%
    filter(dataset == "GLAD") %>%
    collect()
  as.data.table(filtered)
}

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
  n_obs_sum           = sum(n_obs, na.rm = TRUE)
), by = .(dataset, year, climate_zone)]

### Global summary (across all climate zones)
global_stats_GSWO <- gswo_dt[, .(
  median_water_median = median(median_water, na.rm = TRUE),
  median_water_mean   = mean(median_water, na.rm = TRUE),
  median_water_sum    = sum(median_water, na.rm = TRUE),
  n_obs_median        = median(n_obs, na.rm = TRUE),
  n_obs_mean          = mean(n_obs, na.rm = TRUE),
  n_obs_sum           = sum(n_obs, na.rm = TRUE)
), by = .(dataset, year)][, climate_zone := "Global"]

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
  n_obs_sum           = sum(n_obs, na.rm = TRUE)
), by = .(dataset, year, climate_zone)]

### Global summary (across all climate zones)
global_stats_GLAD <- glad_dt[, .(
  median_water_median = median(median_water, na.rm = TRUE),
  median_water_mean   = mean(median_water, na.rm = TRUE),
  median_water_sum    = sum(median_water, na.rm = TRUE),
  n_obs_median        = median(n_obs, na.rm = TRUE),
  n_obs_mean          = mean(n_obs, na.rm = TRUE),
  n_obs_sum           = sum(n_obs, na.rm = TRUE)
), by = .(dataset, year)][, climate_zone := "Global"]

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
plot_dt <- melt(df,id.vars = c("dataset", "year", "climate_zone"), 
                measure.vars = vars_to_melt, variable.name = "metric", value.name = "value")

#==========================
# ===== % increase over time from the slope of the line (total / mean / median)
#==========================
slope_pct <- df[
  order(year),
  .(
    total_pct  = { f <- lm(total_lake_area  ~ year); coef(f)[2] * (year[.N] - year[1]) /
      (coef(f)[1] + coef(f)[2] * year[1]) * 100 },
    mean_pct   = { f <- lm(mean_lake_area   ~ year); coef(f)[2] * (year[.N] - year[1]) /
      (coef(f)[1] + coef(f)[2] * year[1]) * 100 },
    median_pct = { f <- lm(median_lake_area ~ year); coef(f)[2] * (year[.N] - year[1]) /
      (coef(f)[1] + coef(f)[2] * year[1]) * 100 }
  ),
  by = .(dataset, climate_zone)
][order(dataset, climate_zone)]

print(slope_pct)

#==========================
# ===== Plots!
#=========================
basesize = 25
pointsize = 6
### function to plot once for each climate zone
plot_climate_zone <- function(plot_dt, df, climate_zone_name, basesize = 25, pointsize = 6) {
  
  # Filter for climate zone
  cz_plot_dt <- plot_dt %>% filter(climate_zone == climate_zone_name)
  cz_df <- df %>% filter(climate_zone == climate_zone_name)
  
  # Helper to create base plots with optional facet labels
  base_plot <- function(metric_name, ylab_text = "", facet_label = NULL, show_strip = TRUE) {
    ggplot(cz_plot_dt[metric == metric_name],
           aes(x = year, y = value, color = dataset)) +
      geom_rect(inherit.aes = FALSE, aes(xmin = -Inf, xmax = 2012.5, ymin = -Inf, ymax = Inf),
                fill = "grey95", alpha = 0.3) +
      geom_point(size = pointsize) +
      scale_color_manual(values = c("GLAD" = "#2a5674", "GSWO" = "#68abb8")) +
      scale_y_continuous(labels = label_number(scale_cut = cut_short_scale())) +
      facet_wrap(~metric,
                 labeller = if (!is.null(facet_label)) as_labeller(facet_label) else label_value) +
      theme_bw(basesize) +
      theme(
        legend.position = "bottom",
        legend.justification = "center",
        legend.title = element_blank(),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        strip.text = if (show_strip) element_text(size = 30) else element_blank()
      ) +
      ylab(ylab_text) +
      xlab("Year")
  }
  
  # Time series plots (keep facet labels)
  LT <- base_plot("total_lake_area", "Lake area (km²)", c(total_lake_area = "Total"), show_strip = TRUE)
  MT <- base_plot("mean_lake_area", "", c(mean_lake_area = "Mean"), show_strip = TRUE)
  RT <- base_plot("median_lake_area", "", c(median_lake_area = "Median"), show_strip = TRUE)
  
  # Observation time series (suppress facet labels)
  LM <- base_plot("n_obs_sum", "Number of observations", show_strip = FALSE)
  MM <- base_plot("n_obs_mean", "", show_strip = FALSE)
  RM <- base_plot("n_obs_median", "", show_strip = FALSE)
  
  # Obs vs Area scatterplots (also no facet labels)
  common_scatter_theme <- theme_bw(basesize) +
    theme(
      legend.position = "bottom",
      legend.justification = "center",
      legend.title = element_blank(),
      panel.grid.major = element_blank(),
      panel.grid.minor = element_blank(),
      strip.text = element_blank()
    )
  
  LB <- ggplot(cz_df, aes(x = n_obs_sum, y = total_lake_area, color = dataset)) +
    geom_point(size = pointsize) +
    scale_color_manual(values = c("GLAD" = "#2a5674", "GSWO" = "#68abb8")) +
    scale_y_continuous(labels = label_number(scale_cut = cut_short_scale())) +
    scale_x_continuous(labels = label_number(scale_cut = cut_short_scale())) +
    common_scatter_theme +
    ylab("Lake area (km²)") + xlab("Number of observations")
  
  MB <- ggplot(cz_df, aes(x = n_obs_mean, y = mean_lake_area, color = dataset)) +
    geom_point(size = pointsize) +
    scale_color_manual(values = c("GLAD" = "#2a5674", "GSWO" = "#68abb8")) +
    scale_y_continuous(labels = label_number(scale_cut = cut_short_scale())) +
    scale_x_continuous(labels = label_number(scale_cut = cut_short_scale())) +
    common_scatter_theme +
    ylab("") + xlab("Number of observations")
  
  RB <- ggplot(cz_df, aes(x = n_obs_median, y = median_lake_area, color = dataset)) +
    geom_point(size = pointsize) +
    scale_color_manual(values = c("GLAD" = "#2a5674", "GSWO" = "#68abb8")) +
    scale_y_continuous(labels = label_number(scale_cut = cut_short_scale())) +
    scale_x_continuous(labels = label_number(scale_cut = cut_short_scale())) +
    common_scatter_theme +
    ylab("") + xlab("Number of observations")
  
  # Combine
  final_plot <- (LT + MT + RT) / (LM + MM + RM) / (LB + MB + RB) +
    plot_layout(guides = "collect") +
    plot_annotation(
      title = climate_zone_name,
      theme = theme(
        plot.title = element_text(
          size = 30,
          face = "bold",
          hjust = 0.5
        ),
        legend.position = "bottom",
        legend.justification = "center"
      )
    )
  
  return(final_plot)
}

Tropical<-plot_climate_zone(plot_dt, df, "Tropical")
Dry<-plot_climate_zone(plot_dt, df, "Dry")
Polar<-plot_climate_zone(plot_dt, df, "Polar")
Continental<-plot_climate_zone(plot_dt, df, "Continental")
Temperate<-plot_climate_zone(plot_dt, df, "Temperate")

ggsave("S1_Temperate.jpg", Temperate,  width=450, height=400,units="mm", scale=1, dpi=500,
       path="Google Drive/My Drive/PostDoc/Landsat8/figures/")
ggsave("S1_Tropical.jpg", Tropical,  width=450, height=400,units="mm", scale=1, dpi=500,
       path="Google Drive/My Drive/PostDoc/Landsat8/figures/")
ggsave("S1_Dry.jpg", Dry,  width=450, height=400,units="mm", scale=1, dpi=500,
       path="Google Drive/My Drive/PostDoc/Landsat8/figures/")
ggsave("S1_Polar.jpg", Polar,  width=450, height=400,units="mm", scale=1, dpi=500,
       path="Google Drive/My Drive/PostDoc/Landsat8/figures/")
ggsave("S1_Continental.jpg", Continental,  width=450, height=400,units="mm", scale=1, dpi=500,
       path="Google Drive/My Drive/PostDoc/Landsat8/figures/")

unique(plot_dt$climate_zone)

#==========================
# ===== quick count of the number of lakes  
#==========================
gswo_dt[, climate_zone := as.character(climate_zone)]
gswo_dt[climate_zone == 1, climate_zone := "Tropical"]
gswo_dt[climate_zone == 2, climate_zone := "Dry"]
gswo_dt[climate_zone == 3, climate_zone := "Temperate"]
gswo_dt[climate_zone == 4, climate_zone := "Continental"]
gswo_dt[climate_zone == 5, climate_zone := "Polar"]
gswo_dt[climate_zone == 6, climate_zone := NA]

glad_dt[, climate_zone := as.character(climate_zone)]
glad_dt[climate_zone == 1, climate_zone := "Tropical"]
glad_dt[climate_zone == 2, climate_zone := "Dry"]
glad_dt[climate_zone == 3, climate_zone := "Temperate"]
glad_dt[climate_zone == 4, climate_zone := "Continental"]
glad_dt[climate_zone == 5, climate_zone := "Polar"]
glad_dt[climate_zone == 6, climate_zone := NA]

# Count unique lakes by climate zone
gswo_dt[, .(n_unique_lakes = uniqueN(lake_id)), by = climate_zone]
glad_dt[, .(n_unique_lakes = uniqueN(lake_id)), by = climate_zone]

#==========================
# ===== test to see contributions to increasing lake area
#==========================
########
## TOTAL LAKE AREA
#######
## GSWO
gswo<- data %>% filter(dataset=='GSWO') %>% mutate(l8 = ifelse(year <2013, 0, 1))

model1_GSWO<- lm(total_lake_area ~ n_obs_sum + year + l8, data=gswo)
model2_GSWO<- lm(total_lake_area ~ n_obs_sum + l8, data=gswo)
model3_GSWO<- lm(total_lake_area ~ n_obs_sum + year, data=gswo)
model4_GSWO<- lm(total_lake_area ~ l8 + year, data=gswo)

AIC(model1_GSWO, model2_GSWO, model3_GSWO, model4_GSWO)
## model 2 has lowest AIC 
anova(model1_GSWO, model2_GSWO) # p =0.7005
anova(model1_GSWO, model3_GSWO) # p = 0.016
## model 2 is not statistically significantly better than model 1; either model 1 or model 2 works,
## but using the l8 step change (model 2) is better using  year (model 3).

## GLAD
glad<- data %>% filter(dataset=='GLAD') %>% mutate(l8 = ifelse(year <2013, 0, 1))

model1_GLAD<- lm(total_lake_area ~ n_obs_sum + year + l8, data=glad)
model2_GLAD<- lm(total_lake_area ~ n_obs_sum + l8, data=glad)
model3_GLAD<- lm(total_lake_area ~ n_obs_sum + year, data=glad)
model4_GLAD<- lm(total_lake_area ~ l8 + year, data=glad)

AIC(model1_GLAD, model2_GLAD, model3_GLAD, model4_GLAD)
## model 2 has lowest AIC 
anova(model1_GLAD, model2_GLAD) # p =0.4837
anova(model1_GLAD, model3_GLAD) # p = 0.02693
## model 2 is not statistically significantly better than model 1; either model 1 or model 2 works,
## but using the l8 step change (model 2) is better using  year (model 3).

########
## MEAN LAKE AREA
#######
mean1_GSWO<- lm(mean_lake_area ~ n_obs_mean + year + l8, data=gswo)
mean2_GSWO<- lm(mean_lake_area ~ n_obs_mean + l8, data=gswo)
mean3_GSWO<- lm(mean_lake_area ~ n_obs_mean + year, data=gswo)
mean4_GSWO<- lm(mean_lake_area ~ l8 + year, data=gswo)
mean5_GSWO<- lm(mean_lake_area ~ l8, data=gswo)
mean6_GSWO<- lm(mean_lake_area ~ n_obs_sum + year + l8, data=gswo)
mean7_GSWO<- lm(mean_lake_area ~ n_obs_sum + l8, data=gswo)
mean8_GSWO<- lm(mean_lake_area ~ n_obs_sum + year, data=gswo)
mean9_GSWO<- lm(mean_lake_area ~ l8 + year, data=gswo)
mean10_GSWO<- lm(mean_lake_area ~ l8, data=gswo)

GSWO_mean_table <- AIC(mean1_GSWO, mean2_GSWO, mean3_GSWO, mean4_GSWO,
                 mean5_GSWO, mean6_GSWO, mean7_GSWO, mean8_GSWO,
                 mean9_GSWO, mean10_GSWO)

GSWO_mean_table[order(GSWO_mean_table$AIC), ]

## mean2_GSWO has best AIC, mean1_GSWO is nearly the same
summary(mean1_GSWO)
anova(mean5_GSWO, mean4_GSWO) # 0.5089

summary(mean5_GSWO)
cor(gswo$n_obs_sum, gswo$year)
cor(gswo$n_obs_sum, gswo$mean_lake_area)


anova(mean1_GSWO, mean3_GSWO) # p = 0.016
## mean 2 is not statistically significantly better than mean 1; either mean 1 or mean 2 works,
## but using the l8 step change (mean 2) is better using  year (mean 3).


mean1_GLAD<- lm(mean_lake_area ~ n_obs_sum + year + l8, data=glad)
mean2_GLAD<- lm(meam_lake_area ~ n_obs_sum + l8, data=glad)
mean3_GLAD<- lm(mean_lake_area ~ n_obs_sum + year, data=glad)
mean4_GLAD<- lm(meam_lake_area ~ l8 + year, data=glad)

AIC(mean1_GLAD, mean2_GLAD, mean3_GLAD, mean4_GLAD)
## mean 2 has lowest AIC 
anova(mean1_GLAD, mean2_GLAD) # p =0.4837
anova(mean1_GLAD, mean3_GLAD) # p = 0.02693
## mean 2 is not statistically significantly better than mean 1; either mean 1 or mean 2 works,
## but using the l8 step change (mean 2) is better using  year (mean 3).
