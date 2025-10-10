########
### This code takes a directory of parquet files with annual information of:
### (1) the per-lake proportion of observations that come from wet years as well and 
### (2) the per-lake number of observations 
### and outputs a figure showing the relationship between observation frequency and
### the proportion of observations that come from wet vs. dry years
### Last updated Sept 6, 2025 by E. Webb
########
library(arrow)
library(purrr)
library(tidyverse)
library(scales)

#==========================
# ===== read in files
#==========================

# Directory containing .parquet files
parquet_path <- '.../seasonal_dataset'
parquet_files <- list.files(parquet_path, pattern = "\\.parquet$", full.names = TRUE, recursive = TRUE)


process_climate_zone_dataset <- function(zone, ds) {
  # Read only needed columns for this zone + dataset
  zone_data <- map_dfr(parquet_files, ~ {
    open_dataset(.x) %>%
      select(lake_id, dataset, year, n_total, prop_wet, climate_zone) %>% 
      filter(climate_zone == zone, dataset == ds) %>%
      collect()})
  
  if (nrow(zone_data) == 0) return(NULL)  # skip empty
  
  # Create full grid of lake_id–year combinations
  full_grid <- zone_data %>%
    distinct(lake_id, dataset, climate_zone) %>%
    crossing(year = 1999:2021)
  
  # Complete the data with missing values
  completed <- full_grid %>%
    left_join(zone_data, by = c("lake_id", "dataset", "climate_zone", "year")) %>%
    mutate( n_total = replace_na(n_total, 0),
      prop_wet = replace_na(prop_wet, NA) )
  
  # Summarise
  completed %>%
    group_by(year, dataset, climate_zone) %>%
    summarise(
      mean_wet = mean(prop_wet, na.rm = TRUE),
      median_wet = median(prop_wet, na.rm = TRUE),
      median_obs = median(n_total),
      mean_obs = mean(n_total),
      total_observations = sum(n_total),
      .groups = "drop")
}

# Get all unique (climate_zone, dataset) pairs without loading everything
pairs <- map_dfr(parquet_files, ~ {
  open_dataset(.x) %>%
    select(climate_zone, dataset) %>%
    distinct() %>%
    collect()}) %>%
  distinct()

# Loop through zone–dataset pairs in chunks
data <- map_dfr(
  1:nrow(pairs),
  ~ process_climate_zone_dataset(pairs$climate_zone[.x], pairs$dataset[.x]))


#==========================
# ===== data wrangling
#=========================

## recode
df <- data %>% mutate(climate_zone = case_when(
  climate_zone == 1 ~ "Tropical",
  climate_zone == 2 ~ "Dry",
  climate_zone == 3 ~ "Temperate",
  climate_zone == 4 ~ "Continental",
  climate_zone == 5 ~ "Polar",
  climate_zone == 6 ~ NA_character_,
  TRUE ~ as.character(climate_zone))) %>% filter(!is.na(climate_zone)) 


df$climate_zone <- factor(df$climate_zone,levels = c("Polar", "Continental", "Temperate", "Tropical", "Dry"))
df$dataset <- factor(df$dataset,levels = c("GSWO", "GLAD"))

#==========================
# ===== Plots!
#=========================
basesize = 40
pointsize = 8
plot<-ggplot(df, aes(x = mean_obs, y = mean_wet, color=dataset)) + 
  geom_point(size = pointsize) + 
  scale_color_manual(values = c("GLAD" = "#2a5674", "GSWO" = "#68abb8")) +
  scale_x_continuous(breaks = scales::breaks_width(1),
                     labels = scales::label_number(accuracy = 1))+
  facet_grid(~climate_zone, scales='free')+
  theme_bw(basesize) +
  theme( axis.text.x = element_text(angle = 45, hjust = 1),
         legend.position = "bottom",
         legend.justification = "center",
         legend.title = element_blank(),
         # panel.spacing = unit(2, "lines"),
         panel.grid.major = element_blank(),
         panel.grid.minor = element_blank(),
         strip.background = element_rect(fill = "grey90"),
         strip.text = element_text(face = "bold")) +
  ylab('Proportion of observations\nfrom wet months') + 
  xlab('Number of observations')


ggsave(plot, filename='....jpg',
       width=550, height=300,units="mm", scale=1, dpi=500)

#==========================
# ===== Test for significance
#=========================


df %>%group_by(dataset, climate_zone) %>% summarise(
  test = list(cor.test(  ~ mean_wet + mean_obs, data = cur_data(),method = "kendall",
                         alternative = "greater"  ) ),.groups = "drop") %>%
  mutate(tau = sapply(test, function(x) x$estimate),  
         p_value = sapply(test, function(x) x$p.value), 
         significant = p_value < 0.05 ) %>%
  select(dataset, climate_zone, tau, p_value, significant)

