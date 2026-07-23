library(arrow)
library(purrr)
library(tidyverse)
library(scales)

# ---- Paths ----
parquet_path <- 'seasonal_dataset'
parquet_files <- list.files(parquet_path, pattern = "\\.parquet$", full.names = TRUE, recursive = TRUE)

# ---- Plot styling params ----
pointsize <- 3
basesize  <- 18

# ---- Per (zone, dataset) processing ----

process_climate_zone_dataset <- function(zone, ds) {
  # Read matching rows once; arrow pushes the filter down across all files
  zone_data <- open_dataset(parquet_files) %>%
    dplyr::select(lake_id, dataset, year, n_total, prop_wet, climate_zone) %>%
    dplyr::filter(climate_zone == zone, dataset == ds) %>%
    collect()

  if (nrow(zone_data) == 0) return(NULL)  # skip empty

  L <- dplyr::n_distinct(zone_data$lake_id)  # total lakes in this zone+dataset

  zone_data %>%
    group_by(year) %>%
    summarise(
      mean_wet           = mean(prop_wet, na.rm = TRUE),
      median_wet         = median(prop_wet, na.rm = TRUE),
      # pad each year's observed counts with zeros for lakes not seen that year
      median_obs         = median(c(rep(0, L - dplyr::n_distinct(lake_id)), n_total)),
      mean_obs           = sum(n_total) / L,
      total_observations = sum(n_total),
      .groups = "drop"
    ) %>%
    # ensure every year 1999-2021 is present; fully-empty years are all zeros
    tidyr::complete(
      year = 1999:2021,
      fill = list(median_obs = 0, mean_obs = 0, total_observations = 0)
    ) %>%
    mutate(dataset = ds, climate_zone = zone)
}

# ---- Unique (climate_zone, dataset) pairs ----
pairs <- open_dataset(parquet_files) %>%
  dplyr::select(climate_zone, dataset) %>%
  dplyr::distinct() %>%
  collect()

# ---- Build df across all pairs ----
df <- map2_dfr(pairs$climate_zone, pairs$dataset, process_climate_zone_dataset)

# ---- Label climate zones (code 6 dropped) ----
zone_levels <- c("Polar", "Continental", "Temperate", "Tropical", "Dry")
df <- df %>%
  mutate(climate_zone = case_when(
    climate_zone == 1 ~ "Tropical",
    climate_zone == 2 ~ "Dry",
    climate_zone == 3 ~ "Temperate",
    climate_zone == 4 ~ "Continental",
    climate_zone == 5 ~ "Polar",
    climate_zone == 6 ~ NA_character_,
    TRUE ~ as.character(climate_zone)
  )) %>%
  filter(!is.na(climate_zone)) %>%
  mutate(climate_zone = factor(climate_zone, levels = zone_levels))

# ---- Plot ----
plot <- ggplot(df, aes(x = mean_obs, y = mean_wet, color = dataset)) +
  geom_point(size = pointsize) +
  scale_color_manual(values = c("GLAD" = "#2a5674", "GSWO" = "#68abb8")) +
  scale_x_continuous(
    breaks = scales::breaks_width(1),
    labels = scales::label_number(accuracy = 1)) +
  facet_grid(~ climate_zone, scales = "free") +
  theme_bw(base_size = basesize) +
  theme(axis.text.x       = element_text(angle = 45, hjust = 1),
    legend.position   = "bottom",
    legend.justification = "center",
    legend.text = element_text(size = basesize),
    legend.title      = element_blank(),
    panel.grid.major  = element_blank(),
    panel.grid.minor  = element_blank(),
    strip.background  = element_rect(fill = "grey90"),
    strip.text        = element_text(face = "bold")) +
  guides(color = guide_legend(override.aes = list(size = pointsize)))+ 
  ylab('Proportion of observations\nfrom wet months') +
  xlab('Number of observations')

print(plot)

ggsave(plot, filename='Figure5_2026.jpg',
       width=550, height=300,units="mm", scale=0.5, dpi=500)
