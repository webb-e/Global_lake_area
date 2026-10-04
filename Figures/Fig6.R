## code to reproduce figure 6. output from wet_dry.R

### read in data and set settings
df <- fread('csvs/zone_year.csv')

ZONE_LABELS <- c(`1` = "Tropical", `2` = "Dry", `3` = "Temperate", `4` = "Continental", `5` = "Polar")
ZONE_ORDER  <- c("Polar", "Continental", "Temperate", "Tropical", "Dry")

df[, zone_name := factor(unname(ZONE_LABELS[as.character(climate_zone)]), levels = ZONE_ORDER)]
df <- df[!is.na(zone_name)]  
pointsize <-5
basesize  <- 30

## make the figure!
plot <- ggplot(df, aes(x = mean_n_obs, y = mean_prop_wet, color = dataset)) +
  geom_point(size = pointsize) +
  scale_color_manual(values = c("GLAD" = "#2a5674", "GSWO" = "#68abb8")) +
  scale_x_continuous(
    breaks = scales::breaks_width(1),
    labels = scales::label_number(accuracy = 1)) +
  facet_grid(~ zone_name, scales = "free") +
  theme_bw(base_size = basesize) +
  theme(axis.text.x          = element_text(angle = 45, hjust = 1),
        legend.position      = "bottom",
        legend.justification = "center",
        legend.text          = element_text(size = basesize),
        legend.title         = element_blank(),
        panel.grid.major     = element_blank(),
        panel.grid.minor     = element_blank(),
        strip.background     = element_rect(fill = "grey90"),
        strip.text           = element_text(face = "bold")) +
  guides(color = guide_legend(override.aes = list(size = pointsize))) +
  ylab('Proportion of observations\nfrom wet months') +
  xlab('Number of observations')

## save
ggsave("seasonality_fig.png",
       plot, width = 20, height = 10, dpi = 300)
