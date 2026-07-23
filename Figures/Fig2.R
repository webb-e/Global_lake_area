library(rnaturalearth)
library(sf)
library(cowplot)
library(RColorBrewer)
library(tidyverse)

#########
### READ IN DATA AND DEAL WITH PROJECTIONS
########

GSWOdata<-st_read('Obs_freq_area_trends_100km_GSWO.shp')
GLADdata<-st_read('Obs_freq_area_trends_100km_GLAD.shp')

# earth background
land <- ne_download(scale = "medium", type = "land", category = "physical", returnclass = "sf")
coast <- ne_download(scale = "medium", type = "coastline", category = "physical", returnclass = "sf")

# Project to Robinson projection (ESRI:54030)
land<-st_transform(land, st_crs(GSWOdata))
coast<-st_transform(coast, st_crs(GSWOdata))

#########
### PREPARE DATA
########
data<-rbind(GSWOdata, GLADdata)

# frq_chg (from the python script): % difference in mean obs frequency,
# 2013-2021 vs 1999-2012

data$dataset <- factor(data$dataset, levels = c('GSWO', 'GLAD'))

### one dataset per panel row
# max 12 obs/yr (monthly), so bins are simply 1-12
top_data <- data %>% mutate(panel = "Observation\nfrequency",
                            value_bin = cut(avg_freq, breaks = 0:12,
                                            labels = as.character(1:12),
                                            include.lowest = TRUE))
# discrete bins: distinct red bin for decreases (<=0), narrow bins at the low
# end for more resolution where most of the data sits
mid_breaks <- c(-Inf, 0, 25, 50, 75, 100, 150, 200, 300, Inf)
mid_labels <- c("≤0", "0–25", "25–50", "50–75", "75–100", "100–150", "150–200", "200–300", ">300")
mid_colors <- c("grey60", colorRampPalette(RColorBrewer::brewer.pal(9, "YlGnBu"))(8))

mid_data <- data %>% mutate(panel = "Change in obs.\nfrequency",
                            value_bin = cut(frq_chg, breaks = mid_breaks,
                                            labels = mid_labels))


## for figuring out how to cut off white space
bbox <- st_bbox(land)

## plotting options
text_size = 95

## shared theme pieces: horizontal legend below each map
legend_theme <- theme(
  legend.position = 'bottom',
  legend.direction = 'horizontal',
  legend.title.position = 'top',
  legend.title = element_text(size = text_size*.9, hjust = 0.5, angle = 0),
  legend.text = element_text(size = text_size*.65),
  legend.key.width = unit(10, "cm"),
  legend.key.height = unit(1, "cm"),
  legend.margin = margin(t = 5, r = 0, b = 0, l = 0, unit = "pt"))

#------- ------- -------
#------- PLOTS -------
#------- ------- -------

### (top) average observation frequency -- purple sequential scale
top_plot<-ggplot() +
  geom_sf(data = land, fill = "#e0dac9", color = "#e0dac9", linewidth = 0) +
  geom_sf(data= top_data, aes(fill=value_bin),
          color=NA, linewidth = 0)+
  geom_sf(data = land, fill = NA, color = "grey30", linewidth = 0.25) +
  coord_sf(xlim = c(-12000000, 16000000),
           ylim = c(-6000000, 8748765)) +
  # discrete 1-unit bins; labels sit centered below each color swatch
  scale_fill_manual(values = colorRampPalette(RColorBrewer::brewer.pal(9, "BuPu"))(12),
                    drop = FALSE, na.translate = FALSE,
                    name = expression("Observations lake"^-1*" yr"^-1)) +
  guides(fill = guide_legend(nrow = 1, byrow = TRUE,
                             title.position = "top", label.position = "bottom",
                             label.hjust = 0.5))+
  facet_grid(panel~dataset, switch = "y")+
  theme_void(base_size = text_size) +
  theme(plot.margin = margin(t = 10, r = 10, b = 0, l = 10),
        panel.background = element_rect(fill = "grey95", color = NA),
        strip.text.y.left = element_blank(),
        strip.background.y = element_blank(),
        strip.background = element_blank(),
        panel.border = element_rect(color = "black", fill = NA, linewidth = 0.5),
        strip.text = element_text(color = "black", size=text_size, face = "bold",
                                  margin = margin(t = 10, b = 20, l=25, r=25)),
        text = element_text(size = text_size)) +
  legend_theme +
  # discrete legend keys: connected boxes (no gaps), like a stepped colorbar
  theme(legend.key.width = unit(5, "cm"),
        legend.key.height = unit(1.2, "cm"),
        legend.spacing.x = unit(0, "pt"),
        legend.key.spacing.x = unit(0, "pt"),
        legend.key.spacing.y = unit(0, "pt"))

### (middle) relative trend in observation frequency (%/yr, normalized by
### average frequency) -- YlGnBu scale (the old top scheme). NOTE: this is a
### sequential scale; if you get a lot of negative values, switch to a
### diverging scale so 0 stands out.
mid_plot<-ggplot() +
  geom_sf(data = land, fill = "#e0dac9", color = "#e0dac9", linewidth = 0) +
  geom_sf(data= mid_data, aes(fill=value_bin),
          color=NA, linewidth = 0)+
  geom_sf(data = land, fill = NA, color = "grey30", linewidth = 0.25) +
  coord_sf(xlim = c(-12000000, 16000000),
           ylim = c(-6000000, 8748765)) +
  scale_fill_manual(values = mid_colors, drop = FALSE, na.translate = FALSE,
                    name = "Change in observation frequency (%),\n2013-2021 vs 1999-2012") +
  guides(fill = guide_legend(nrow = 1, byrow = TRUE,
                             title.position = "top", label.position = "bottom",
                             label.hjust = 0.5))+
  facet_grid(panel~dataset, switch = "y")+
  theme_void(base_size = text_size) +
  theme(plot.margin = margin(t = 0, r = 10, b = 0, l = 10),
        panel.background = element_rect(fill = "grey95", color = NA),
        strip.background = element_blank(),
        strip.text.y.left = element_blank(),
        # invisible x strips sized like the top panel's GSWO/GLAD headers so
        # both rows render their maps at identical sizes
        strip.text.x = element_text(color = "transparent", size = text_size, face = "bold",
                                    margin = margin(t = 10, b = 20, l=25, r=25)),
        panel.border = element_rect(color = "black", fill = NA, linewidth = 0.5),
        text = element_text(size = text_size)) +
  legend_theme +
  # discrete legend keys: connected boxes (no gaps), like a stepped colorbar
  theme(legend.key.width = unit(9.5, "cm"),
        legend.key.height = unit(1.2, "cm"),
        legend.spacing.x = unit(0, "pt"),
        legend.key.spacing.x = unit(0, "pt"),
        legend.key.spacing.y = unit(0, "pt"))

#------- ------- -------
#------- COMBINE PLOTS -------
#------- ------- -------
all_plots<-plot_grid(top_plot, NULL, mid_plot,
                     rel_heights = c(1, -0.12, 1),
                     labels = c("a", "", "b"), label_size = text_size,
                     label_fontface = "bold", label_x = 0.01, label_y = 0.99,
                     axis = "tblr", ncol=1, align = "hv")
ggsave("Fig_2.jpg", all_plots,  width=580, height=370,
       units="mm",  dpi=200, scale=2.5,
       limitsize = FALSE,
       path='')
