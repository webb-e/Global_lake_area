library(rnaturalearth)
library(sf)
library(cowplot)
library(RColorBrewer)
library(tidyverse)

#########
### READ IN DATA AND DEAL WITH PROJECTIONS
########

GSWOdata<-st_read('/Users/elizabethwebb/Library/CloudStorage/GoogleDrive-webb.elizabeth.e@gmail.com/My Drive/PostDoc/Landsat8/shapefiles/L8_trends_withwithout_100KM_GSWO.shp')
GLADdata<-st_read('/Users/elizabethwebb/Library/CloudStorage/GoogleDrive-webb.elizabeth.e@gmail.com/My Drive/PostDoc/Landsat8/shapefiles/L8_trends_withwithout_100KM_GLAD.shp')

# earth background
land <- ne_download(scale = "medium", type = "land", category = "physical", returnclass = "sf")
coast <- ne_download(scale = "medium", type = "coastline", category = "physical", returnclass = "sf")

# Project to Robinson projection (ESRI:54030)
land<-st_transform(land, st_crs(GSWOdata))
coast<-st_transform(coast, st_crs(GSWOdata))

#########
### PREPARE DATA 
########
GLADdata$adj_median<-ifelse(GLADdata$adj_count == 0, 0, GLADdata$adj_median)
GLADdata$adj_mean<-ifelse(GLADdata$adj_count == 0, 0, GLADdata$adj_mean)
GLADdata$un_mean<-ifelse(GLADdata$un_count == 0, 0, GLADdata$un_mean)
GLADdata$un_median<-ifelse(GLADdata$un_count == 0, 0, GLADdata$un_median)
GLADdata$difference_median<-GLADdata$un_median - GLADdata$adj_median
GLADdata$difference_mean<-GLADdata$un_mean - GLADdata$adj_mean
GLADdata$difference_sum<-GLADdata$un_sum - GLADdata$adj_sum

GSWOdata$adj_median<-ifelse(GSWOdata$adj_count == 0, 0, GSWOdata$adj_median)
GSWOdata$adj_mean<-ifelse(GSWOdata$adj_count == 0, 0, GSWOdata$adj_mean)
GSWOdata$un_mean<-ifelse(GSWOdata$un_count == 0, 0, GSWOdata$un_mean)
GSWOdata$un_median<-ifelse(GSWOdata$un_count == 0, 0, GSWOdata$un_median)
GSWOdata$difference_median<-GSWOdata$un_median - GSWOdata$adj_median
GSWOdata$difference_mean<-GSWOdata$un_mean - GSWOdata$adj_mean
GSWOdata$difference_sum<-GSWOdata$un_sum - GSWOdata$adj_sum

data<-rbind(GSWOdata, GLADdata)

### get data in a better format for plotting
# convert to tibble and bring geometry in as column
data_tbl <- data %>%
  as_tibble() %>%
  mutate(geometry = st_geometry(data))

# pivot longer
data_long <- data_tbl %>%
  pivot_longer(
    cols = c(adj_mean, adj_sum, adj_median, adj_count, un_mean, un_sum, un_median, un_count,
             difference_mean, difference_sum, difference_median),
    names_to = c("model", "statistic"),
    names_pattern = "(adj|un|difference)_(mean|sum|median|count)",
    values_to = "value") %>%
  mutate( model = recode(model, "adj" = "With bias adjustment", "un" = "Without bias adjustment",
                         "difference" = "Effect of bias"))

# convert back to sf
data_long <- st_as_sf(data_long, crs = st_crs(data))

## for figuring out how to cut off white space
bbox <- st_bbox(land)


#########
### PLOT FOR TOTAL CHANGE IN LAKE AREA (SUM)
########

### set up data -- get range of legend and get datasets in the right order
sum_data <- data_long %>% 
  filter(statistic == "sum" & model != "Effect of bias") %>% mutate(value = value*23) 
iqr_bounds_sum <- quantile(sum_data$value, probs = c(0.01, 0.99), na.rm = TRUE)
sum_data$dataset <-factor(sum_data$dataset, levels=c('GSWO', 'GLAD'))
sum_data$model <- factor(sum_data$model,
                         levels = c("Without bias adjustment", "With bias adjustment"),
                         labels = c("Without bias\nadjustment", "With bias\nadjustment"))
lower_sum <- iqr_bounds_sum[[1]]
upper_sum <- iqr_bounds_sum[[2]]
limit_sum <- round(max(abs(lower_sum), abs(upper_sum)),0)

### repeat for the difference column
sum_datadiff <- data_long %>% filter(statistic == "sum" & model == "Effect of bias") %>% mutate(value = value*23) 
iqr_boundsdiff <- quantile(sum_datadiff$value, probs = c(0.01, 0.99), na.rm = TRUE)
sum_datadiff$dataset <-factor(sum_datadiff$dataset, levels=c('GSWO', 'GLAD'))
sum_datadiff$model <- factor(sum_datadiff$model,
                             levels = c("Effect of bias"),
                             labels = c("Effect\nof bias"))
lowerdiff <- iqr_boundsdiff[[1]]
upperdiff <- iqr_boundsdiff[[2]]
limit_diff <- round(max(abs(lowerdiff), abs(upperdiff)),0)


## plotting options
text_size = 95

#------- ------- ------- 
#------- PLOTS------- 
#------- ------- ------- 
### unadjusted
top_plot<-ggplot() +
  geom_sf(data = land, fill = "#e0dac9", color = "#e0dac9", linewidth = 0) +
  geom_sf(data= sum_data, aes(fill=value), 
          color=NA, linewidth = 0)+
  geom_sf(data = land, fill = NA, color = "grey30", linewidth = 0.25) +
  coord_sf(xlim = c(-12000000, 16000000),  
           ylim = c(-6000000, 8748765)) +
  scale_fill_gradient2(low ='#d7191c',  mid = "#f9f8eb", high = '#2c7bb6', midpoint = 0,
                       limits = c(-limit_sum, limit_sum),oob = scales::squish,
                       breaks = c(-30, -15, 0,15, 30),                                               
                       name = expression("Total change in lake area (km"^2*") ")) + 
  guides(fill = guide_colorbar(frame.colour = "black", frame.linewidth = 0.5))+
  facet_grid(model~dataset, switch = "y")+
  theme_void(base_size = text_size) +
  theme(legend.title.position='right',
        legend.title = element_text(size = text_size*.9,hjust = 0.5, angle = 270),
        legend.text = element_text(size = text_size*.9),
        legend.key.width = unit(1, "cm"),   
        legend.key.height = unit(10, "cm"), 
        legend.margin = margin(t = 0, r = 0, b = 0, l = 50, unit = "pt"),
        legend.direction='vertical',
        plot.margin = margin(t = 10, r = 10, b = 0, l = 10),
        strip.text.y.left = element_text(angle = 90, margin(t=30, b=30) ),
        strip.background = element_rect(fill = "gray90", color = "black", linewidth=1),
        panel.border = element_rect(color = "black", fill = NA, linewidth = 0.5),
        strip.text = element_text(color = "black", size=text_size,
                                         margin = margin(t = 45, b = 25, l=25, r=25)),
        text = element_text(size = text_size))
     

  
bottom_plot<-ggplot() +
  geom_sf(data = land, fill = "#e0dac9", color = "#e0dac9", linewidth = 0) +
  geom_sf(data= sum_datadiff, aes(fill=value), 
          color=NA, linewidth = 0)+
  geom_sf(data = land, fill = NA, color = "grey30", linewidth = 0.25) +
  coord_sf(xlim = c(-12000000, 16000000),  
           ylim = c(-6000000, 8748765)) +
  scale_fill_gradient2(low ='#512888',  mid = "#f9f8eb", high = '#2F847C', midpoint = 0,
                       limits = c(-limit_diff, limit_diff),
                       oob = scales::squish,
                       breaks = c(-50, -25, 0,25, 50),            
                       name = bquote("Unadjusted - adjusted (" * km^2 * ")"))+
  guides(fill = guide_colorbar(frame.colour = "black", frame.linewidth = 0.5))+
  facet_grid(model~dataset,switch = "y")+
  theme_void(base_size = text_size) +
  theme(plot.margin = margin(t = 0, r = 10, b = 10, l = 10),
        legend.title.position='right',
        legend.title = element_text(size = text_size*.9,hjust = 0.5, angle = 270),
        legend.text = element_text(size = text_size*.9),
        legend.key.width = unit(1, "cm"),   
        legend.key.height = unit(6, "cm"), 
        legend.margin = margin(t = 0, r = 0, b = 0, l = 50, unit = "pt"),
        legend.direction='vertical',
        strip.background = element_rect(fill = "gray90", color = "black", linewidth=1),
        strip.text.y.left = element_text(color = "black", size=text_size,angle = 90,
                                         margin = margin(t = 25, b = 25, l=25, r=25)),
        strip.text.x = element_blank(),
        strip.background.x = element_blank(),
        panel.border = element_rect(color = "black", fill = NA, linewidth = 0.5),
        text = element_text(size = text_size))



#------- ------- ------- 
#------- COMBINE PLOTS ------- 
#------- ------- ------- 
both_plots<-plot_grid(top_plot,NULL, bottom_plot, rel_heights = c(2,-0.425,1.2),
                      axis = "tblr", ncol=1, align = "hv")
ggsave("Fig2_trendmap_100km.jpg", both_plots,  width=580, height=460, 
       units="mm",  dpi=500, scale=2.5,
       limitsize = FALSE,
       path='/Users/elizabethwebb/Library/CloudStorage/GoogleDrive-webb.elizabeth.e@gmail.com/My Drive/PostDoc/Landsat8/figures') 




