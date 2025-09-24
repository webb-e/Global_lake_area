########
### This code takes the global total, mean, and median annual lake areas 
### and produces a global figure of the trends of these variables over time as well as
### how they relate to observation frequency.
### There is a seperate file that makes the same figure, but by climate_zone
###
### Last updated Sept 24, 2025 by E. Webb
########

library(arrow)
library(tidyverse)
library(data.table)
library(scales)
library(patchwork)
library(EnvStats)
library(broom)
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


#==========================
# =====  stats for statements in paper
#==========================
dat_gswo <- data[data$dataset == "GSWO" & data$climate_zone == "Global", ]
dat_glad <- data[data$dataset == "GLAD" & data$climate_zone == "Global", ]

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


#==========================
# ===== Generate ED Table 2
#==========================
#####   the total, mean, and median lake area were strongly correlated with observation frequency

cor_tests <- list(
  list(test = cor.test(dat_gswo$total_lake_area, dat_gswo$n_obs_sum, method = "pearson"),
       dataset = "GSWO", variable = "total_lake_area", outcome = "n_obs_sum"),
  list(test = cor.test(dat_glad$total_lake_area, dat_glad$n_obs_sum, method = "pearson"),
       dataset = "GLAD", variable = "total_lake_area", outcome = "n_obs_sum"),
  list(test = cor.test(dat_gswo$mean_lake_area, dat_gswo$n_obs_mean, method = "pearson"),
       dataset = "GSWO", variable = "mean_lake_area", outcome = "n_obs_mean"),
  list(test = cor.test(dat_glad$mean_lake_area, dat_glad$n_obs_mean, method = "pearson"),
       dataset = "GLAD", variable = "mean_lake_area", outcome = "n_obs_mean"),
  list(test = cor.test(dat_gswo$median_lake_area, dat_gswo$n_obs_median, method = "pearson"),
       dataset = "GSWO", variable = "median_lake_area", outcome = "n_obs_median"),
  list(test = cor.test(dat_glad$median_lake_area, dat_glad$n_obs_median, method = "pearson"),
       dataset = "GLAD", variable = "median_lake_area", outcome = "n_obs_median")
)

# Extract results into a tidy data frame
results_df <- map_dfr(cor_tests, function(x) {
  test_result <- tidy(x$test)
  data.frame(
    dataset = x$dataset,
    variable = x$variable,
    outcome = x$outcome,
    r = test_result$estimate,
    conf_low = test_result$conf.low,
    conf_high = test_result$conf.high,
    df = test_result$parameter,
    p_value = test_result$p.value,
    stringsAsFactors = FALSE
  )
})

# Clean up and format the table
correlation_table <- results_df %>%
  mutate(
    # Round numeric values for cleaner presentation
    r = round(r, 3),
    conf_low = round(conf_low, 3),
    conf_high = round(conf_high, 3),
    p_value = case_when(
      p_value < 0.001 ~ "<0.001",
      TRUE ~ as.character(round(p_value, 3))
    ),
    # Create confidence interval column
    ci_95 = paste0("[", conf_low, ", ", conf_high, "]"),
    # Create a descriptive test name
    test_name = paste(dataset, variable, "vs", outcome, sep = " - ")
  ) %>%
  select(test_name, r, ci_95, df, p_value) %>%
  rename(
    "Test" = test_name,
    "r (Pearson)" = r,
    "95% CI" = ci_95,
    "df" = df,
    "p-value" = p_value
  )


#==========================
# ===== Generate ED Table 1
#==========================
###   Total, mean, and median global lake area derived from the GSWO and GLAD
###   surface water products increased over the 23-year record 


kt_extract <- function(x, y, df) {
  res <- kendallTrendTest(reformulate(x, y), data = df)
  
  # Extract confidence interval for slope (if available)
  conf_int <- if(!is.null(res$interval) && !is.null(res$interval$limits)) {
    res$interval$limits
  } else {
    c(NA, NA)
  }
  
  tibble(
    tau = unname(res$estimate[["tau"]]),
    slope = unname(res$estimate[["slope"]]),  # Theil-Sen slope estimate
    conf_low = conf_int[1],
    conf_high = conf_int[2],
    z_statistic = res$statistic,  # Test statistic
    n = res$sample.size,  # Sample size (related to degrees of freedom)
    p.value = res$p.value
  )
}

# Run Kendall trend tests for all combinations
kendall_results <- bind_rows(
  kt_extract("year", "median_lake_area", data[data$dataset == "GSWO" & data$climate_zone == "Global", ]) %>%
    mutate(dataset = "GSWO", metric = "median"),
  kt_extract("year", "mean_lake_area", data[data$dataset == "GSWO" & data$climate_zone == "Global", ]) %>%
    mutate(dataset = "GSWO", metric = "mean"),
  kt_extract("year", "total_lake_area", data[data$dataset == "GSWO" & data$climate_zone == "Global", ]) %>%
    mutate(dataset = "GSWO", metric = "total"),
  kt_extract("year", "median_lake_area", data[data$dataset == "GLAD" & data$climate_zone == "Global", ]) %>%
    mutate(dataset = "GLAD", metric = "median"),
  kt_extract("year", "mean_lake_area", data[data$dataset == "GLAD" & data$climate_zone == "Global", ]) %>%
    mutate(dataset = "GLAD", metric = "mean"),
  kt_extract("year", "total_lake_area", data[data$dataset == "GLAD" & data$climate_zone == "Global", ]) %>%
    mutate(dataset = "GLAD", metric = "total")
)

# Format the results table
kendall_table <- kendall_results %>%
  mutate(
    # Round numeric values for cleaner presentation
    tau = round(tau, 3),
    slope = round(slope, 4),
    conf_low = round(conf_low, 4),
    conf_high = round(conf_high, 4),
    z_statistic = round(z_statistic, 3),
    p.value = case_when(
      p.value < 0.001 ~ "<0.001",
      TRUE ~ as.character(round(p.value, 3))
    ),
    # Create confidence interval column for slope
    slope_ci_95 = case_when(
      !is.na(conf_low) & !is.na(conf_high) ~ paste0("[", conf_low, ", ", conf_high, "]"),
      TRUE ~ "Not available"
    ),
    # Create descriptive test name
    test_name = paste(dataset, metric, "lake area", sep = " - "),
    # Effect size interpretation (rough guidelines for tau)
    effect_size = case_when(
      abs(tau) < 0.1 ~ "Negligible",
      abs(tau) < 0.3 ~ "Small",
      abs(tau) < 0.5 ~ "Medium",
      TRUE ~ "Large"
    )
  ) %>%
  select(test_name, tau, effect_size, slope, slope_ci_95, z_statistic, n, p.value) %>%
  rename(
    "Test" = test_name,
    "τ (Kendall's tau)" = tau,
    "Effect Size" = effect_size,
    "Slope (Theil-Sen)" = slope,
    "Slope 95% CI" = slope_ci_95,
    "Z-statistic" = z_statistic,
    "Sample Size (n)" = n,
    "p-value" = p.value
  )

# Display the table
print(kendall_table)

# Alternative with more detailed output showing all statistics
kendall_detailed <- kendall_results %>%
  mutate(
    across(c(tau, slope, conf_low, conf_high, z_statistic), ~ round(.x, 4)),
    p_formatted = case_when(
      p.value < 0.001 ~ "<0.001",
      p.value < 0.01 ~ paste0("p = ", round(p.value, 3)),
      TRUE ~ paste0("p = ", round(p.value, 2))
    ),
    ci_formatted = case_when(
      !is.na(conf_low) & !is.na(conf_high) ~ paste0("95% CI [", conf_low, ", ", conf_high, "]"),
      TRUE ~ "CI not available"
    )
  )
