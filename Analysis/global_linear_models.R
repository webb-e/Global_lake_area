#############
### This code takes the global total, mean, and median annual lake areas 
### and relates them to observation frequency, year, and pre/post Landsat-8
###     The first section is data wangling and summary stats.
###   The second section is model selection - using AIC to determine the best model
###   The third section takes the best models and makes a table for export
###   The last section specifies models with a year coefficient and tests for statistical significance
### Last updated on July 25, 2025 by E.Webb
#############

library(arrow)
library(tidyverse)
library(data.table)
library(relaimpo)

#==========================
# ===== read in files
#==========================
# Set the file path
parquet_path <- "....annual_lake_medians_dataset/"  
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
# ===== summary stats
#=========================

#########
##  summary GSWO 
#########

### Global summary (across all climate zones)
gswo <- gswo_dt[, .(
  median_water_median = median(median_water, na.rm = TRUE),
  median_water_mean   = mean(median_water, na.rm = TRUE),
  median_water_sum    = sum(median_water, na.rm = TRUE),
  n_obs_median        = median(n_obs, na.rm = TRUE),
  n_obs_mean          = mean(n_obs, na.rm = TRUE),
  n_obs_sum           = sum(n_obs, na.rm = TRUE)), by = .(dataset, year)][, climate_zone := "Global"]


#########
##  summary GLAD 
#########

### Global summary (across all climate zones)
glad <- glad_dt[, .(
  median_water_median = median(median_water, na.rm = TRUE),
  median_water_mean   = mean(median_water, na.rm = TRUE),
  median_water_sum    = sum(median_water, na.rm = TRUE),
  n_obs_median        = median(n_obs, na.rm = TRUE),
  n_obs_mean          = mean(n_obs, na.rm = TRUE),
  n_obs_sum           = sum(n_obs, na.rm = TRUE)), by = .(dataset, year)][, climate_zone := "Global"]

#### get area in km2
gswo[, total_lake_area := median_water_sum / 1e6]
gswo[, median_lake_area := median_water_median / 1e6]
gswo[, mean_lake_area := median_water_mean / 1e6]

glad[, total_lake_area := median_water_sum / 1e6]
glad[, median_lake_area := median_water_median / 1e6]
glad[, mean_lake_area := median_water_mean / 1e6]

## add in column for Landsat-8
gswo$l8 <-ifelse(gswo$year<2013, 0, 1)
glad$l8 <-ifelse(glad$year<2013, 0, 1)

#==========================
# ===== which models are better - test to see contributions to increasing lake area
#==========================
########
## TOTAL LAKE AREA
#######
## GSWO
model1_GSWO<- lm(total_lake_area ~ n_obs_sum + year + l8, data=gswo)
model2_GSWO<- lm(total_lake_area ~ n_obs_sum + l8, data=gswo)
model3_GSWO<- lm(total_lake_area ~ n_obs_sum + year, data=gswo)
model4_GSWO<- lm(total_lake_area ~ l8 + year, data=gswo)

AIC(model1_GSWO, model2_GSWO, model3_GSWO, model4_GSWO)
## model 2 has lowest AIC 
anova(model1_GSWO, model2_GSWO) # p =0.6715
anova(model1_GSWO, model3_GSWO) # p = 0.09499
## model 2 is not statistically significantly better than model 1; either model 1 or model 2 works,
## but using the l8 step change (model 2) is better using  year (model 3).

## GLAD

model1_GLAD<- lm(total_lake_area ~ n_obs_sum + year + l8, data=glad)
model2_GLAD<- lm(total_lake_area ~ n_obs_sum + l8, data=glad)
model3_GLAD<- lm(total_lake_area ~ n_obs_sum + year, data=glad)
model4_GLAD<- lm(total_lake_area ~ l8 + year, data=glad)

AIC(model1_GLAD, model2_GLAD, model3_GLAD, model4_GLAD)
## model 1 has lowest AIC 
anova(model1_GLAD, model2_GLAD) 
anova(model1_GLAD, model3_GLAD) 
## model 1 is not statistically significantly better than model 2; either model 1 or model 2 works,
## but using the l8 step change (model 2) is better using  year (model 3).

########
## MEAN LAKE AREA
#######
mean0_GSWO<- lm(mean_lake_area ~ n_obs_mean, data=gswo)
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

GSWO_mean_table <- AIC(mean0_GSWO,mean1_GSWO, mean2_GSWO, mean3_GSWO, mean4_GSWO,
                       mean5_GSWO, mean6_GSWO, mean7_GSWO, mean8_GSWO,
                       mean9_GSWO, mean10_GSWO)

GSWO_mean_table[order(GSWO_mean_table$AIC), ]

## mean2_GSWO has best AIC, mean3_GSWO is nearly the same

####GLAD
mean0_GLAD<- lm(mean_lake_area ~ n_obs_mean, data=glad)
mean1_GLAD<- lm(mean_lake_area ~ n_obs_sum + year + l8, data=glad)
mean2_GLAD<- lm(mean_lake_area ~ n_obs_sum + l8, data=glad)
mean3_GLAD<- lm(mean_lake_area ~ n_obs_sum + year, data=glad)
mean4_GLAD<- lm(mean_lake_area ~ l8 + year, data=glad)

AIC(mean0_GLAD,mean1_GLAD, mean2_GLAD, mean3_GLAD, mean4_GLAD)
## mean 1 has lowest AIC 
anova(mean1_GLAD, mean2_GLAD) # p =0.01914

## mean 1 is  statistically significantly better than mean 2;

########
## MEDIAN LAKE AREA
#######
median0_GSWO<- lm(median_lake_area ~ n_obs_median , data=gswo)
median1_GSWO<- lm(median_lake_area ~ n_obs_median + year + l8, data=gswo)
median2_GSWO<- lm(median_lake_area ~ n_obs_median + l8, data=gswo)
median3_GSWO<- lm(median_lake_area ~ n_obs_median + year, data=gswo)
median4_GSWO<- lm(median_lake_area ~ l8 + year, data=gswo)
median5_GSWO<- lm(median_lake_area ~ l8, data=gswo)
median6_GSWO<- lm(median_lake_area ~ n_obs_sum + year + l8, data=gswo)
median7_GSWO<- lm(median_lake_area ~ n_obs_sum + l8, data=gswo)
median8_GSWO<- lm(median_lake_area ~ n_obs_sum + year, data=gswo)
median9_GSWO<- lm(median_lake_area ~ l8 + year, data=gswo)
median10_GSWO<- lm(median_lake_area ~ l8, data=gswo)
median11_GSWO<- lm(median_lake_area ~ n_obs_sum, data=gswo)


GSWO_median_table <- AIC(median0_GSWO, median1_GSWO, median2_GSWO, median3_GSWO, median4_GSWO,
                         median5_GSWO, median6_GSWO, median7_GSWO, median8_GSWO,
                         median9_GSWO, median10_GSWO,median11_GSWO)

GSWO_median_table[order(GSWO_median_table$AIC), ] ## model 8 has lowest AIC, model 7 is nearly the same


## glad
median0_GLAD<- lm(median_lake_area ~ n_obs_median , data=glad)
median1_GLAD<- lm(median_lake_area ~ n_obs_median + year + l8, data=glad)
median2_GLAD<- lm(median_lake_area ~ n_obs_median + l8, data=glad)
median3_GLAD<- lm(median_lake_area ~ n_obs_median + year, data=glad)
median4_GLAD<- lm(median_lake_area ~ l8 + year, data=glad)
median5_GLAD<- lm(median_lake_area ~ l8, data=glad)
median6_GLAD<- lm(median_lake_area ~ n_obs_sum + year + l8, data=glad)
median7_GLAD<- lm(median_lake_area ~ n_obs_sum + l8, data=glad)
median8_GLAD<- lm(median_lake_area ~ n_obs_sum + year, data=glad)
median9_GLAD<- lm(median_lake_area ~ l8 + year, data=glad)
median10_GLAD<- lm(median_lake_area ~ l8, data=glad)
median11_GLAD<- lm(median_lake_area ~ n_obs_sum, data=glad)

GLAD_median_table <- AIC(median0_GLAD, median1_GLAD, median2_GLAD, median3_GLAD, median4_GLAD,
                         median5_GLAD, median6_GLAD, median7_GLAD, median8_GLAD,
                         median9_GLAD, median10_GLAD, median11_GLAD)

GLAD_median_table[order(GLAD_median_table$AIC), ] ## median 7 is best; median 6 not much different

#==========================
# ===== Take best models and determine variance explained and % of maginitude
#==========================

### total area
model2_GSWO<- lm(total_lake_area ~ n_obs_sum + l8, data=gswo)
model2_GLAD<- lm(total_lake_area ~ n_obs_sum + l8, data=glad)

## mean
mean0_GSWO<- lm(mean_lake_area ~ n_obs_mean, data=gswo)
mean1_GLAD<- lm(mean_lake_area ~ n_obs_sum + year + l8, data=glad)

## median
median11_GSWO<- lm(median_lake_area ~ n_obs_sum, data=gswo)
median7_GLAD<- lm(median_lake_area ~ n_obs_sum + l8, data=glad)

## function to extract partial R2 and coefficients
analyze_model <- function(model, data, model_name = "model") {
  coefs <- coef(model)
  predictors <- names(coefs)[-1]  # exclude intercept
  
  # Extract summary info
  model_summary <- summary(model)
  coef_table <- model_summary$coefficients
  
  # Extract predictor p-values
  p_values <- as.vector(coef_table[predictors, "Pr(>|t|)"])
  
  # Range of predictors
  delta <- sapply(predictors, function(var) {
    diff(range(data[[var]], na.rm = TRUE))
  })
  
  # Contribution to predicted change
  contrib <- coefs[predictors] * delta
  total_change <- sum(contrib)
  perc_contrib <- 100 * contrib / total_change
  
  # Model R-squared and p-value from F-stat
  r2_total <- model_summary$r.squared
  model_pval <- pf(model_summary$fstatistic[1],
                   model_summary$fstatistic[2],
                   model_summary$fstatistic[3],
                   lower.tail = FALSE)
  
  # Partial r-squared
  if (length(predictors) >= 2) {
    relimp <- calc.relimp(model, type = "lmg", rela = FALSE)
    r2_parts <- relimp$lmg[predictors]} else {
    r2_parts <- r2_total
    names(r2_parts) <- predictors}
  
  # Combine all
  result <- data.frame(
    model = model_name,
    predictor = predictors,
    coefficient = unname(coefs[predictors]),
    p_value = unname(p_values),
    delta_x = unname(delta),
    contribution = unname(contrib),
    percent_of_total = unname(perc_contrib),
    partial_r2 = unname(r2_parts),
    total_r2 = r2_total,
    model_p_value = model_pval)
  
  return(result)
}

explained_df <- bind_rows(
  analyze_model(model2_GSWO, gswo, "total_GSWO"),
  analyze_model(model2_GLAD, glad, "total_GLAD"),
  analyze_model(mean0_GSWO, gswo, "mean_GSWO"),
  analyze_model(mean1_GLAD, glad, "mean_GLAD"),
  analyze_model(median11_GSWO, gswo, "median_GSWO"),
  analyze_model(median7_GLAD, glad, "median_GLAD"))%>%
  separate(model, into = c("model", "dataset"), sep = "_") %>%
  filter(predictor != "(Intercept)") %>% 
  #select(model, dataset, predictor, percent_of_total, partial_r2, total_r2) %>% 
  arrange(dataset, model)

#==========================
# ===== Now make a table to print/add to excel spreadsheet
#==========================
create_summary_table <- function(model, data, model_name = "model") {
  df <- analyze_model(model, data, model_name)
  
  # Pull total r_squared and model p-value from model
  total_r2 <- unique(df$total_r2)
  model_pval <- unique(df$model_p_value)
  
  # Extract only the rows we want
  metric_names <- c("p_value", "r_squared", "coefficient", "percent_of_total")
  
  # Add r_squared, select relevant columns
  df <- df %>% mutate(r_squared = partial_r2) %>%
    dplyr::select(predictor, coefficient, p_value, percent_of_total, r_squared)
  
  # Build long form table
  long_df <- purrr::map_dfr(metric_names, function(metric) {df %>% dplyr::select(predictor, !!sym(metric)) %>%
      rename(value = !!sym(metric)) %>%
      mutate(metric = metric)}) %>%
    dplyr::select(metric, predictor, value)
  
  # Pivot to wide format
  table <- long_df %>%
    tidyr::pivot_wider(names_from = predictor, values_from = value)
  
  # Reorder columns: metric, year, ..., whole model
  predictor_cols <- colnames(table)
  ordered_cols <- c(
    "metric",
    intersect("year", predictor_cols),                          
    setdiff(predictor_cols, c("metric", "year", "whole model")),
    intersect("whole model", predictor_cols))
  
  table <- table[, ordered_cols]
  
  # Insert whole model stats
  table <- table %>%
    mutate(`whole model` = case_when(
      metric == "p_value"    ~ model_pval,
      metric == "r_squared" ~ total_r2,
      TRUE ~ NA_real_
    ))
  
  # Add model label and spacer
  header <- tibble::tibble(metric = model_name)
  spacer <- tibble::tibble(metric = "")
  
  dplyr::bind_rows(header, table, spacer)
}

summary_table <- bind_rows(
  create_summary_table(model2_GSWO, gswo, "total_GSWO"),
  create_summary_table(model2_GLAD, glad, "total_GLAD"),
  create_summary_table(mean0_GSWO, gswo, "mean_GSWO"),
  create_summary_table(mean1_GLAD, glad, "mean_GLAD"),
  create_summary_table(median11_GSWO, gswo, "median_GSWO"),
  create_summary_table(median7_GLAD, glad, "median_GLAD"))

#==========================
# ===== Models to state there is no temporal trend
#==========================

### total area
total_GSWO<- lm(total_lake_area ~ n_obs_sum + l8 + year, data=gswo)
total_GLAD<- lm(total_lake_area ~ n_obs_sum + l8 + year, data=glad)

## mean
mean_GSWO<- lm(mean_lake_area ~ n_obs_mean+ year + l8, data=gswo)
mean_GLAD<- lm(mean_lake_area ~ n_obs_sum + year + l8, data=glad)

## median
median_GSWO<- lm(median_lake_area ~ n_obs_sum + year + l8, data=gswo)
median_GLAD<- lm(median_lake_area ~ n_obs_sum + l8 + year, data=glad)

get_year_pvalue <- function(model, model_name = "model") {
  coefs <- summary(model)$coefficients
  if ("year" %in% rownames(coefs)) {
    pval <- coefs["year", "Pr(>|t|)"]} else {pval <- NA }
  data.frame(model = model_name, p_value_year = pval)}

year_pvals <- bind_rows(
  get_year_pvalue(total_GSWO, "total_GSWO"),
  get_year_pvalue(total_GLAD, "total_GLAD"),
  get_year_pvalue(mean_GSWO,  "mean_GSWO"),
  get_year_pvalue(mean_GLAD,  "mean_GLAD"),
  get_year_pvalue(median_GSWO,"median_GSWO"),
  get_year_pvalue(median_GLAD,"median_GLAD"))

print(year_pvals)


