`global_over_time.R` produces basic statistics on how aggregated annual lake area relates to observation frequency and data collected post-2013

`global_linear_models.R` takes the global total, mean, and median annual lake areas and relates them to observation frequency, year, and pre/post Landsat-8 using linear regression.

`wet_dry.R` culsters months into wet and dry categories for each lake

`breakpoint.R` determines which years lakes experience structural changes in their time series.


## need to add 
`full_base_models.R` estimates area trends for individual lakes accounting for bias and not accounting for bias
`full_vs_basemodel.py` takes the trend estimates and counts the number of lakes with positive/negative trends using the models accounting for and not accounting for bias. This file also outputs the shapefile used to generate figue 2.
