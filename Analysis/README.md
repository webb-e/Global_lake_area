`buffer_sensitivity.R` recomputes aggregated lake area model results for 100,000 lakes randomly selected from the set of lakes that were observed in every year at varying buffer distances.

`compute_observationfrequency.R` calculates per-lake observation frequency by climate zone, percent increase in observation frequency, and trends in observation frequency over time.

`global_trend_stats.R` produces basic statistics on how aggregated annual lake area relates to observation frequency and data collected post-2013. Also produces supplementary tables 1 and 2.

`lake_area_models_all_methods.R` takes the global total, mean, and median annual lake areas and relates them to observation frequency, year, and pre/post Landsat-8 using generalized linear regression. Analyzes the full record, the complete-record lakes, and the composition-adjusted series.

`wet_dry.R` culsters months into wet and dry categories for each lake
