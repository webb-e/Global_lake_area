 #!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This file takes the lake-wise trend data from the base model (excluding L8 and n_obs)
and the full model (including L8 and n_obs) and:
    (1) Counts lakes to figure out how many were excluded because the model couldn't run
    (2) Calculates the number/proportion of lakes that had significant and positive/negative trends
        globally and by climate zone
    (3) Calculates basic statistics for the base vs. full model trends
    (4) Averages and sums significant trends by grid_id and exports a map for future figures

@author: elizabethwebb
"""
#%%
import pandas as pd
import glob
import os
import geopandas as gpd
import subprocess
import tempfile

#%%

############################
#######  FILE PATHS  #######
############################

base_model_path = '.../MK_results'
full_model_path = .../GAM_results'
lake_info_path = ".../lake_area_observations.parquet"

shapefile_path = '.../Grid_global_100km.shp'


summary_output_path = '.../model_statistics_by_dataset_climate.csv'
map_output_path = '.../shapefiles/'

grid_id_csv = '.../lake_grid100_match.csv'
##GDAL info
ogr2ogr_path = '/opt/homebrew/bin/ogr2ogr'
#%%
############################
#######   FUNCTIONS  #######
############################

def read_parquet_folder(folder_path, columns, subset=None):
    """Read all parquet files in a folder with specified columns"""
    parquet_files = glob.glob(os.path.join(folder_path, "*.parquet"))
    dfs = []
    for file in parquet_files:
        df = pd.read_parquet(file, columns=columns)
        dfs.append(df)
    combined_df = pd.concat(dfs, ignore_index=True)
    if subset is not None:
        combined_df = combined_df.drop_duplicates(subset=subset)
    return combined_df

def calculate_stats(series):
    # Convert m² to km²
    series_km2 = series * 1e-6
    return {
        'median_trend': series_km2.median(),
        'mean_trend': series_km2.mean(),
        'std_trend': series_km2.std(),
        'sum_trend': series_km2.sum(),
    }
#%%
############################
####### READ IN DATA #######
############################
basemodel_columns = ['lake_id', 'dataset', 'p_value', 'sen_slope']
base_df = read_parquet_folder(base_model_path, basemodel_columns, subset=['lake_id', 'dataset'])

fullmodel_columns = ['lake_id', 'dataset', 'year_coef_full', 'year_pval_full']
full_df = read_parquet_folder(full_model_path, fullmodel_columns, subset=['lake_id', 'dataset'])

lake_info_df1 = pd.read_parquet(lake_info_path, columns=['lake_id', 'climate_zone'])
lake_info_df = lake_info_df1.drop_duplicates(subset=['lake_id'])

lake_info_df2 = pd.read_parquet(lake_info_path, columns=['lake_id', 'climate_zone', 'mean_lake_size','dataset'])

gdf_grid = gpd.read_file(shapefile_path)

grid_id_list = pd.read_csv(grid_id_csv)

#%%

############################
####### CLEAN/FILTER DATA #######
############################

#Identify lake_id/dataset combinations that didn't run in the full model

invalid_combinations = full_df[full_df['year_coef_full'].isna()][['lake_id', 'dataset']].drop_duplicates()

# Create a composite key for filtering
base_df['composite_key'] = base_df['lake_id'].astype(str) + '_' + base_df['dataset'].astype(str)
full_df['composite_key'] = full_df['lake_id'].astype(str) + '_' + full_df['dataset'].astype(str)
invalid_combinations['composite_key'] = invalid_combinations['lake_id'].astype(str) + '_' + invalid_combinations['dataset'].astype(str)

# Store total combinations
total_MK_combinations = len(base_df[['lake_id', 'dataset']].drop_duplicates())
total_gam_combinations = len(full_df[['lake_id', 'dataset']].drop_duplicates())

# Filter out combinations where the full model didn't run
base_filtered = base_df[~base_df['composite_key'].isin(invalid_combinations['composite_key'])].copy()
full_filtered = full_df[~full_df['composite_key'].isin(invalid_combinations['composite_key'])].copy()

# Only consider lakes with statstically significant trends
base_pval_filtered = base_filtered[base_filtered['p_value'] < 0.05].copy()
full_pval_filtered = full_filtered[full_filtered['year_pval_full'] < 0.05].copy()


# Merge datasets
merged_df = pd.merge(
    base_pval_filtered[['lake_id', 'dataset', 'sen_slope', 'p_value']],
    full_pval_filtered[['lake_id', 'dataset', 'year_coef_full', 'year_pval_full']],
    on=['lake_id', 'dataset'],
    how='outer'
)

final_df = pd.merge(
    merged_df,
    lake_info_df,
    on='lake_id',
    how='left'
)

# Rename columns 
final_df = final_df.rename(columns={
    'year_coef_full': 'adjusted',
    'sen_slope': 'unadjusted'
})
#%%
############################
####### LAKE COUNTS #######
############################

# Get the total number of lakes and total lake area before any filtering
total_lakes_climate = (
    lake_info_df2
    .groupby(['dataset', 'climate_zone'])
    .agg(
        n_all_lakes=('lake_id', 'count'),
        total_lake_area_km2=('mean_lake_size', lambda x: round(x.sum() * 1e-6, 1))) # convert m2 to km2
    .reset_index())
total_lakes_global = (
    lake_info_df2
    .groupby('dataset')
    .agg(
        n_all_lakes=('lake_id', 'count'),
        total_lake_area_km2=('mean_lake_size', lambda x: round(x.sum() * 1e-6, 1)))
    .reset_index())

# Calculate lakes with sufficient data (after removing failed/insufficient_data, but before 
# filtering for signficiant trends)

base_sufficient = pd.merge(
    base_filtered[['lake_id', 'dataset', 'p_value', 'sen_slope']],
    lake_info_df,
    on='lake_id',
    how='left')

full_sufficient = pd.merge(
    full_filtered[['lake_id', 'dataset', 'year_pval_full', 'year_coef_full']],
    lake_info_df,
    on='lake_id',
    how='left')

sufficient_combinations = pd.merge(
    base_sufficient[['lake_id', 'dataset']],
    full_sufficient[['lake_id', 'dataset']], 
    on=['lake_id', 'dataset'],
    how='left')

sufficient_with_lake_info = pd.merge(sufficient_combinations, lake_info_df2, on=['lake_id', 'dataset'], how='left')

# lakes with sufficient data by climate zone
sufficient_by_climate = sufficient_with_lake_info.groupby(['dataset', 'climate_zone']).size().reset_index(name='sufficient_data_lakes')

## area of sufficient lakes by climate zone
sufficient_area_climate = (
    sufficient_with_lake_info
    .groupby(['dataset', 'climate_zone'])
    .agg(sufficient_lake_area_km2=('mean_lake_size', lambda x: round(x.sum() * 1e-6, 1)))
    .reset_index()
)

## area of sufficient lakes globally
sufficient_area_global = (
    sufficient_with_lake_info
    .groupby('dataset')
    .agg(sufficient_lake_area_km2=('mean_lake_size', lambda x: round(x.sum() * 1e-6, 1)))
    .reset_index()
)
#%%
############################
####### STATISTICS, ETC. #######
############################

climate_results = []

# Adjusted (full model) climate-zone-level stats
for (dataset, climate_zone), group in (
    full_pval_filtered
    .merge(lake_info_df, on='lake_id', how='left')
    .groupby(['dataset', 'climate_zone'])
):
    total_lakes = total_lakes_climate[
        (total_lakes_climate['dataset'] == dataset) & 
        (total_lakes_climate['climate_zone'] == climate_zone)
    ]['n_all_lakes'].iloc[0] if not total_lakes_climate[
        (total_lakes_climate['dataset'] == dataset) & 
        (total_lakes_climate['climate_zone'] == climate_zone)
    ].empty else 0

    sufficient_data_lakes = sufficient_by_climate[
        (sufficient_by_climate['dataset'] == dataset) & 
        (sufficient_by_climate['climate_zone'] == climate_zone)
    ]['sufficient_data_lakes'].iloc[0] if not sufficient_by_climate[
        (sufficient_by_climate['dataset'] == dataset) & 
        (sufficient_by_climate['climate_zone'] == climate_zone)
    ].empty else 0

    adj_stats = calculate_stats(group['year_coef_full'])
    pos_sig_lakes = (group['year_coef_full'] > 0).sum()
    neg_sig_lakes = (group['year_coef_full'] < 0).sum()
    climate_results.append({
        'dataset': dataset,
        'climate_zone': climate_zone,
        'model_type': 'adjusted',
        'n_significant_lakes': len(group),
        'n_sufficient_data': sufficient_data_lakes,
        'pos_sig_lakes': pos_sig_lakes,
        'neg_sig_lakes': neg_sig_lakes,
        'change_in_lake_area': round(adj_stats['sum_trend'] * 23, 1),
        **adj_stats
    })

# Unadjusted (base model) climate-zone-level stats
for (dataset, climate_zone), group in (
    base_pval_filtered
    .merge(lake_info_df, on='lake_id', how='left')
    .groupby(['dataset', 'climate_zone'])
):
    total_lakes = total_lakes_climate[
        (total_lakes_climate['dataset'] == dataset) & 
        (total_lakes_climate['climate_zone'] == climate_zone)
    ]['n_all_lakes'].iloc[0] if not total_lakes_climate[
        (total_lakes_climate['dataset'] == dataset) & 
        (total_lakes_climate['climate_zone'] == climate_zone)
    ].empty else 0

    sufficient_data_lakes = sufficient_by_climate[
        (sufficient_by_climate['dataset'] == dataset) & 
        (sufficient_by_climate['climate_zone'] == climate_zone)
    ]['sufficient_data_lakes'].iloc[0] if not sufficient_by_climate[
        (sufficient_by_climate['dataset'] == dataset) & 
        (sufficient_by_climate['climate_zone'] == climate_zone)
    ].empty else 0
   
    unadj_stats = calculate_stats(group['sen_slope'])
    pos_sig_lakes = (group['sen_slope'] > 0).sum()
    neg_sig_lakes = (group['sen_slope'] < 0).sum()

    climate_results.append({
        'dataset': dataset,
        'climate_zone': climate_zone,
        'model_type': 'unadjusted',
        'n_significant_lakes': len(group),
        'n_sufficient_data': sufficient_data_lakes,
        'pos_sig_lakes': pos_sig_lakes,
        'neg_sig_lakes': neg_sig_lakes,
        'change_in_lake_area': round(unadj_stats['sum_trend'] * 23, 1),
        **unadj_stats
    })
    
climate_df = pd.DataFrame(climate_results)


global_results = []

# Adjusted (full model) global stats
for dataset, group in (
    full_pval_filtered
    .merge(lake_info_df, on='lake_id', how='left')
    .groupby('dataset')
):
    total_lakes = total_lakes_global[
        total_lakes_global['dataset'] == dataset
    ]['n_all_lakes'].sum() if not total_lakes_global[
        total_lakes_global['dataset'] == dataset
    ].empty else 0

    sufficient_data_lakes = sufficient_by_climate[
        sufficient_by_climate['dataset'] == dataset
    ]['sufficient_data_lakes'].sum() if not sufficient_by_climate[
        sufficient_by_climate['dataset'] == dataset
    ].empty else 0

    adj_stats = calculate_stats(group['year_coef_full'])
    pos_sig_lakes = (group['year_coef_full'] > 0).sum()
    neg_sig_lakes = (group['year_coef_full'] < 0).sum()

    global_results.append({
        'dataset': dataset,
        'climate_zone': 'Global',
        'model_type': 'adjusted',
        'n_significant_lakes': len(group),
        'n_sufficient_data': sufficient_data_lakes,
        'pos_sig_lakes': pos_sig_lakes,
        'neg_sig_lakes': neg_sig_lakes,
        'change_in_lake_area': round(adj_stats['sum_trend'] * 23, 1),
        **adj_stats
    })


# Unadjusted (base model) global stats
for dataset, group in (
    base_pval_filtered
    .merge(lake_info_df, on='lake_id', how='left')
    .groupby('dataset')
):
    total_lakes = total_lakes_global[
        total_lakes_global['dataset'] == dataset
    ]['n_all_lakes'].sum() if not total_lakes_global[
        total_lakes_climate['dataset'] == dataset
    ].empty else 0

    sufficient_data_lakes = sufficient_by_climate[
        sufficient_by_climate['dataset'] == dataset
    ]['sufficient_data_lakes'].sum() if not sufficient_by_climate[
        sufficient_by_climate['dataset'] == dataset
    ].empty else 0

    unadj_stats = calculate_stats(group['sen_slope'])
    pos_sig_lakes = (group['sen_slope'] > 0).sum()
    neg_sig_lakes = (group['sen_slope'] < 0).sum()

    global_results.append({
        'dataset': dataset,
        'climate_zone': 'Global',
        'model_type': 'unadjusted',
        'n_significant_lakes': len(group),
        'n_sufficient_data': sufficient_data_lakes,
        'pos_sig_lakes': pos_sig_lakes,
        'neg_sig_lakes': neg_sig_lakes,
        'change_in_lake_area': round(unadj_stats['sum_trend'] * 23, 1),
        **unadj_stats
    })

global_df = pd.DataFrame(global_results)


#%%
################################
####### CLEAN AND MERGE  #######
################################

climate_df = climate_df.merge(
    sufficient_area_climate,
    on=['dataset', 'climate_zone'],
    how='left')


climate_df = climate_df.merge(
    total_lakes_climate,
    on=['dataset', 'climate_zone'],
    how='left' )

total_lakes_global = total_lakes_global.rename(columns={
    'total_lakes': 'n_all_lakes',
    'lake_area_km2': 'total_lake_area_km2'
})
global_df = global_df.merge(
    total_lakes_global,
    on='dataset',
    how='left')

global_df = global_df.merge(
    sufficient_area_global,
    on='dataset',
    how='left')

boxplot_df = pd.concat([climate_df, global_df], ignore_index=True)

boxplot_df['percent_sufficient'] = ((boxplot_df['n_sufficient_data'] / boxplot_df['n_all_lakes']) * 100).round(1)
boxplot_df['percent_significant'] = ((boxplot_df['n_significant_lakes'] / boxplot_df['n_sufficient_data']) * 100).round(1)
boxplot_df['percent_positive'] = ((boxplot_df['pos_sig_lakes'] / boxplot_df['n_sufficient_data']) * 100).round(1)
boxplot_df['percent_negative'] = ((boxplot_df['neg_sig_lakes'] / boxplot_df['n_sufficient_data']) * 100).round(1)
boxplot_df['percent_change_in_area'] = ((boxplot_df['change_in_lake_area'] / boxplot_df['sufficient_lake_area_km2']) * 100).round(1)
boxplot_df['percent_sufficient_area'] = ((boxplot_df['sufficient_lake_area_km2'] / boxplot_df['total_lake_area_km2']) * 100).round(1)

boxplot_df = boxplot_df[boxplot_df['climate_zone'] != 6]
climate_zone_map = {
    1: "Tropical",
    2: "Dry",
    3: "Temperate",
    4: "Continental",
    5: "Polar"
}

boxplot_df['climate_zone'] = boxplot_df['climate_zone'].replace(climate_zone_map)


#%%
##########################################
#######   SAVE AS .CSV #######
##########################################

boxplot_df.to_csv(summary_output_path, index=False)

#%%
###############################################################
#######  AGGREGATE TRENDS AND PREPARE SHAPEFILE FOR MAP #######
###############################################################

#%%
### convert trend from m2/yr to km2/yr
final_df['adjusted_km2'] = final_df['adjusted']*1e-6
final_df['unadjusted_km2'] = final_df['unadjusted']*1e-6
grid_id_list['lake_id'] = (
    pd.to_numeric(grid_id_list['lake_id'], errors='coerce')  # convert safely to numbers
      .round()
      .astype('Int64')  
      .astype(str)     
)

grid_id_list['grid_id'] = (
    pd.to_numeric(grid_id_list['grid_id'], errors='coerce')
      .round()
      .astype('Int64')
      .astype(str)
)

df_with_grid = final_df.merge(
    grid_id_list,
    on='lake_id',
    how='left')
#%%
#### Take mean, median, and sum of trends (adjusted and unadjusted) by grid_id

agg_dict = {
    'adjusted_km2': ['mean', 'sum', 'median', lambda x: x.notna().sum()],
    'unadjusted_km2': ['mean', 'sum', 'median', lambda x: x.notna().sum()],
}

df_aggregated = df_with_grid.groupby(['grid_id', 'dataset'], as_index=False).agg(agg_dict)

## deal with column names
df_aggregated.columns = [
    col if isinstance(col, str) else '_'.join([str(c) for c in col if c])
    for col in df_aggregated.columns
]
rename_dict = {
    'adjusted_km2_mean': 'adj_mean',
    'adjusted_km2_sum': 'adj_sum',
    'adjusted_km2_median': 'adj_median',
    'unadjusted_km2_mean': 'un_mean',
    'unadjusted_km2_sum': 'un_sum',
    'unadjusted_km2_median': 'un_median',
    'adjusted_km2_<lambda_0>': 'adj_count',
    'unadjusted_km2_<lambda_0>': 'un_count'
}

df_aggregated.rename(columns=rename_dict, inplace=True)

# Merge with grid shapefile
gdf_grid_renamed = gdf_grid.rename(columns={'id': 'grid_id'})

gdf_grid_renamed['grid_id'] = (
    pd.to_numeric(gdf_grid_renamed['grid_id'], errors='coerce')
      .round()
      .astype('Int64')
      .astype(str)
)

df_aggregated['grid_id'] = df_aggregated['grid_id'].astype(str)
gdf_final = gdf_grid_renamed.merge(df_aggregated, on='grid_id', how='inner')

#%%
#####################################
#######  DEAL WITH PROJECTION #######
#####################################

gdf_toproject = gdf_final.copy()

# Drop problematic columns before processing
columns_to_drop = ['grid_area_', 'land_area_', 'land_propo']
gdf_toproject = gdf_toproject.drop(columns=[col for col in columns_to_drop if col in gdf_toproject.columns])

# Save to a temporary shapefile
with tempfile.TemporaryDirectory() as temp_dir:
    temp_shp = os.path.join(temp_dir, "temp.shp")
    gdf_toproject.to_file(temp_shp)
    
    # Use ogr2ogr with wrapdateline
    output_shp = os.path.join(temp_dir, "fixed.shp")
    cmd = [
        ogr2ogr_path,
        "-wrapdateline", 
        "-t_srs", "EPSG:4326",
        output_shp, 
        temp_shp
    ]
    
    try:
        subprocess.run(cmd, check=True)
        gdf_fixed = gpd.read_file(output_shp)
        gdf_fixed = gdf_fixed.to_crs("ESRI:54030")
        
    except subprocess.CalledProcessError:
        print("GDAL command failed - make sure GDAL is installed")

#%%
##################################
#######  EXPORT SHAPEFILES #######
##################################
# Create shapefiles for each dataset with all columns
for dataset in gdf_fixed['dataset'].unique():
    dataset_data = gdf_fixed[gdf_fixed['dataset'] == dataset].copy()
    clean_dataset_name = str(dataset).replace(' ', '_').replace('/', '_')
    
    output_file = os.path.join(map_output_path,f"L8_trends_withwithout_100KM_{clean_dataset_name}.shp")
    
    dataset_data.to_file(output_file)

