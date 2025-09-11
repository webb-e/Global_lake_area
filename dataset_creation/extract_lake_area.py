#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 16 10:55:32 2025

@author: Elizabeth Webb

Description: Extract lake area for every observation with no masked pixels using both
the GSWO and GLAD datasets. Limit  1999-2021 to GLAD 1999-2021.

"""
#%%
import ee
import os
import csv
import concurrent.futures
#%%
ee.Authenticate()

#%%
"""
DEFINE DATASETS
"""

# Define collections and filter years
pickens_collection = ee.ImageCollection('projects/glad/water/C2/individualMonths')
pickens_dataset = pickens_collection.filter(ee.Filter.calendarRange(1999, 2021, 'year')) 

pekel_collection = ee.ImageCollection("JRC/GSW1_4/MonthlyHistory")
pekel_dataset = pekel_collection.filter(ee.Filter.calendarRange(1999, 2021, 'year'))
                  

# Lakes Shapefiles (buffered PLD)
lake_sample = ee.FeatureCollection('projects/focal-psyche-100516/assets/PLD_random_20percent')
lake_sample = lake_sample.merge(ee.FeatureCollection('projects/focal-psyche-100516/assets/PLD_random_20-40percent'))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_random_40-60percent"))
lake_sample = lake_sample.merge( ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_random_60-80percent"))
lake_sample = lake_sample.merge( ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_random_80pluspercent"))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_south_buff_0"))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_south_buff__1"))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_south_buff__2a"))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_south_buff__2b"))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_south_buff__3"))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_south_buff__4"))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_south_buff__5"))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_south_buff__6a"))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_south_buff__6b"))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_south_buff__7"))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_south_buff__8"))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_south_buff__9"))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_south_buff__10"))

#%% 

"""
MAIN FUNCTION - extracts lake area for each lake

Filters lake feature collection to an individual lake, clips surface water products to the lake,
extracts surface water area and masked pixel area within lake polygon, removes observations where
the masked pixel area >900 m2 (i.e., one Landsat pixel)

"""


def getResults(index,lakeID):
    try:
        pld = ee.Feature(lake_sample.filter(ee.Filter.eq('lake_id', lakeID)).first())
        roi = ee.Geometry.MultiPolygon(pld.geometry().getInfo()['coordinates'][:])
        
        # Process Pickens data
        def process_pickens_image(image):
            # Unmask and get binary image (masked/not masked)
            masked_img = image.unmask(-1).eq(-1).clip(pld)
            # Create binary water image
            water_img = image.clip(pld).gt(0)
            # Get area of water
            water_area = water_img.multiply(ee.Image.pixelArea()).rename('water')
            # Get area of masked
            masked_area = masked_img.multiply(ee.Image.pixelArea()).rename('masked')
            both_areas = water_area.addBands(masked_area)
            
            # Sum by lake
            reducer_output = both_areas.reduceRegion(
                geometry=roi,
                scale=30,
                reducer=ee.Reducer.sum()
            )
            
            masked = reducer_output.get('masked')
            water = reducer_output.get('water')
            date = ee.Date(image.get('system:time_start'))
            year = date.get('year')
            month = date.get('month')
            
            # Set values into properties
            pickens_answer = image.set({
                'masked_area': masked,
                'values': {
                    'water': water,
                    'month': month,
                    'year': year,
                    'lake_id': lakeID,
                    'dataset': 'GLAD'
                }
            })
            return pickens_answer
        
        pickens_water = pickens_dataset.map(process_pickens_image)
        
        # Process Pekel data
        def process_pekel_image(image):
            # 0 = no observations, 1 = not water, 2 = water detected
            clip_image = image.clip(pld)
            masked_area = clip_image.eq(0).multiply(ee.Image.pixelArea()).rename('masked')
            water_area = clip_image.eq(2).multiply(ee.Image.pixelArea()).rename('water')
            both_areas = water_area.addBands(masked_area)
            
            # Sum by lake
            reducer_output = both_areas.reduceRegion(
                geometry=roi,
                scale=30,
                reducer=ee.Reducer.sum()
            )
            
            masked = reducer_output.get('masked')
            water = reducer_output.get('water')
            date = ee.Date(image.get('system:time_start'))
            year = date.get('year')
            month = date.get('month')
            
            pekel_answer = image.set({
                'masked_area': masked,
                'values': {
                    'water': water,
                    'month': month,
                    'year': year,
                    'lake_id': lakeID,
                    'dataset': 'GSWO'
                }
            })
            return pekel_answer
        
        pekel_water = pekel_dataset.map(process_pekel_image)
        
        # Filter for clear images (no masked areas)
        clear_pekel = pekel_water.filter(ee.Filter.lt('masked_area', 900))
        clear_pickens = pickens_water.filter(ee.Filter.lt('masked_area', 900))
        clear_images = ee.ImageCollection(clear_pekel.merge(clear_pickens))
        
        return clear_images.aggregate_array('values').getInfo()
    
    except Exception as e:
        print(f"Error processing lake {lakeID}: {e}")
        return None
    
    

#%%  # IF YOU JUST WANT TO TRY ONE
#lakeids = lake_sample.aggregate_array('lake_id').getInfo()
#answer = getResults(0, lakeids[0])
#print(answer)


#%%

"""
FUNCTION TO PARALLEL PROCESS -

Processes lakes in batches, saves to CSVs (be sure to specify directory), skips chunks that already exist

"""

def results_to_csvs(lakeids, start_chunk=1, batch_size=1000, max_workers=None):

    # Ensure Earth Engine is initialized
    ee.Initialize(opt_url='https://earthengine-highvolume.googleapis.com')

    # Output directory
    directory = '/....'

    os.makedirs(directory, exist_ok=True)

    # Process lakes in chunks
    for chunk_index in range(start_chunk, len(lakeids), batch_size):
        # Calculate the chunk number 
        chunk_number = start_chunk + (chunk_index - start_chunk) // batch_size

        # Get current chunk of lakes
        chunk = lakeids[chunk_index:chunk_index + batch_size]

        # Skip if no lakes in chunk
        if not chunk:
            continue

        # Filename for this chunk
        filename = f'ake_area_{chunk_number}.csv'
        filepath = os.path.join(directory, filename)

        # Check if this chunk has already been processed
        if os.path.exists(filepath):
            print(f"Chunk {chunk_number} already exists. Skipping...")
            continue

        print(f"Processing chunk {chunk_number}...")

        # Process this chunk
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Create futures for each lake ID
            futures = [
                executor.submit(getResults, index, lakeID) 
                for index, lakeID in enumerate(chunk)
            ]
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    if result is not None:
                        results.append(result)
                except Exception as e:
                    print(f"Error in processing: {e}")

        # Write results to CSV
        with open(filepath, 'w', newline='') as out_file:
            headers = ['lake_id', 'year', 'month', 'water', 'dataset']
            csv_writer = csv.DictWriter(out_file, fieldnames=headers)
            csv_writer.writeheader()
            for lake_results in results:
                for result_dict in lake_results:
                    csv_writer.writerow({
                        'lake_id': result_dict['lake_id'],
                        'year': result_dict['year'], 
                        'month': result_dict['month'],
                        'water': result_dict['water'],
                        'dataset': result_dict['dataset']
                    })

        print(f"Chunk {chunk_number}: Processed {len(results)} out of {len(chunk)} lakes")

#%%
"""
RUN THE SCRIPT
"""

def main():
   lakeids = lake_sample.aggregate_array('lake_id').getInfo()
   results_to_csvs(lakeids)
   
if __name__ == '__main__':
    main()

