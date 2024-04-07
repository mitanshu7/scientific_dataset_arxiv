## Importing the required libraries
import pandas as pd
import os
from time import time

#####################################################################################################################
#####################################################################################################################

## Track the time taken to process the metadata
start_time = time()

#####################################################################################################################
#####################################################################################################################

## Creating a list for the year and month
def create_yy_list(start_year, end_year):
    yy_list = []
    for year in range(start_year, end_year + 1):
        yy = year % 100

        ## Pad a 0 if the year is less than 10
        if len(str(yy)) == 1:
            yy = '0' + str(yy)
            
        yy_list.append(yy)

    ## Convert the yymm list to string
    yy_list = [str(i) for i in yy_list]

    return yy_list

#####################################################################################################################
#####################################################################################################################

## Download the arXiv metadata from Kaggle
from kaggle.api.kaggle_api_extended import KaggleApi
api = KaggleApi() # Create an API client object
api.authenticate() # Authenticate the API client

## Defining paths
download_path = 'kaggle'
data_file = f'{download_path}/arxiv-metadata-oai-snapshot.json'

## Download the dataset if it doesn't exist
if not os.path.exists(data_file):
    print(f'Downloading {data_file}')
    api.dataset_download_files('Cornell-University/arxiv', path=download_path, unzip=True)
else:
    print(f'{data_file} already exists')

#####################################################################################################################
#####################################################################################################################

## Load the entire dataset into a pandas dataframe
print('Loading the entire dataset into a pandas dataframe')
arxiv_metadata_all = pd.read_json(data_file, lines=True, convert_dates=True)

## Create a list of yy from 2007 to 2023
yy_list = create_yy_list(2007, 2023)


for yy in yy_list:
    print(f'Extracting the metadata for the papers with id that starts with {yy}')
    arxiv_metadata = arxiv_metadata_all[arxiv_metadata_all['id'].str.match(yy)] 

    ## Save the dataframe to a parquet file
    print('Saving the dataframe to a parquet file')
    arxiv_metadata.to_parquet(f'kaggle/arxiv_metadata_20{yy}.parquet', index=False)

    ## Print the shape of the metadata dataframe
    print(f'Shape of metadata dataframe: {arxiv_metadata.shape}')

## Print the time taken to process the metadata
end_time = time()
print(f'Time taken: {end_time - start_time:.2f} seconds')