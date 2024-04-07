## Importing the required libraries
import pandas as pd
import os
from time import time

## Track the time taken to process the metadata
start_time = time()

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

## Load the entire dataset into a pandas dataframe
print('Loading the entire dataset into a pandas dataframe')
arxiv_metadata_all = pd.read_json(data_file, lines=True, convert_dates=True)

## Extract the metadata for the papers with id that starts with 20, 21, 22, 23
print('Extracting the metadata for the papers with id that starts with 20, 21, 22, 23')
arxiv_metadata = arxiv_metadata_all[arxiv_metadata_all['id'].str.match(r'2[0-3]')] 

## Only save columns id, abstract, title
print('Saving only columns id, abstract, title')
arxiv_metadata = arxiv_metadata[['id', 'abstract', 'title']]

## Turn the contents of the dataframe to lowercase
print('Turning the contents of the dataframe to lowercase')
arxiv_metadata = arxiv_metadata.apply(lambda x: x.str.lower())

## Save the dataframe to a parquet file
print('Saving the dataframe to a parquet file')
arxiv_metadata.to_parquet('kaggle/arxiv_metadata_2020_to_2023.parquet', index=False)

## Print the shape of the metadata dataframe
print(f'Shape of metadata dataframe: {arxiv_metadata.shape}')

## Print the time taken to process the metadata
end_time = time()
print(f'Time taken: {end_time - start_time:.2f} seconds')