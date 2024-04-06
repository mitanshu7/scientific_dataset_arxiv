## This script processes the metadata of the dataset and creates a parquet file with the metadata information
"""
process_metadata.py

This script processes the metadata of the arXiv dataset and creates a pandas DataFrame with the metadata information.

The script performs the following steps:
1. Imports the required libraries.
2. Downloads the arXiv metadata from Kaggle using the Kaggle API.
3. Defines the paths for the downloaded data.
4. Checks if the dataset already exists. If not, it downloads the dataset.
5. Defines a generator function `get_metadata` that yields lines from the large JSON file one by one.
6. Calls the `get_metadata` function to get the metadata from the JSON file.
7. Creates a pandas DataFrame `metadata_df` to store the id, title, and abstract of papers.
8. Iterates over the metadata and extracts the id, title, and abstract of papers published between 2020 and 2023.

Libraries used:
- pandas: A data analysis and manipulation library.
- os: Provides functions for interacting with the operating system.
- json: A library to work with JSON data.
- kaggle: A library to download datasets from Kaggle.

"""
## Importing the required libraries
import pandas as pd
import os
import json
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

## Get metadata from the JSON file, one line at a time
def get_metadata():
  """Generator that yields lines from a large JSON file one by one.

  Yields:
      str: A single line from the JSON file.
  """
  with open(data_file, 'r') as f:
    for line in f:
      yield line

## Get metadata from the JSON file, one line at a time
metadata = get_metadata()

## Create a pandas dataframe to store the id, title, abstract of of papers published between 2020 and 2023
metadata_df = pd.DataFrame(columns=['id', 'title', 'abstract'])

## Iterate over the metadata and extract the id, title, and abstract of papers published between 2020 and 2023
for paper in metadata:

    ## Load the JSON data
    paper_dict = json.loads(paper)

    ## Extract the year and month from the paper id
    paper_id = paper_dict.get('id')
    paper_yymm = paper_id.split('.')[0]
    paper_yy = paper_yymm[0:2]
    # paper_mm = paper_yymm[2:4]

    ## Check if the paper was published between 2020 and 2023
    if 20 <= int(paper_yy) <= 23:

        ## Extract the title and abstract
        title = paper_dict.get('title')
        abstract = paper_dict.get('abstract')

        ## Create a new row with the extracted data
        new_row = pd.DataFrame({'id': paper_id.lower(), 'title': title.lower(), 'abstract': abstract.lower()}, index=[0])

        ## Append the new row to the dataframe
        metadata_df = pd.concat([metadata_df, new_row], ignore_index=True)

    ## If the paper is not published between 2020 and 2023, continue to the next paper
    else:
        continue

## Save the dataframe to a parquet file
metadata_df.to_parquet('kaggle/arxiv_metadata_2020_to_2023.parquet', index=False)

## Print the shape of the metadata dataframe
print(f'Shape of metadata dataframe: {metadata_df.shape}')

## Print the time taken to process the metadata
end_time = time()
print(f'Time taken: {end_time - start_time:.2f} seconds')

