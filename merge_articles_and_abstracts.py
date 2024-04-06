## Importing the required libraries
import pandas as pd
from glob import glob
from time import time

## Track the time taken to merge the articles and abstracts
start_time = time()

## Helper functions
def extract_id_from_file(file_path):
    full_id = file_path.split('/')[-1].split('.')[0] + '.' + file_path.split('/')[-1].split('.')[1]
    id_without_version = full_id.split('v')[0]
    return id_without_version

## Defining paths
data_file = 'kaggle/arxiv_metadata_2020_to_2023.parquet'

## Load the dataframe into memory
metadata_df = pd.read_parquet(data_file)

## Create a dataset dataframe
dataset_df = pd.DataFrame(columns=['id', 'title', 'abstract', 'article'])

## Get a list of all txt files from the scientific_dataset*
txt_files = glob('extracted_articles/**/*.txt', recursive=True)
txt_files.sort()

## Iterate over the txt files

for file_path in txt_files:

    ## Extract the id from the file path
    id_without_version = extract_id_from_file(file_path)

    ## Get the metadata for the id
    metadata = metadata_df[metadata_df['id'] == id_without_version]

    ## If the metadata is found
    if not metadata.empty:

        ## Get the title and abstract
        title = metadata['title'].values[0]
        abstract = metadata['abstract'].values[0]

        ## Read the content of the file
        with open(file_path, 'r') as rf:
            article = rf.read()

        ## Create a new row
        new_row = pd.DataFrame({'id': id_without_version, 'title': title, 'abstract': abstract, 'article': article}, index=[0])

        ## Append the new row to the dataset dataframe
        dataset_df = pd.concat([dataset_df, new_row], ignore_index=True)

    ## If the metadata is not found
    else:
        print(f"Metadata not found for {id_without_version}")

## Remove duplicates
dataset_df.drop_duplicates(subset=['id'], keep='last', inplace=True)

## Save the dataset dataframe to a parquet file
dataset_df.to_parquet('kaggle/arxiv_dataset_2020_to_2023.parquet', index=False)

## Print the shape of the dataset dataframe
print(f'Shape of dataset dataframe: {dataset_df.shape}')

## Print the time taken to merge the articles and abstracts
end_time = time()
print(f'Time taken: {end_time - start_time:.2f} seconds')

