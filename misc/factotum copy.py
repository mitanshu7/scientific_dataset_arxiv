## This code will be used to create the dataset from scratch

"""
Step 1: Download the papers from the google storage bucket
Step 2: Extract the text from the papers
Step 3: Extract articles from the papers
Step 4: Process the metadata
Step 5: Create the dataset by merging the metadata and articles
"""

#####################################################################################################################
#####################################################################################################################
#####################################################################################################################
## Step 5: Create the dataset by merging the metadata and articles

## This script is used to merge the articles and abstracts into a single file

## Importing the required libraries
import pandas as pd
from glob import glob
from time import time

## Track the time taken to merge the articles and abstracts
start_time = time()

## Helper functions
## Function to extract the id from the file path
def extract_id_from_file(file_path):
    full_id = file_path.split('/')[-1].split('.')[0] + '.' + file_path.split('/')[-1].split('.')[1]
    id_without_version = full_id.split('v')[0]
    return id_without_version

## Function to process a file
def process_file(file_path):

    ## Extract the id from the file path
    id_without_version = extract_id_from_file(file_path)

    ## Get the metadata for the id
    metadata = metadata_df[metadata_df['id'] == id_without_version]

    ## If the metadata is found
    if not metadata.empty:

        ## Get the title, abstract and article
        title = metadata['title'].values[0]
        abstract = metadata['abstract'].values[0]

        with open(file_path, 'r') as rf:
            article = rf.read()

        ## Create a new row
        new_row = pd.DataFrame({'id': [id_without_version], 'title': [title], 'abstract': [abstract], 'article': [article]}, index=[0])

        return new_row
    else:
        print(f"Metadata not found for {id_without_version}")
        return None
    
## Load the trimmed dataframe into memory
print('Loading the trimmed metadata dataframe into memory')
metadata_df = pd.read_parquet(trimmed_data_file)

## Create a dataset dataframe
dataset_df = pd.DataFrame(columns=['id', 'title', 'abstract', 'article'])

## Get a list of all txt files from the extracted_articles
txt_files = glob('extracted_articles/**/*.txt', recursive=True)
txt_files.sort()

print(f'Processing {len(txt_files)} files')

## Process the files in parallel
with Pool() as pool:
    results = pool.map(process_file, txt_files)

## Concatenate the results into a single dataframe
dataset_df = pd.concat([res for res in results if res is not None], ignore_index=True)

## Remove duplicates
dataset_df.drop_duplicates(subset=['id'], keep='last', inplace=True)

## Save the dataset dataframe to a parquet file
print('Saving the dataset dataframe to a parquet file')
dataset_file = f'{download_path}/arxiv_dataset_{start_year}_to_{end_year}.parquet'
dataset_df.to_parquet(dataset_file, index=False)

## Print the shape of the dataset dataframe
print(f'Shape of dataset dataframe: {dataset_df.shape}')

## Print the time taken to merge the articles and abstracts
end_time = time()
print(f'Time taken to merge the extracted articles and abstracts: {end_time - start_time:.2f} seconds')

## Track the time taken to run the program
program_end_time = time()

print(f'Time taken to run the program: {program_end_time - program_start_time:.2f} seconds')

#####################################################################################################################
#####################################################################################################################
#####################################################################################################################

## End of the code