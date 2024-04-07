#####################################################################################################################
## Enter the year range for which you want to download the papers and convert them to text files
## The year range is inclusive, valid for yymm >=0704. Arxiv metadata starts from 2007-04
start_year = 2019
end_year = 2020
#####################################################################################################################

## This script merges all the parquet files in a directory into a single parquet file

import os
import pandas as pd
from glob import glob

## Function to create a folder if it doesn't exist
def create_folder(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

## Function to merge all the parquet files in a directory
def merge_parquet_files(directory_path, output_file_path):
    
    ## Get all the parquet files
    parquet_files = glob(f"{directory_path}/**/*.parquet", recursive=True)
    
    ## Read the first parquet file
    df = pd.read_parquet(parquet_files[0])
    
    ## Loop through the rest of the parquet files
    for file in parquet_files[1:]:
        
        ## Read the parquet file
        temp_df = pd.read_parquet(file)
        
        ## concat the dataframes
        df = pd.concat([df, temp_df], ignore_index=True)
    
    ## Write the merged data to a new parquet file
    df.to_parquet(output_file_path, index=False)
    
    print(f"Successfully merged all the parquet files to {output_file_path}")

## Main code

if __name__ == '__main__':
        
        ## Directory path containing all the parquet files
        directory_path = f'arxiv_dataset_{start_year}_to_{end_year}'

        ## Output file path
        output_file_path = os.path.join(directory_path, 'merged_articles.parquet')
        
        ## Merge all the parquet files
        merge_parquet_files(directory_path, output_file_path)