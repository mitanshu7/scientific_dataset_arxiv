## This script merges all the parquet files in a directory into a single parquet file

import os
import pandas as pd
from glob import glob
from scientific_dataset_arxiv.config import start_year, end_year

def create_folder(directory_path):
    """
    Create a folder if it doesn't exist.

    Args:
        directory_path (str): The path of the directory to be created.
    """
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

def merge_parquet_files(directory_path, output_file_path):
    """
    Merge all the parquet files in a directory into a single parquet file.

    Args:
        directory_path (str): The path of the directory containing the parquet files.
        output_file_path (str): The path of the output merged parquet file.
    """
    parquet_files = glob(f"{directory_path}/**/*.parquet", recursive=True)
    df = pd.read_parquet(parquet_files[0])
    for file in parquet_files[1:]:
        temp_df = pd.read_parquet(file)
        df = pd.concat([df, temp_df], ignore_index=True)
    df.to_parquet(output_file_path, index=False)
    print(f"Successfully merged all the parquet files to {output_file_path}")

if __name__ == '__main__':
    directory_path = f'arxiv_dataset_{start_year}_to_{end_year}'
    output_file_path = os.path.join(directory_path, 'merged_articles.parquet')
    merge_parquet_files(directory_path, output_file_path)
