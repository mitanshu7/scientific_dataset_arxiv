## This code exrtacts the text after the term 'introduction' from the articles and adds to the metadata dataframe
## The metadata dataframe is then saved to a parquet file
#####################################################################################################################
## Importing the required libraries
from datasets import load_dataset
from glob import glob
import pandas as pd
import os
from time import time
from multiprocessing import Pool
from functools import partial
from scientific_dataset_arxiv.config import start_year, end_year, search_term

## Function to create a folder if it doesn't exist
def create_folder(directory_path):
    """
    Create a folder if it doesn't exist.

    Args:
        directory_path (str): The path of the directory to be created.
    """
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

## Function to create a list of yy
def create_yy_list(start_year, end_year):
    """
    Create a list of yy (year in two-digit format) based on the start year and end year.

    Args:
        start_year (int): The starting year.
        end_year (int): The ending year.

    Returns:
        list: A list of yy values.
    """
    start_year = start_year % 100
    end_year = end_year % 100

    yy_list = range(start_year, end_year + 1)
        
    ## Convert the yymm list to string
    ## Pad a 0 if the year is less than 10
    yy_list = ['0' + str(i) if len(str(i)) == 1 else str(i) for i in yy_list]

    return yy_list

## Function to extract the article from the text file
def find_text_after_term(content, term):
    """
    Extracts the text after a given term in the content.

    Parameters:
    content (str): The content to search for the term.
    term (str): The term to search for.

    Returns:
    str: The text after the term, or None if the term is not found.
    """
    ## Find the index of the term in the content
    index = content.find(term.lower()) 

    ## If the term is found in the content
    if index != -1:
        
        ## Extract the text after the term
        selection = content[index:] 

        ## Return the selection
        return selection
    
    else:
        return None

## Function to extract the id from the file path
def extract_id_from_file(file_path):
    """
    Extract the id from the file path.

    Args:
        file_path (str): The path of the file.

    Returns:
        str: The id extracted from the file path.
    """
    ## Extract the original filename from the file path
    original_filename = os.path.basename(file_path)

    ## Extract the id from the original filename
    id_without_version = original_filename.split('v')[0]

    return id_without_version

## Function to process a file
def process_file(file_path, metadata_df):
    """
    Process a file and extract metadata.

    Args:
        file_path (str): The path of the file to be processed.
        metadata_df (pd.DataFrame): The metadata dataframe.

    Returns:
        pd.DataFrame: The processed metadata as a dataframe.
    """
    ## Extract the id from the file path
    id_without_version = extract_id_from_file(file_path)

    ## Add a try except block to handle the UnicodeDecodeError or a general error
    try:
        ## Open the file and read the content
        with open(file_path, 'r', encoding='utf-8') as rf:
            content = rf.read().lower() ## Convert the content to lower case
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None
    
    ## Find the text after the term 'introduction'
    article = find_text_after_term(content, search_term)

    ## Get the metadata for the id
    metadata = metadata_df[metadata_df['id'] == id_without_version]

    ## If the metadata is found
    if not metadata.empty and article is not None:

        ## Get the title, abstract and article in lower case
        title = metadata['title'].values[0]
        abstract = metadata['abstract'].values[0]

        ## Create a new row
        new_row = pd.DataFrame({'id': [id_without_version], 'title': [title], 'abstract': [abstract], 'article': [article]}, index=[0])

        return new_row
    else:
        print(f"Metadata or Article not found for {file_path}")
        return None


## Main code

yy_list = create_yy_list(start_year, end_year)

dataset_path = f'arxiv_dataset_{start_year}_to_{end_year}'
create_folder(dataset_path)

if __name__ == '__main__':

    ## Track time
    tic = time()

    for yy in yy_list:

        ## Load the trimmed dataframe into memory
        print('Loading the trimmed metadata dataframe into memory')

        REPO_ID = "bluuebunny/arxiv_metadata_by_year"
        FILENAME = f'data/arxiv_metadata_20{yy}.parquet'
        dataset = load_dataset(REPO_ID, data_files=FILENAME, verification_mode='no_checks')

        metadata_df = dataset['train'].to_pandas()


        ## Get a list of all txt files from the extracted_articles
        txt_files = glob(f'unprocessed_txts_{start_year}_to_{end_year}/{yy}*/**/*.txt', recursive=True)
        txt_files.sort()

        ## Track the progress
        print(f'Processing {len(txt_files)} files')

        ## Process the files in parallel
        pool = Pool()

        # Create a new function with metadata_df as a default argument
        process_file_with_metadata = partial(process_file, metadata_df=metadata_df)
        
        # Use the new function with pool.map
        results = pool.map(process_file_with_metadata, txt_files)

        # Close the pool and wait for all worker processes to finish
        pool.close()
        pool.join()

        ## Concatenate the results into a single dataframe
        dataset_df = pd.concat([res for res in results if res is not None], ignore_index=True)

        ## Remove duplicates
        dataset_df.drop_duplicates(subset=['id'], keep='last', inplace=True)

        ## Turn the contents of the dataframe to lowercase
        print('Turning the contents of the dataframe to lowercase')
        dataset_df = dataset_df.apply(lambda x: x.str.lower())

        ## Save the dataset dataframe to a parquet file
        print('Saving the dataset dataframe to a parquet file')
        dataset_file = f'{dataset_path}/arxiv_dataset_20{yy}.parquet'
        dataset_df.to_parquet(dataset_file, index=False)

        ## Print the shape of the dataset dataframe
        print(f'Shape of dataset dataframe: {dataset_df.shape}')

    ## Track the time
    toc = time()
    print(f'Time taken: {toc - tic:.2f} seconds')
